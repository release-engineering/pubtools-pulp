import collections
import datetime
import logging
import os
import sys

import attr
from more_executors.futures import f_flat_map, f_map, f_return, f_sequence
from pubtools.pulplib import (
    ErratumUnit,
    FileUnit,
    ModulemdUnit,
    PublishOptions,
    RpmUnit,
)
from pushsource import ErratumPushItem, FilePushItem, ModuleMdPushItem, RpmPushItem

from pubtools._pulp.services import (
    CdnClientService,
    FastPurgeClientService,
    UdCacheClientService,
)
from pubtools._pulp.task import PulpTask

from ..hooks import pm

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step

# Since these classes are designed to be mixed in with PulpTask but pylint
# doesn't know that...
# pylint: disable=no-member


class CDNCache(FastPurgeClientService, CdnClientService):
    """Provide features to interact with CDN cache."""

    @step("Flush CDN cache", depends_on=["publish"], skipped_value=[])
    def flush_cdn(self, repos):
        """Clears the CDN cache for the repositories provided

        Arguments:
            repos (list)
                Repositories to be cleared from the cache

        Returns:
            list[Future]
                List of futures that resolve to the repository
                objects on completion
                An empty list when a client is not available
        """
        if not self.fastpurge_client:
            LOG.info("CDN cache flush is not enabled.")
            return []

        def purge_repo(repo):
            to_flush = []
            for url in repo.mutable_urls:
                relative_mutable_url = os.path.join(repo.relative_url, url)
                flush_url = os.path.join(self.fastpurge_root_url, relative_mutable_url)
                LOG.debug("Flush: %s", flush_url)

                to_flush.append(f_return(flush_url))

                if self.cdn_client:
                    arl_fts = self.cdn_client.get_arl_for_path(
                        relative_mutable_url,
                        self.args.cdn_arl_template,
                    )
                    to_flush.extend(arl_fts)

            flush = f_flat_map(
                f_sequence(to_flush), lambda urls: self.purge_urls(repo.id, urls)
            )
            return f_map(flush, lambda _: repo)

        return [purge_repo(r) for r in repos if r.relative_url]

    def purge_urls(self, repo_id: str, urls: list):
        LOG.info("Flushing cache for %s:", repo_id)
        for url in sorted(urls):
            LOG.info("   %s", url)
        return self.fastpurge_client.purge_by_url(urls)


class UdCache(UdCacheClientService):
    """Provide features to interact with UD cache."""

    @step("Flush UD cache")
    def flush_ud(self, repos, errata=None):
        client = self.udcache_client
        out = []
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return out

        for repo in repos:
            # RHELDST-24551: UD can't flush cache of repos that have no eng product ID.
            # Ensure this condition is met before flushing.
            if repo.eng_product_id:
                out.append(client.flush_repo(repo.id))
                out.append(client.flush_product(repo.eng_product_id))

        out.extend([client.flush_erratum(erratum.id) for erratum in (errata or [])])

        return out


class Publisher(CDNCache, UdCache):
    """Provides behavior relating to Pulp repo publish which can be shared by
    multiple tasks."""

    def add_publisher_args(self, parser):
        group = parser.add_argument_group(
            "Publish options", "Options affecting the behavior of Pulp repo publishes."
        )
        group.add_argument(
            "--clean",
            help="attempt to delete remote content not in the repo",
            action="store_true",
        )
        group.add_argument(
            "--force",
            help="force publish of repos even if Pulp thinks nothing has changed",
            action="store_true",
        )

    @step("Publish")
    def publish(self, repos):
        out = []

        publish_opts = PublishOptions(force=self.args.force, clean=self.args.clean)
        for repo in repos:
            LOG.info("Publishing %s", repo.id)
            f = repo.publish(publish_opts)
            out.append(f)

        return out

    @classmethod
    def cdn_published_value(cls):
        # Return a value which should be used for cdn_published field.
        #
        # This method exists mainly to ensure this is mockable during tests.
        return datetime.datetime.utcnow()

    @step("Set cdn_published")
    def set_cdn_published(self, units, pulp_client):
        now = self.cdn_published_value()
        out = []
        for unit in units or []:
            out.append(pulp_client.update_content(attr.evolve(unit, cdn_published=now)))

        if out:
            LOG.info(
                "Setting cdn_published = %s on %s unit(s)",
                now,
                len(out),
            )
        return out

    def publish_with_cache_flush(
        self, repos, units=None, pulp_client=None, errata=None
    ):
        # Ensure all repos in 'repos' are fully published, and CDN/UD caches are flushed.
        #
        # If 'units' are provided, ensures those units have cdn_published field set after
        # the publish and before the UD cache flush.
        #
        units = units or []
        pulp_client = pulp_client or self.pulp_client

        # publish the repos found
        publish_fs = self.publish(repos)

        # wait for the publish to complete before
        # flushing caches.
        f_sequence(publish_fs).result()

        # hook implementation(s) may now flush pulp-derived caches and datastores
        pm.hook.task_pulp_flush()

        # flush CDN cache
        out = self.flush_cdn(repos)

        # set units as published
        set_published = f_sequence(self.set_cdn_published(units, pulp_client))

        # flush UD cache only after cdn_published is set (if applicable)
        flush_ud = f_flat_map(
            set_published, lambda _: f_sequence(self.flush_ud(repos, errata))
        )
        out.append(flush_ud)

        return out


class PulpRepositoryOperation(CDNCache, UdCache, PulpTask):
    def __init__(self):
        super(PulpRepositoryOperation, self).__init__()

        self.task_state = "PENDING"

    def add_args(self):
        super(PulpRepositoryOperation, self).add_args()

        self.parser.add_argument(
            "--skip", help="skip given comma-separated sub-steps", type=str
        )

    def fail(self, *args, **kwargs):
        LOG.error(*args, **kwargs)
        sys.exit(30)

    def log_remove(self, removed_repo):
        return self.log_repo_action(removed_repo, "removed")

    def log_copy(self, copied_repo):
        return self.log_repo_action(copied_repo, "copied")

    def log_repo_action(self, repo, action):
        # Given a repo which has been altered, log some messages
        # summarizing the affected unit(s).
        content_types = collections.defaultdict(int)

        for task in repo.tasks:
            for unit in task.units:
                type_id = unit.content_type_id
                content_types[type_id] += 1

        task_ids = ", ".join(sorted([t.id for t in repo.tasks]))
        repo_id = repo.repo.id
        if not content_types:
            LOG.warning("%s: no content %s, tasks: %s", repo_id, action, task_ids)
        else:
            types = []
            for key in sorted(content_types.keys()):
                types.append("%s %s(s)" % (content_types[key], key))
            types = ", ".join(types)

            LOG.info("%s: %s %s, tasks: %s", repo_id, action, types, task_ids)
        return repo

    @step("Record push items")
    def record_push_items(self, repo_fs, state=None):
        if state:
            self.task_state = state
        return [f_flat_map(f, self.record_repo_action) for f in repo_fs]

    def record_repo_action(self, repo):
        push_items = []
        for task in repo.tasks:
            repo_id = None if self.task_state == "DELETED" else repo.repo.id
            push_items.extend(self.push_items_for_task(task, repo_id))
        return self.collector.update_push_items(push_items)

    @step("Publish")
    def publish(self, repo_fs, clean=False):
        return [
            f_flat_map(f, lambda r: r.publish(PublishOptions(clean=clean)))
            for f in repo_fs
        ]

    def push_items_for_task(self, task, repo_id):
        out = []
        for unit in task.units:
            push_item = self.push_item_for_unit(unit, repo_id)
            if push_item:
                out.append(push_item)
        return out

    def push_item_for_unit(self, unit, repo_id):
        for unit_type, fn in [
            (ModulemdUnit, self.push_item_for_modulemd),
            (RpmUnit, self.push_item_for_rpm),
            (ErratumUnit, self.push_item_for_erratum),
            (FileUnit, self.push_item_for_file),
        ]:
            if isinstance(unit, unit_type):
                return fn(unit, repo_id)

    def push_item_for_modulemd(self, unit, repo_id):
        out = {}
        out["state"] = self.task_state
        out["origin"] = "pulp"

        # Note: N:S:V:C:A format here is kept even if some part
        # of the data is missing (never expected to happen).
        # For example, if C was missing, you'll get N:S:V::A
        # so the arch part can't be misinterpreted as context.
        nsvca = ":".join(
            [unit.name, unit.stream, str(unit.version), unit.context, unit.arch]
        )

        out["name"] = nsvca
        out["dest"] = [repo_id]

        return ModuleMdPushItem(**out)

    def push_item_for_rpm(self, unit, repo_id):
        out = {}

        out["state"] = self.task_state
        out["origin"] = "pulp"

        filename_parts = [
            unit.name,
            "-",
            unit.version,
            "-",
            unit.release,
            ".",
            unit.arch,
            ".rpm",
        ]
        out["name"] = "".join(filename_parts)
        out["dest"] = [repo_id]

        # Note: in practice we don't necessarily expect to get all of these
        # attributes, as after a delete the server will only provide those
        # which make up the unit key. We still copy them anyway (even if
        # values are None) in case this is improved some day.
        out["sha256sum"] = unit.sha256sum
        out["md5sum"] = unit.md5sum
        out["signing_key"] = unit.signing_key

        return RpmPushItem(**out)

    def push_item_for_erratum(self, unit, repo_id):
        out = {}

        out["state"] = self.task_state
        out["origin"] = "pulp"
        out["name"] = unit.id
        out["dest"] = [repo_id]

        return ErratumPushItem(**out)

    def push_item_for_file(self, unit, repo_id):
        out = {}

        out["state"] = self.task_state
        out["origin"] = "pulp"
        out["name"] = unit.path
        out["sha256sum"] = unit.sha256sum
        out["dest"] = [repo_id]

        return FilePushItem(**out)

    def run(self):
        """Implement a specific task"""

        raise NotImplementedError()
