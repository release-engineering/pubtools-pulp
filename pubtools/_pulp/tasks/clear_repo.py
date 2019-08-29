import logging
import sys
import os
from functools import partial
import attr

from more_executors.futures import f_map, f_flat_map, f_sequence
from pubtools.pulplib import (
    Criteria,
    ContainerImageRepository,
    PublishOptions,
    FileUnit,
    RpmUnit,
    ModulemdUnit,
)

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import (
    CollectorService,
    FastPurgeClientService,
    UdCacheClientService,
    PulpClientService,
)

step = PulpTask.step


LOG = logging.getLogger("pubtools.pulp")


@attr.s
class ClearedRepo(object):
    """Represents a single repo which has been cleared."""

    tasks = attr.ib()
    """The completed Pulp tasks for clearing this repo."""

    repo = attr.ib()
    """The repo which was cleared."""


class ClearRepo(
    CollectorService,
    FastPurgeClientService,
    UdCacheClientService,
    PulpClientService,
    PulpTask,
):
    """Remove all contents from one or more Pulp repositories.

    This command will remove contents from repositories and record
    information on what was removed.  Removal may optionally be
    filtered to selected content types.
    """

    @property
    def content_type(self):
        type_strs = (self.args.content_type or "").split(",")
        # Only return non-None if there were really any types given.
        # Otherwise, return None to let library defaults apply
        return [x for x in type_strs if x] or None

    def add_args(self):
        super(ClearRepo, self).add_args()

        self.parser.add_argument(
            "--skip", help="skip given comma-separated sub-steps", type=str
        )
        self.parser.add_argument(
            "--content-type",
            help="remove only content of these comma-separated type(s)",
            type=str,
        )
        self.parser.add_argument("repo", nargs="+", help="Repositories to be cleared")

    def fail(self, *args, **kwargs):
        LOG.error(*args, **kwargs)
        sys.exit(30)

    @step("Check repos")
    def get_repos(self):
        # Returns all repos to be operated on by this task.
        # Eagerly loads the repos so we fail early if the user passed any nonexistent
        # repo.
        repo_ids = self.args.repo
        found_repo_ids = []

        out = []
        search = self.pulp_client.search_repository(Criteria.with_id(repo_ids))
        for repo in search.result().as_iter():
            out.append(repo)
            found_repo_ids.append(repo.id)

        # Bail out if user requested repos which don't exist
        missing = set(repo_ids) - set(found_repo_ids)

        missing = sorted(list(missing))
        if missing:
            self.fail("Requested repo(s) don't exist: %s", ", ".join(missing))

        # Bail out if we'd be processing any container image repos.
        # We don't support this now because:
        #
        # - recording push items isn't implemented yet and it's not clear
        #   how to implement it (as we traditionally used docker-image-*.tar.gz
        #   filenames from brew as push item filename, but those aren't available
        #   in pulp metadata)
        #
        # - no known use-case for clearing them
        #
        container_repo_ids = sorted(
            [repo.id for repo in out if isinstance(repo, ContainerImageRepository)]
        )
        if container_repo_ids:
            self.fail(
                "Container image repo(s) provided, not supported: %s"
                % ", ".join(sorted(container_repo_ids))
            )

        return out

    def log_remove(self, cleared_repo):
        # Given a repo which has been cleared, log some messages
        # summarizing the removed unit(s)
        content_types = {}

        for task in cleared_repo.tasks:
            for unit in task.units:
                type_id = unit.content_type_id
                content_types[type_id] = content_types.get(type_id, 0) + 1

        task_ids = ", ".join(sorted([t.id for t in cleared_repo.tasks]))
        repo_id = cleared_repo.repo.id
        if not content_types:
            LOG.warning("%s: no content removed, tasks: %s", repo_id, task_ids)
        else:
            removed_types = []
            for key in sorted(content_types.keys()):
                removed_types.append("%s %s(s)" % (content_types[key], key))
            removed_types = ", ".join(removed_types)

            LOG.info("%s: removed %s, tasks: %s", repo_id, removed_types, task_ids)

        return cleared_repo

    @step("Clear content")
    def clear_content(self, repos):
        out = []

        for repo in repos:
            f = repo.remove_content(type_ids=self.content_type)
            f = f_map(f, partial(ClearedRepo, repo=repo))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    @step("Record push items")
    def record_clears(self, cleared_repo_fs):
        return [f_flat_map(f, self.record_cleared_repo) for f in cleared_repo_fs]

    def record_cleared_repo(self, cleared_repo):
        push_items = []
        for task in cleared_repo.tasks:
            push_items.extend(self.push_items_for_task(task))
        return self.collector.update_push_items(push_items)

    def push_items_for_task(self, task):
        out = []
        for unit in task.units:
            push_item = self.push_item_for_unit(unit)
            if push_item:
                out.append(push_item)
        return out

    def push_item_for_unit(self, unit):
        for (unit_type, fn) in [
            (ModulemdUnit, self.push_item_for_modulemd),
            (RpmUnit, self.push_item_for_rpm),
            (FileUnit, self.push_item_for_file),
        ]:
            if isinstance(unit, unit_type):
                return fn(unit)

    def push_item_for_modulemd(self, unit):
        out = {}
        out["state"] = "DELETED"
        out["origin"] = "pulp"

        # Note: N:S:V:C:A format here is kept even if some part
        # of the data is missing (never expected to happen).
        # For example, if C was missing, you'll get N:S:V::A
        # so the arch part can't be misinterpreted as context.
        nsvca = ":".join(
            [unit.name, unit.stream, str(unit.version), unit.context, unit.arch]
        )

        out["filename"] = nsvca

        return out

    def push_item_for_rpm(self, unit):
        out = {}

        out["state"] = "DELETED"
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
        out["filename"] = "".join(filename_parts)

        out["checksums"] = {}
        if unit.sha256sum:
            out["checksums"]["sha256"] = unit.sha256sum
        if unit.md5sum:
            out["checksums"]["md5"] = unit.md5sum

        out["signing_key"] = unit.signing_key

        return out

    def push_item_for_file(self, unit):
        return {
            "state": "DELETED",
            "origin": "pulp",
            "filename": unit.path,
            "checksums": {"sha256": unit.sha256sum},
        }

    @step("Publish")
    def publish(self, repo_fs):
        return [
            f_flat_map(f, lambda r: r.publish(PublishOptions(clean=True)))
            for f in repo_fs
        ]

    @step("Flush UD cache")
    def flush_ud(self, repos):
        client = self.udcache_client
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return []

        out = []
        for repo in repos:
            out.append(client.flush_repo(repo.id))
            if repo.eng_product_id:
                out.append(client.flush_product(repo.eng_product_id))

        return out

    @step("Flush CDN cache")
    def flush_cdn(self, repos):
        if not self.fastpurge_client:
            LOG.info("CDN cache flush is not enabled.")
            return []

        def purge_repo(repo):
            to_flush = []
            for url in repo.mutable_urls:
                flush_url = os.path.join(
                    self.fastpurge_root_url, repo.relative_url, url
                )
                to_flush.append(flush_url)

            LOG.debug("Flush: %s", to_flush)
            flush = self.fastpurge_client.purge_by_url(to_flush)
            return f_map(flush, lambda _: repo)

        return [purge_repo(r) for r in repos if r.relative_url]

    def run(self):
        to_await = []

        # Get the repos we'll be dealing with.
        # This is blocking so we'll fail early on missing/bad repos.
        repos = self.get_repos()

        # Start clearing repos.
        cleared_repos_fs = self.clear_content(repos)

        # As clearing completes, record pushitem info on what was removed.
        # We don't have to wait on this before continuing.
        to_await.extend(self.record_clears(cleared_repos_fs))

        # Don't need the repo clearing tasks for anything more.
        repos_fs = [f_map(f, lambda cr: cr.repo) for f in cleared_repos_fs]

        # Now move repos into the desired state:

        # They should be published.
        publish_fs = self.publish(repos_fs)

        # Wait for all repo publishes to complete before continuing.
        # Why: cache flush is what makes changes visible, and we want that to be
        # as near atomic as we can get (i.e. changes appear in every repo "at once",
        # rather than as each repo is published).
        f_sequence(publish_fs).result()

        # They should have UD cache flushed.
        to_await.extend(self.flush_ud(repos))

        # They should have CDN cache flushed.
        to_await.extend(self.flush_cdn(repos))

        # Now make sure we wait for everything to finish.
        for f in to_await:
            f.result()


def entry_point(cls=ClearRepo):
    cls().main()


def doc_parser():
    return ClearRepo().parser
