import os
import logging
from more_executors.futures import f_map, f_sequence

from pubtools.pulplib import PublishOptions

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import FastPurgeClientService, UdCacheClientService

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step

# Since these classes are designed to be mixed in with PulpTask but pylint
# doesn't know that...
# pylint: disable=no-member


class CDNCache(FastPurgeClientService):
    """Provide features to interact with CDN cache."""

    @step("Flush CDN cache")
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
                flush_url = os.path.join(
                    self.fastpurge_root_url, repo.relative_url, url
                )
                to_flush.append(flush_url)

            LOG.debug("Flush: %s", to_flush)
            flush = self.fastpurge_client.purge_by_url(to_flush)
            return f_map(flush, lambda _: repo)

        return [purge_repo(r) for r in repos if r.relative_url]


class UdCache(UdCacheClientService):
    """Provide features to interact with UD cache."""

    @step("Flush UD cache")
    def flush_ud(self, repos):
        client = self.udcache_client
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return []

        out = []
        for repo in repos:
            out.append(client.flush_repo(repo.id))

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

    def publish_with_cache_flush(self, repos):
        # publish the repos found
        publish_fs = self.publish(repos)

        # wait for the publish to complete before
        # flushing caches.
        f_sequence(publish_fs).result()

        # flush CDN cache
        out = self.flush_cdn(repos)

        # flush UD cache
        out.extend(self.flush_ud(repos))

        return out
