import os
import logging
from more_executors.futures import f_map

from pubtools._pulp.task import PulpTask
from .services import FastPurgeClientService

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step


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
