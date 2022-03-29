import threading
import logging

from .pulp import PulpClientService

LOG = logging.getLogger("pubtools.pulp")


class CachingPulpClient(object):
    """A Pulp client wrapper adding some modest caching."""

    def __init__(self, delegate):
        # Most methods work as usual, so just copy some references across.
        self.search_repository = delegate.search_repository
        self.search_content = delegate.search_content
        self.copy_content = delegate.copy_content
        self.update_content = delegate.update_content

        self._delegate = delegate
        self._repo_cache = {}
        self._lock = threading.Lock()

    def get_repository(self, repo_id):
        with self._lock:
            # Use cached object if we have one - but not if it was
            # unsuccessful.
            out = self._repo_cache.get(repo_id)
            if out and (not out.done() or not out.exception()):
                return out

        out = self._delegate.get_repository(repo_id)
        with self._lock:
            self._repo_cache[repo_id] = out

        return out

    def _invalidate(self, repo_id):
        with self._lock:
            self._repo_cache.pop(repo_id, None)

    def update_repository(self, repo):
        # update_repository needs a simple wrapper to ensure our
        # cache becomes invalidated.
        out = self._delegate.update_repository(repo)
        out.add_done_callback(lambda _: self._invalidate(repo.id))
        return out


# Because class is designed as a mix-in...
# pylint: disable=no-member


class CachingPulpClientService(PulpClientService):
    """A service providing a caching Pulp client.

    When this service is inherited by a task, that task will have access to
    both a pulp_client property which is a Pulp client, and a
    caching_pulp_client property which returns the same client with some caching
    added.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(CachingPulpClientService, self).__init__(*args, **kwargs)

    @property
    def caching_pulp_client(self):
        """A caching Pulp client used during task, instantiated on demand."""
        with self.__lock:
            if not self.__instance:
                self.__instance = CachingPulpClient(self.pulp_client)
        return self.__instance
