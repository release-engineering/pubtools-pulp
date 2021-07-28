import threading
import os
import logging

import requests
from more_executors import Executors
from more_executors.futures import f_map

LOG = logging.getLogger("pubtools.pulp")


class UdCacheClient(object):
    # Client for flushing UD cache.

    # Default number of request thread modifiable by an env variable.
    # This is not a documented/supported feature of the library.
    _REQUEST_THREADS = int(os.environ.get("UDCACHE_REQUEST_THREADS", "4"))

    def __init__(self, url, max_retry_sleep=None, **kwargs):
        """Create a new UD cache flush client.

        Arguments:
            url (str)
                Base URL of UD cache flushing API.
            max_retry_sleep (float)
                Max number of seconds to sleep between retries.
                Mainly provided so that tests can reduce the time needed to retry.
            kwargs
                Remaining arguments are used to initialize the requests.Session()
                used within this class (e.g. "verify", "auth").
        """
        self._url = url
        self._tls = threading.local()

        retry_args = {}
        if max_retry_sleep:
            retry_args["max_sleep"] = max_retry_sleep

        self._session_attrs = kwargs
        self._executor = (
            Executors.thread_pool(name="ud-client", max_workers=self._REQUEST_THREADS)
            .with_map(self._check_http_response)
            .with_retry(**retry_args)
        )

    @staticmethod
    def _check_http_response(response):
        response.raise_for_status()

    @property
    def _session(self):
        if not hasattr(self._tls, "session"):
            self._tls.session = requests.Session()
            for (key, value) in self._session_attrs.items():
                setattr(self._tls.session, key, value)
        return self._tls.session

    def _get(self, *args, **kwargs):
        return self._session.get(*args, **kwargs)

    def _on_failure(self, object_type, object_id, exception):
        LOG.error("Invalidating %s %s failed: %s", object_type, object_id, exception)
        raise exception

    def _flush_object(self, object_type, object_id):
        url = os.path.join(
            self._url, "internal/rcm/flush-cache", object_type, str(object_id)
        )

        LOG.info("Invalidating %s %s", object_type, object_id)

        # This is pretty odd, but yes, an HTTP *GET* here is used to flush cache.
        out = self._executor.submit(self._get, url)

        # Wrap with logging on failure
        out = f_map(
            out, error_fn=lambda ex: self._on_failure(object_type, object_id, ex)
        )

        return out

    def flush_product(self, product_id):
        """Flush a particular product by ID.

        Arguments:
            product_id (int)
                Engineering product ID (e.g. 270).

        Returns:
            Future[None]
                A future resolved once flush has completed.
        """
        return self._flush_object("eng-product", product_id)

    def flush_repo(self, repo_id):
        """Flush a particular repository by ID.

        Arguments:
            repo_id (str)
                Pulp repository ID.

        Returns:
            Future[None]
                A future resolved once flush has completed.
        """
        return self._flush_object("repo", repo_id)
