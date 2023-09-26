import logging
import os
import re
import threading
from collections import namedtuple, OrderedDict


import requests
from more_executors import Executors
from more_executors.futures import f_map

LOG = logging.getLogger("pubtools.pulp")


HeaderPair = namedtuple("HeaderPair", ["request", "response"])


class CdnClient(object):
    # Client for requesting special headers from CDN service.

    # Default number of request thread modifiable by an env variable.
    # This is not a documented/supported feature of the library.
    _REQUEST_THREADS = int(os.environ.get("CDN_REQUEST_THREADS", "4"))
    _ATTEMPTS = int(os.environ.get("CDN_RETRY_ATTEMPTS", "9"))
    _SLEEP = float(os.environ.get("CDN_RETRY_SLEEP", "1.0"))
    _EXPONENT = float(os.environ.get("CDN_RETRY_EXPONENT", "3.0"))
    _MAX_SLEEP = float(os.environ.get("CDN_RETRY_MAX_SLEEP", "120.0"))

    TTL_REGEX = re.compile(r".*/(\d+[smhd])/.*")
    CACHE_KEY_HEADER = HeaderPair("akamai-x-get-cache-key", "X-Cache-Key")

    def __init__(self, url, max_retry_sleep=_MAX_SLEEP, **kwargs):
        """Create a new CDN client.

        Arguments:
            url (str)
                Base URL of CDN
            max_retry_sleep (float)
                Max number of seconds to sleep between retries.
                Mainly provided so that tests can reduce the time needed to retry.
            kwargs
                Remaining arguments are used to initialize the requests.Session()
                used within this class (e.g. "verify", "cert").
        """
        self._url = url
        self._tls = threading.local()

        retry_args = {
            "max_sleep": max_retry_sleep,
            "max_attempts": CdnClient._ATTEMPTS,
            "sleep": CdnClient._SLEEP,
            "exponent": CdnClient._EXPONENT,
        }

        self._session_attrs = kwargs
        self._executor = (
            Executors.thread_pool(name="cdn-client", max_workers=self._REQUEST_THREADS)
            .with_map(self._check_http_response)
            .with_retry(**retry_args)
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self._executor.__exit__(*exc_details)

    @staticmethod
    def _check_http_response(response):
        response.raise_for_status()
        return response

    @property
    def _session(self):
        if not hasattr(self._tls, "session"):
            self._tls.session = requests.Session()
            for key, value in self._session_attrs.items():
                setattr(self._tls.session, key, value)
        return self._tls.session

    def _head(self, *args, **kwargs):
        return self._session.head(*args, **kwargs)

    def _on_failure(self, header, exception):
        LOG.error("Requesting header %s failed: %s", header, exception)
        raise exception

    def _get_headers_for_path(self, path, headers):
        url = os.path.join(self._url, path)

        LOG.debug("Getting headers %s for %s", list(headers.values()), url)

        out = self._executor.submit(self._head, url, headers=headers)
        out = f_map(
            out,
            fn=lambda resp: resp.headers,
            error_fn=lambda ex: self._on_failure(list(headers.values()), ex),
        )

        return out

    def _get_ttl(self, path):
        headers = {"Pragma": self.CACHE_KEY_HEADER.request}
        out = self._get_headers_for_path(path, headers)

        def _parse_ttl(value):
            parsed = re.match(
                self.TTL_REGEX, value.get(self.CACHE_KEY_HEADER.response) or ""
            )
            return parsed.group(1) if parsed else None

        return f_map(out, _parse_ttl)

    def _is_valid_template(self, template):
        return all(["{ttl}" in template, "path" in template])

    def get_arl_for_path(self, path, templates):
        """Get ARL for particular path using provided templates.
        This method generates ARLs for given path according to
        provided ARL templates. TTL value is requested from CDN
        special headers.

        If value of TTL cannot be fetched from CDN service,
        we fallback to hardcoded values.

        Arguments:
            path (str)
                Relative path/URL (e.g. content/foo/bar/repomd.xml).
            templates (List[str])
                A list of templates used for generating ARLs.
                (e.g. ["/foo/bar/{ttl}/{path}", ...]). The {ttl} and
                {path} formatting substrings are required.
        Returns:
            List[Future]
                A list of futures holding formatted ARLs.
        """

        def _format_template(ttl, template, path):
            ttl = f_map(ttl, fn=lambda x: x, error_fn=lambda _: ttl_for_path(path))
            return f_map(ttl, lambda x: template.format(ttl=x, path=path))

        out = []
        ttl_ft = self._get_ttl(path)

        for item in templates:
            if self._is_valid_template(item):
                out.append(_format_template(ttl_ft, item, path))

        return out


# ordering of items matters as it's used as priority
CDN_TTL_CONFIG = OrderedDict(
    {
        re.compile(r"/repodata/.*\.xml$"): "4h",
        re.compile(r".*/ostree/repo/refs/heads/.*/(base|standard)$"): "10m",
        re.compile(r"(/PULP_MANIFEST$|/listing$|/repodata/)"): "10m",
        re.compile(r"/$"): "4h",
    }
)

DEFAULT_TTL = "30d"


def ttl_for_path(path):
    out = DEFAULT_TTL
    for regex, ttl in CDN_TTL_CONFIG.items():
        if regex.search(path):
            out = ttl
            break

    return out
