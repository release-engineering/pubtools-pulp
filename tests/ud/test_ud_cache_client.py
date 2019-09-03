import logging


from pubtools._pulp.ud import UdCacheClient


def test_flush(requests_mock):
    """Client flushes by hitting expected URLs."""

    client = UdCacheClient("https://ud.example.com/", auth=("user", "pass"))

    urls = [
        "https://ud.example.com/internal/rcm/flush-cache/eng-product/some-product",
        "https://ud.example.com/internal/rcm/flush-cache/repo/some-repo",
    ]

    for url in urls:
        requests_mock.register_uri("GET", url)

    # It should succeed
    client.flush_product("some-product").result()
    client.flush_repo("some-repo").result()

    # It should have called above two URLs
    fetched_urls = [req.url for req in requests_mock.request_history]
    assert fetched_urls == urls


def test_retries(requests_mock):
    """Client retries automatically on error."""

    client = UdCacheClient(
        "https://ud.example.com/", auth=("user", "pass"), max_retry_sleep=0.001
    )

    url = "https://ud.example.com/internal/rcm/flush-cache/repo/some-repo"

    requests_mock.register_uri(
        "GET",
        url,
        [
            # Fails on first try
            {"status_code": 500},
            # Then succeeds
            {"status_code": 200},
        ],
    )

    # It should succeed due to retrying
    client.flush_repo("some-repo").result()

    # It should have called above URL twice
    fetched_urls = [req.url for req in requests_mock.request_history]
    assert fetched_urls == [url] * 2


def test_logs(requests_mock, caplog):
    """Client produces logs before/after requests."""

    caplog.set_level(logging.INFO)

    client = UdCacheClient(
        "https://ud.example.com/", auth=("user", "pass"), max_retry_sleep=0.001
    )

    url = "https://ud.example.com/internal/rcm/flush-cache/repo/some-repo"

    requests_mock.register_uri("GET", url, status_code=500)

    # It should eventually fail with the HTTP error
    exception = client.flush_repo("some-repo").exception()
    assert "500 Server Error" in str(exception)

    # It should have logged what it was doing and what failed
    assert caplog.messages == [
        "Invalidating repo some-repo",
        "Invalidating repo some-repo failed: %s" % exception,
    ]
