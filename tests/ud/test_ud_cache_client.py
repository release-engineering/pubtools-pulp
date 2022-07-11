import logging


from pubtools._pulp.ud import UdCacheClient


def test_flush(requests_mock, caplog):
    """Client flushes by hitting expected URLs."""

    caplog.set_level(logging.INFO)

    with UdCacheClient("https://ud.example.com/", auth=("user", "pass")) as client:
        urls = [
            "https://ud.example.com/internal/rcm/flush-cache/eng-product/1234",
            "https://ud.example.com/internal/rcm/flush-cache/repo/some-repo",
            "https://ud.example.com/internal/rcm/flush-cache/erratum/RHBA-1234",
        ]

        for url in urls:
            requests_mock.register_uri("GET", url)

        # It should succeed
        client.flush_product(1234).result()
        client.flush_repo("some-repo").result()
        client.flush_erratum("RHBA-1234").result()

    # It should have called above two URLs
    fetched_urls = [req.url for req in requests_mock.request_history]
    assert fetched_urls == urls

    # It should log flush for each unit
    assert caplog.messages == [
        "Invalidating eng-product 1234",
        "Invalidating repo some-repo",
        "Invalidating erratum RHBA-1234",
    ]


def test_retries(requests_mock):
    """Client retries automatically on error."""

    with UdCacheClient(
        "https://ud.example.com/", auth=("user", "pass"), max_retry_sleep=0.001
    ) as client:
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

    with UdCacheClient(
        "https://ud.example.com/", auth=("user", "pass"), max_retry_sleep=0.001
    ) as client:
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
