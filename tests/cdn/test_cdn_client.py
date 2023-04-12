import logging
from concurrent.futures import as_completed

from pubtools._pulp.cdn import CdnClient


def test_format_arl_template(requests_mock, caplog):
    """Client formats ARL template with live TTL."""
    caplog.set_level(logging.INFO)

    # test templates also with invalid template which should be skipped from processing
    templates = [
        "/fake/template-1/{ttl}/{path}",
        "/fake/template-2/{ttl}/{path}",
        "invalid-template/{xyz}",
    ]

    with CdnClient(
        "https://cdn.example.com/", cert=("path/cert", "path/key")
    ) as client:
        url_ttl = [
            ("https://cdn.example.com/content/foo/test-path-1/repomd.xml", "10h"),
            ("https://cdn.example.com/content/foo/test-path-2/other-file.xml", "30m"),
        ]

        for url, ttl in url_ttl:
            headers = {"X-Cache-Key": f"/fake/cache-key/{ttl}/something"}
            requests_mock.register_uri("HEAD", url, headers=headers)

        fts = []
        # Let client get arl for given paths
        fts.extend(
            client.get_arl_for_path("content/foo/test-path-1/repomd.xml", templates)
        )
        fts.extend(
            client.get_arl_for_path("content/foo/test-path-2/other-file.xml", templates)
        )

        # It should succeed and return expected ARLs
        arls = [ft.result() for ft in as_completed(fts)]
        assert sorted(arls) == [
            "/fake/template-1/10h/content/foo/test-path-1/repomd.xml",
            "/fake/template-1/30m/content/foo/test-path-2/other-file.xml",
            "/fake/template-2/10h/content/foo/test-path-1/repomd.xml",
            "/fake/template-2/30m/content/foo/test-path-2/other-file.xml",
        ]
    # It should have called above URLs
    fetched_urls = [req.url for req in requests_mock.request_history]
    assert sorted(fetched_urls) == [url for url, _ in url_ttl]

    # It should log 'Getting headers...' for each path
    assert caplog.messages == [
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-1/repomd.xml",
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-2/other-file.xml",
    ]


def test_retries(requests_mock):
    """Client retries automatically on error."""
    templates = [
        "/fake/template-1/{ttl}/{path}",
    ]
    with CdnClient("https://cdn.example.com/", max_retry_sleep=0.001) as client:
        url = "https://cdn.example.com/content/foo/test-path-1/repomd.xml"

        requests_mock.register_uri(
            "HEAD",
            url,
            [
                # Fails on first try
                {"status_code": 500},
                # Then succeeds
                {
                    "status_code": 200,
                    "headers": {"X-Cache-Key": f"/fake/cache-key/10h/something"},
                },
            ],
        )

        # It should succeed due to retrying
        results = [
            item.result()
            for item in client.get_arl_for_path(
                "content/foo/test-path-1/repomd.xml", templates
            )
        ]

    # It should have called above URL twice
    fetched_urls = [req.url for req in requests_mock.request_history]
    assert fetched_urls == [url] * 2

    # Checked ARL result
    assert results[0] == "/fake/template-1/10h/content/foo/test-path-1/repomd.xml"


def test_logs(requests_mock, caplog):
    """Client produces logs after requests."""

    caplog.set_level(logging.INFO)

    templates = [
        "/fake/template-1/{ttl}/{path}",
    ]
    with CdnClient("https://cdn.example.com/", max_retry_sleep=0.001) as client:
        url = "https://cdn.example.com/content/foo/test-path-1/repomd.xml"

        requests_mock.register_uri("HEAD", url, status_code=500)

        # It should eventually fail with the HTTP error
        exception = client.get_arl_for_path(
            "content/foo/test-path-1/repomd.xml", templates
        )[0].exception()
        assert "500 Server Error" in str(exception)

    # It should have logged what it was doing and what failed
    assert caplog.messages == [
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-1/repomd.xml",
        "Requesting header ['akamai-x-get-cache-key'] failed: 500 Server Error: None "
        "for url: https://cdn.example.com/content/foo/test-path-1/repomd.xml",
    ]
