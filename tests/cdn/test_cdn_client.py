import logging
import pytest
from concurrent.futures import as_completed

from pubtools._pulp.cdn import CdnClient


def test_format_arl_template(requests_mock, caplog):
    """Client formats ARL template with live TTL."""
    caplog.set_level(logging.DEBUG)

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

        # ARLs are generated from the template using the {ttl} placeholder, which is replaced with
        # the real TTL value. The real TTL value is extracted from the cache key header of the real
        # request for the given path using '/(\d+[smhd])/' regex.
        # The /1h/foo in the mocked header here is to test that if the path contains a component
        # that also matches the TTL regex ('/1h/'), it will still find the correct value
        # ('/10h/' or '/30m/').
        for url, ttl in url_ttl:
            headers = {"X-Cache-Key": f"/fake/cache-key/{ttl}/something/1h/foo"}
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
    for message in [
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-1/repomd.xml",
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-2/other-file.xml",
    ]:
        assert message in caplog.messages


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
    """Client produces logs before/after requests."""

    caplog.set_level(logging.DEBUG)

    templates = [
        "/fake/template-1/{ttl}/{path}",
    ]
    with CdnClient("https://cdn.example.com/", max_retry_sleep=0.001) as client:
        url = "https://cdn.example.com/content/foo/test-path-1/some-file"

        requests_mock.register_uri("HEAD", url, status_code=500)

        # Request ARLs
        arls_ft = client.get_arl_for_path(
            "content/foo/test-path-1/some-file", templates
        )

        # It should be successful
        arl = [item.result() for item in as_completed(arls_ft)][0]

    # It should have logged what it was doing and what failed
    for message in [
        "Getting headers ['akamai-x-get-cache-key'] for "
        "https://cdn.example.com/content/foo/test-path-1/some-file",
        "Requesting header ['akamai-x-get-cache-key'] failed: 500 Server Error: None "
        "for url: https://cdn.example.com/content/foo/test-path-1/some-file",
    ]:
        assert message in caplog.messages

    # Eventually it should fallback to default ttl value
    assert arl == "/fake/template-1/30d/content/foo/test-path-1/some-file"


@pytest.mark.parametrize(
    "path, expected_ttl",
    [
        ("content/test/repodata/repomd.xml", "4h"),
        ("content/test/repodata/", "10m"),
        ("/ostree/repo/refs/heads/test-path/base", "10m"),
        ("content/test/PULP_MANIFEST", "10m"),
        ("content/test/", "4h"),
        ("content/test/some-file", "30d"),
    ],
)
def test_arl_fallback(requests_mock, path, expected_ttl):
    """
    Tests fallback to default TTL values when TTL cannot be
    fetched from CDN service.
    """
    templates = [
        "/fake/template-1/{ttl}/{path}",
    ]
    with CdnClient("https://cdn.example.com/", max_retry_sleep=0.001) as client:
        url = "https://cdn.example.com/content/foo/test-path-1/some-file"

        requests_mock.register_uri("HEAD", url, status_code=500)

        # Request ARLs
        arls_ft = client.get_arl_for_path(path, templates)

        # It should be successful
        arl = [item.result() for item in as_completed(arls_ft)][0]

    # It should fallback to default ttl value
    assert arl == "/fake/template-1/{ttl}/{path}".format(ttl=expected_ttl, path=path)
