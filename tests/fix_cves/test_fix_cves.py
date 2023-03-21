import pytest
from mock import patch

from more_executors.futures import f_return
from fastpurge import FastPurgeClient

from pubtools.pulplib import (
    FakeController,
    Client,
    Criteria,
    YumRepository,
    ErratumUnit,
    ErratumReference,
)

from pubtools._pulp.ud import UdCacheClient
from pubtools._pulp.tasks.fix_cves import FixCves, entry_point


class FakeUdCache(object):
    def __init__(self):
        self.flushed_repos = []
        self.flushed_products = []
        self.flushed_errata = []

    def flush_repo(self, repo_id):
        self.flushed_repos.append(repo_id)
        return f_return()

    def flush_product(self, product_id):
        self.flushed_products.append(product_id)
        return f_return()

    def flush_erratum(self, erratum_id):
        self.flushed_errata.append(erratum_id)
        return f_return()


class FakeFastPurge(object):
    def __init__(self):
        self.purged_urls = []

    def purge_by_url(self, urls):
        self.purged_urls.extend(urls)
        return f_return()


class FakeFixCves(FixCves):
    """publish with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeFixCves, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()
        self._fastpurge_client = FakeFastPurge()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeFixCves, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeFixCves, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None

    @property
    def fastpurge_client(self):
        # Super may or may not give a fastpurge client, depends on arguments
        from_super = super(FakeFixCves, self).fastpurge_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, FastPurgeClient)

        # We'll substitute our own, only if fastpurge client is being used
        return self._fastpurge_client if from_super else None

    def get_affected_repos(self, erratum):
        repos = super(FakeFixCves, self).get_affected_repos(erratum)
        return sorted(repos)


def _setup_controller(controller):
    # add repo
    repo = YumRepository(
        id="repo",
        eng_product_id=101,
        distributors=[],
        relative_url="content/unit/1/client",
        mutable_urls=["mutable1", "mutable2"],
    )
    nochannel_repo = YumRepository(
        id="all-rpm-content",
        eng_product_id=100,
        distributors=[],
        relative_url="content/unit/1/all-rpm",
        mutable_urls=["mutable1", "mutable2"],
    )
    # add unit
    erratum = ErratumUnit(
        id="RHSA-1234:56",
        version="2",
        content_types=["rpm", "module"],
        references=[
            ErratumReference(
                title="title",
                href="https://example.com/test-advisory",
                type="self",
                id="self-id",
            ),
            ErratumReference(
                title="CVE-123",
                href="https://example.com/test-cve",
                type="cve",
                id="CVE-123",
            ),
        ],
        pkglist=[],
        repository_memberships=["repo", "all-rpm-content"],
    )

    controller.insert_repository(repo)
    controller.insert_repository(nochannel_repo)
    controller.insert_units(repo, [erratum])
    controller.insert_units(nochannel_repo, [erratum])


@pytest.fixture(autouse=True)
def predictable_random(monkeypatch):
    monkeypatch.setenv("PUBTOOLS_SEED", "1")
    yield


def test_fix_cves(command_tester):
    with FakeFixCves() as fake_fix_cves:
        fake_pulp = fake_fix_cves.pulp_client_controller
        client = fake_pulp.client
        _setup_controller(fake_pulp)

        command_tester.test(
            fake_fix_cves.main,
            [
                "test-fix-cves",
                "--pulp-url",
                "https://pulp.example.com",
                "--advisory",
                "RHSA-1234:56",
                "--cves",
                "CVE-987,CVE-456",
            ],
        )

    updated_erratum = list(
        client.search_content(Criteria.with_field("id", "RHSA-1234:56"))
    )
    # only single erratum exists on Pulp
    assert len(updated_erratum) == 1
    updated_erratum = updated_erratum[0]
    assert updated_erratum.id == "RHSA-1234:56"
    # erratum version bumped
    assert updated_erratum.version == "3"
    cves = sorted([ref for ref in updated_erratum.references if ref.type == "cve"])
    # only new CVEs in the erratum
    assert len(cves) == 2
    assert cves[0].id == "CVE-456"
    assert cves[1].id == "CVE-987"


def test_fix_cves_with_cache_cleanup(command_tester):
    with FakeFixCves() as fake_fix_cves:
        fake_pulp = fake_fix_cves.pulp_client_controller
        _setup_controller(fake_pulp)

        command_tester.test(
            fake_fix_cves.main,
            [
                "test-fix-cves",
                "--pulp-url",
                "https://pulp.example.com",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--fastpurge-root-url",
                "https://cdn.example.com/",
                "--udcache-url",
                "https://ud.example.com/",
                "--advisory",
                "RHSA-1234:56",
                "--cves",
                "CVE-987,CVE-456",
            ],
        )

        ud_client = fake_fix_cves.udcache_client

        assert ud_client.flushed_repos == ["all-rpm-content", "repo"]
        assert ud_client.flushed_products == []
        assert ud_client.flushed_errata == ["RHSA-1234:56"]

        fastpurge_client = fake_fix_cves.fastpurge_client

        assert fastpurge_client.purged_urls == [
            "https://cdn.example.com/content/unit/1/all-rpm/mutable1",
            "https://cdn.example.com/content/unit/1/all-rpm/mutable2",
            "https://cdn.example.com/content/unit/1/client/mutable1",
            "https://cdn.example.com/content/unit/1/client/mutable2",
        ]


def test_no_erratum_found_error(command_tester):
    with FakeFixCves() as fake_fix_cves:
        fake_pulp = fake_fix_cves.pulp_client_controller
        _setup_controller(fake_pulp)

        command_tester.test(
            fake_fix_cves.main,
            [
                "test-fix-cves",
                "--pulp-url",
                "https://pulp.example.com",
                "--advisory",
                "RHSA-123:56",
                "--cves",
                "CVE-987,CVE-456",
            ],
        )


def test_no_update_on_same_cves(command_tester):
    with FakeFixCves() as fake_fix_cves:
        fake_pulp = fake_fix_cves.pulp_client_controller
        client = fake_pulp.client
        _setup_controller(fake_pulp)

        command_tester.test(
            fake_fix_cves.main,
            [
                "test-fix-cves",
                "--pulp-url",
                "https://pulp.example.com",
                "--advisory",
                "RHSA-1234:56",
                "--cves",
                "CVE-123",
            ],
        )

    erratum = list(client.search_content(Criteria.with_field("id", "RHSA-1234:56")))

    assert len(erratum) == 1
    erratum = erratum[0]
    assert erratum.id == "RHSA-1234:56"
    # no updates. erratum version not bumped.
    assert erratum.version == "2"
    cves = sorted([ref for ref in erratum.references if ref.type == "cve"])
    # same CVE exists
    assert len(cves) == 1
    assert cves[0].id == "CVE-123"


def test_no_input_advisory_cve(command_tester):
    command_tester.test(
        lambda: entry_point(FakeFixCves),
        ["test-fix-cves", "--pulp-url", "https://pulp.example.com"],
    )
