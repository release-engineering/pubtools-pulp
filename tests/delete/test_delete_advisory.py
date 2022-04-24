from more_executors.futures import f_return
from fastpurge import FastPurgeClient

from pubtools.pulplib import (
    FakeController,
    Client,
    Criteria,
    Matcher,
    YumRepository,
    RpmUnit,
    ErratumUnit,
    ModulemdUnit,
    ErratumPackageCollection,
    ErratumPackage,
    ErratumModule,
)

from pubtools._pulp.ud import UdCacheClient

from pubtools._pulp.tasks.delete import Delete


class FakeUdCache(object):
    def __init__(self):
        self.flushed_repos = []
        self.flushed_products = []

    def flush_repo(self, repo_id):
        self.flushed_repos.append(repo_id)
        return f_return()

    def flush_product(self, product_id):
        self.flushed_products.append(product_id)
        return f_return()


class FakeFastPurge(object):
    def __init__(self):
        self.purged_urls = []

    def purge_by_url(self, urls):
        self.purged_urls.extend(urls)
        return f_return()


class FakeDeleteAdvisory(Delete):
    """clear-repo with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeDeleteAdvisory, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()
        self._fastpurge_client = FakeFastPurge()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeDeleteAdvisory, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeDeleteAdvisory, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None

    @property
    def fastpurge_client(self):
        # Super may or may not give a fastpurge client, depends on arguments
        from_super = super(FakeDeleteAdvisory, self).fastpurge_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, FastPurgeClient)

        # We'll substitute our own, only if fastpurge client is being used
        return self._fastpurge_client if from_super else None


def test_delete_advisory(command_tester, fake_collector, monkeypatch):
    """Deletion of packages and modules in advisories from provided repos succeeds"""

    repo1 = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repo2 = YumRepository(
        id="some-other-repo",
        relative_url="other/publish/url",
        mutable_urls=["repomd.xml"],
    )

    pkglist = [
        ErratumPackageCollection(
            name="colection-0",
            packages=None,
            short="",
            module=ErratumModule(
                name="mymod", stream="s1", version="123", context="a1c2", arch="s390x"
            ),
        ),
        ErratumPackageCollection(
            name="collection-1",
            packages=[
                ErratumPackage(
                    name="bash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="bash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
                ErratumPackage(
                    name="dash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="dash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
            ],
            short="",
            module=None,
        ),
    ]

    files1 = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="bash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm1",
        ),
        RpmUnit(
            name="dash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="dash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm2",
        ),
        RpmUnit(
            name="crash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="crash-1.23-1.test8.module+el8.0.0+3049+59fd2bba.x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm3",
        ),
        ModulemdUnit(
            name="mymod",
            stream="s1",
            version=123,
            context="a1c2",
            arch="s390x",
            artifacts=["crash-0:1.23-1.test8.module+el8.0.0+3049+59fd2bba.x86_64"],
            unit_id="files1_mod1",
        ),
        ErratumUnit(
            unit_id="e3e70682-c209-4cac-629f-6fbed82c07cd",
            id="RHSA-1111:22",
            summary="Dummy erratum",
            content_type_id="erratum",
            repository_memberships=["some-yumrepo"],
            pkglist=pkglist,
        ),
    ]

    files2 = [
        ErratumUnit(
            unit_id="x4e73262-e239-44ac-629f-6fbed82c07cd",
            id="RHBA-1001:22",
            summary="Other erratum",
            content_type_id="erratum",
            repository_memberships=["some-other-repo"],
            pkglist=[],
        ),
    ]

    with FakeDeleteAdvisory() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo1, files1)
        task_instance.pulp_client_controller.insert_units(repo2, files2)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-yumrepo,other-yumrepo",
                "--advisory",
                "RHSA-1111:22",
                "--advisory",
                "RHBA-1001:22",
            ],
        )

        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "dest": "some-yumrepo",
                "signing_key": None,
                "filename": "bash-1.23-1.test8.x86_64.rpm",
                "state": "DELETED",
                "build": None,
                "checksums": {"sha256": "a" * 64},
            },
            {
                "origin": "pulp",
                "src": None,
                "dest": "some-yumrepo",
                "signing_key": None,
                "filename": "crash-1.23-1.test8.x86_64.rpm",
                "state": "DELETED",
                "build": None,
                "checksums": {"sha256": "a" * 64},
            },
            {
                "origin": "pulp",
                "src": None,
                "dest": "some-yumrepo",
                "signing_key": None,
                "filename": "dash-1.23-1.test8.x86_64.rpm",
                "state": "DELETED",
                "build": None,
                "checksums": {"sha256": "a" * 64},
            },
            {
                "origin": "pulp",
                "src": None,
                "dest": "some-yumrepo",
                "signing_key": None,
                "filename": "mymod:s1:123:a1c2:s390x",
                "state": "DELETED",
                "build": None,
                "checksums": None,
            },
        ]

        # verify whether the rpms and modules were deleted from the repo on Pulp
        client = task_instance.pulp_client

        # effectively only some-yumrepo(repo1) was modified
        repos = list(
            client.search_repository(Criteria.with_id("some-yumrepo")).result()
        )
        assert len(repos) == 1
        repo = repos[0]

        # list the removed unit's unit_id
        # RPMs from the erratum package list
        unit_ids = ["files1_rpm1", "files1_rpm2"]
        # module from the erratum package list
        unit_ids.append("files1_mod1")
        # package in the above module
        unit_ids.append("files1_rpm3")
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        # deleted files are not in the repo
        files = list(repo.search_content(criteria).result())
        assert len(files) == 0

        # same files exist on Pulp as orphans
        files_search = list(client.search_content(criteria).result())
        assert len(files_search) == 4


def test_delete_advisory_in_multiple_repos(command_tester, fake_collector, monkeypatch):
    """Deletion of packages succeeds only in the requested repos when the same advisory
    is present in multiple repos"""

    repo1 = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repo2 = YumRepository(
        id="other-yumrepo",
        relative_url="other/publish/url",
        mutable_urls=["repomd.xml"],
    )

    pkglist = [
        ErratumPackageCollection(
            name="collection-1",
            packages=[
                ErratumPackage(
                    name="bash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="bash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
                ErratumPackage(
                    name="dash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="dash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
            ],
            short="",
            module=None,
        ),
    ]

    files = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="bash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm1",
        ),
        RpmUnit(
            name="dash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="dash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm2",
        ),
        RpmUnit(
            name="crash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="crash-1.23-1.test8.module+el8.0.0+3049+59fd2bba.x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm3",
        ),
        ErratumUnit(
            unit_id="x4e73262-e239-44ac-629f-6fbed82c07cd",
            id="RHBA-1001:22",
            summary="Other erratum",
            content_type_id="erratum",
            repository_memberships=["some-yumrepo", "other-yumrepo"],
            pkglist=pkglist,
        ),
    ]

    with FakeDeleteAdvisory() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo1, files)
        task_instance.pulp_client_controller.insert_units(repo2, files)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-yumrepo",
                "--advisory",
                "RHBA-1001:22",
            ],
        )

        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "some-yumrepo",
                "filename": "bash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "some-yumrepo",
                "filename": "dash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
        ]

        # verify whether the rpms were deleted from the repo on Pulp
        client = task_instance.pulp_client

        # get all the repos
        repos = list(
            client.search_repository(Criteria.with_id("some-yumrepo")).result()
        )
        assert len(repos) == 1
        repo1 = repos[0]

        repos = list(
            client.search_repository(Criteria.with_id("other-yumrepo")).result()
        )
        assert len(repos) == 1
        repo2 = repos[0]

        # list the removed unit's unit_id
        # RPMs from the erratum package list
        unit_ids = ["files1_rpm1", "files1_rpm2"]
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        # deleted packages from the advisory are not in the requested repo
        files = list(repo1.search_content(criteria).result())
        assert len(files) == 0

        # packages from the advisory still exist in the other repo
        files = list(repo2.search_content(criteria).result())
        assert len(files) == 2


def test_delete_advisory_no_repos_provided(command_tester, fake_collector, monkeypatch):
    """Deletion of packages succeeds in all the repos when the same advisory is
    present in multiple repos and repos are not provided in the request"""

    repo1 = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repo2 = YumRepository(
        id="other-yumrepo",
        relative_url="other/publish/url",
        mutable_urls=["repomd.xml"],
    )

    pkglist = [
        ErratumPackageCollection(
            name="collection-1",
            packages=[
                ErratumPackage(
                    name="bash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="bash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
                ErratumPackage(
                    name="dash",
                    version="1.23",
                    release="1.test8",
                    arch="x86_64",
                    filename="dash-1.23-1.test8_x86_64.rpm",
                    sha256sum="a" * 64,
                    md5sum="b" * 32,
                ),
            ],
            short="",
            module=None,
        ),
    ]

    files = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="bash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm1",
        ),
        RpmUnit(
            name="dash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="dash-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm2",
        ),
        RpmUnit(
            name="crash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="crash-1.23-1.test8.module+el8.0.0+3049+59fd2bba.x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="files1_rpm3",
        ),
        ErratumUnit(
            unit_id="x4e73262-e239-44ac-629f-6fbed82c07cd",
            id="RHBA-1001:22",
            summary="Other erratum",
            content_type_id="erratum",
            repository_memberships=["some-yumrepo", "other-yumrepo"],
            pkglist=pkglist,
        ),
    ]

    with FakeDeleteAdvisory() as task_instance:

        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo1, files)
        task_instance.pulp_client_controller.insert_units(repo2, files)

        # Let's try setting the cache flush root via env.
        monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--advisory",
                "RHBA-1001:22",
            ],
        )

        assert sorted(
            fake_collector.items, key=lambda pi: (pi["filename"], pi["dest"])
        ) == [
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "other-yumrepo",
                "filename": "bash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "some-yumrepo",
                "filename": "bash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "other-yumrepo",
                "filename": "dash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
            {
                "build": None,
                "checksums": {"sha256": "a" * 64},
                "dest": "some-yumrepo",
                "filename": "dash-1.23-1.test8.x86_64.rpm",
                "origin": "pulp",
                "signing_key": None,
                "src": None,
                "state": "DELETED",
            },
        ]

        # verify whether the rpms were deleted from the repo on Pulp
        client = task_instance.pulp_client

        # get all the repos
        repos = list(
            client.search_repository(Criteria.with_id("some-yumrepo")).result()
        )
        assert len(repos) == 1
        repo1 = repos[0]

        repos = list(
            client.search_repository(Criteria.with_id("other-yumrepo")).result()
        )
        assert len(repos) == 1
        repo2 = repos[0]

        # list the removed unit's unit_id
        # RPMs from the erratum package list
        unit_ids = ["files1_rpm1", "files1_rpm2"]
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        # deleted packages from the advisory are not in both the repos
        files = list(repo1.search_content(criteria).result())
        assert len(files) == 0

        files = list(repo2.search_content(criteria).result())
        assert len(files) == 0

        # same files exist on Pulp as orphans
        files_search = list(client.search_content(criteria).result())
        assert len(files_search) == 2


def test_advisory_not_found(command_tester):
    """Fails if the advisory is not found on Pulp"""

    with FakeDeleteAdvisory() as task_instance:
        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--fastpurge-host",
                "fakehost-xxx.example.net",
                "--fastpurge-client-secret",
                "abcdef",
                "--fastpurge-client-token",
                "efg",
                "--fastpurge-access-token",
                "tok",
                "--repo",
                "some-yumrepo",
                "--advisory",
                "RHSA-1111:22",
            ],
        )
