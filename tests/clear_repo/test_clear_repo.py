import sys

from more_executors.futures import f_return

from pubtools.pulplib import (
    Client,
    FakeController,
    FileRepository,
    FileUnit,
    YumRepository,
    ContainerImageRepository,
    ModulemdUnit,
    RpmUnit,
)
from fastpurge import FastPurgeClient

import pubtools._pulp.tasks.clear_repo
from pubtools._pulp.tasks.clear_repo import ClearRepo
from pubtools._pulp.ud import UdCacheClient


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


class FakeClearRepo(ClearRepo):
    """clear-repo with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeClearRepo, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()
        self._fastpurge_client = FakeFastPurge()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeClearRepo, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeClearRepo, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None

    @property
    def fastpurge_client(self):
        # Super may or may not give a fastpurge client, depends on arguments
        from_super = super(FakeClearRepo, self).fastpurge_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, FastPurgeClient)

        # We'll substitute our own, only if fastpurge client is being used
        return self._fastpurge_client if from_super else None


def test_missing_repos(command_tester):
    """Command fails when pointed at nonexistent repos."""
    # This one test covers the entry point

    command_tester.test(
        lambda: pubtools._pulp.tasks.clear_repo.entry_point(FakeClearRepo),
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--verbose",
            "repo1",
            "repo2",
            "repo3",
        ],
    )


def test_clear_empty_repo(command_tester, fake_collector):
    """Clearing a repo which is already empty succeeds."""

    task_instance = FakeClearRepo()

    repo = FileRepository(id="some-filerepo")

    task_instance.pulp_client_controller.insert_repository(repo)

    command_tester.test(
        task_instance.main,
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--verbose",
            "some-filerepo",
        ],
    )

    # No push items recorded
    assert not fake_collector.items


def test_clear_file_repo(command_tester, fake_collector):
    """Clearing a repo with file content succeeds."""

    task_instance = FakeClearRepo()

    repo = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )

    files = [
        FileUnit(path="hello.txt", size=123, sha256sum="a" * 64),
        FileUnit(path="with/subdir.json", size=0, sha256sum="b" * 64),
    ]

    fakepulp = task_instance.pulp_client_controller
    fakepulp.insert_repository(repo)
    fakepulp.insert_units(repo, files)

    # It should run with expected output.
    command_tester.test(
        task_instance.main,
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--pulp-insecure",
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
            "--verbose",
            "some-filerepo",
        ],
    )

    # It should record that it removed these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "state": "DELETED",
            "origin": "pulp",
            "filename": "hello.txt",
            "checksums": {"sha256": "a" * 64},
        },
        {
            "state": "DELETED",
            "origin": "pulp",
            "filename": "with/subdir.json",
            "checksums": {"sha256": "b" * 64},
        },
    ]

    # It should have published the Pulp repo
    assert [hist.repository.id for hist in fakepulp.publish_history] == [
        "some-filerepo"
    ]

    # It should have flushed these URLs
    assert sorted(task_instance.fastpurge_client.purged_urls) == [
        "https://cdn.example.com/some/publish/url/mutable1",
        "https://cdn.example.com/some/publish/url/mutable2",
    ]

    # It should have flushed these UD objects
    assert task_instance.udcache_client.flushed_repos == ["some-filerepo"]
    assert task_instance.udcache_client.flushed_products == [123]


def test_clear_file_skip_publish(command_tester):
    """Clearing a repo with file content while skipping publish succeeds."""

    task_instance = FakeClearRepo()

    repo = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=[],
    )

    files = [FileUnit(path="hello.txt", size=123, sha256sum="a" * 64)]

    task_instance.pulp_client_controller.insert_repository(repo)
    task_instance.pulp_client_controller.insert_units(repo, files)

    # It should run with expected output.
    command_tester.test(
        task_instance.main,
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--skip",
            "foo,publish,bar",
            "--verbose",
            "some-filerepo",
        ],
    )

    # It should not have published Pulp repos
    assert task_instance.pulp_client_controller.publish_history == []


def test_clear_yum_repo(command_tester, fake_collector, monkeypatch):
    """Clearing a repo with yum content succeeds."""

    task_instance = FakeClearRepo()

    repo = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )

    files = [
        RpmUnit(
            name="bash",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
        ),
        ModulemdUnit(
            name="mymod", stream="s1", version=123, context="a1c2", arch="s390x"
        ),
    ]

    task_instance.pulp_client_controller.insert_repository(repo)
    task_instance.pulp_client_controller.insert_units(repo, files)

    # Let's try setting the cache flush root via env.
    monkeypatch.setenv("FASTPURGE_ROOT_URL", "https://cdn.example2.com/")

    # It should run with expected output.
    command_tester.test(
        task_instance.main,
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--verbose",
            "--fastpurge-host",
            "fakehost-xxx.example.net",
            "--fastpurge-client-secret",
            "abcdef",
            "--fastpurge-client-token",
            "efg",
            "--fastpurge-access-token",
            "tok",
            "some-yumrepo",
        ],
    )

    # It should record that it removed these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "state": "DELETED",
            "origin": "pulp",
            "filename": "bash-1.23-1.test8.x86_64.rpm",
            "checksums": {"sha256": "a" * 64, "md5": "b" * 32},
            "signing_key": "aabbcc",
        },
        {"state": "DELETED", "origin": "pulp", "filename": "mymod:s1:123:a1c2:s390x"},
    ]

    # It should have flushed these URLs
    assert task_instance.fastpurge_client.purged_urls == [
        "https://cdn.example2.com/some/publish/url/repomd.xml"
    ]


def test_clear_container_repo(command_tester):
    """Clearing a container image repo is not allowed."""

    task_instance = FakeClearRepo()

    repo = ContainerImageRepository(id="some-containerrepo")

    task_instance.pulp_client_controller.insert_repository(repo)

    # It should run with expected output.
    command_tester.test(
        task_instance.main,
        [
            "test-clear-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "--verbose",
            "some-containerrepo",
        ],
    )
