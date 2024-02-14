from more_executors.futures import f_return
from pubtools.pulplib import (
    Client,
    ContainerImageRepository,
    ErratumUnit,
    FakeController,
    FileRepository,
    FileUnit,
    ModulemdUnit,
    RpmUnit,
    YumRepository,
)

import pubtools._pulp.tasks.copy_repo
from pubtools._pulp.tasks.copy_repo import CopyRepo
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


class FakeCopyRepo(CopyRepo):
    """copy-repo with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeCopyRepo, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeCopyRepo, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeCopyRepo, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None


def test_missing_repos(command_tester):
    """Command fails when pointed at nonexistent repos."""
    # This one test covers the entry point

    command_tester.test(
        lambda: pubtools._pulp.tasks.copy_repo.entry_point(FakeCopyRepo),
        [
            "test-copy-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "repo1,repo2",
            "repo3,repo4",
        ],
    )


def test_invalid_repo_pair(command_tester, caplog):
    command_tester.test(
        lambda: pubtools._pulp.tasks.copy_repo.entry_point(FakeCopyRepo),
        [
            "test-copy-repo",
            "--pulp-url",
            "https://pulp.example.com/",
            "repo1, ",
        ],
    )
    assert (
        "Pair(s) must contain two repository IDs, source and destination. Got: ['repo1', ' ']"
        in caplog.text
    )


def test_copy_empty_repo(command_tester, fake_collector):
    """Copying a repo which is empty succeeds."""

    with FakeCopyRepo() as task_instance:
        repoA = FileRepository(id="some-filerepo")
        repoB = FileRepository(id="another-filerepo")

        task_instance.pulp_client_controller.insert_repository(repoA)
        task_instance.pulp_client_controller.insert_repository(repoB)

        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "some-filerepo,another-filerepo",
            ],
        )

    # No push items recorded
    assert not fake_collector.items


def test_copy_file_repo(command_tester, fake_collector):
    """Copying a repo with file content succeeds."""

    repoA = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )
    repoB = FileRepository(
        id="another-filerepo",
        eng_product_id=456,
        relative_url="another/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )

    files = [
        FileUnit(path="hello.txt", size=123, sha256sum="a" * 64),
        FileUnit(path="with/subdir.json", size=0, sha256sum="b" * 64),
    ]

    with FakeCopyRepo() as task_instance:
        fakepulp = task_instance.pulp_client_controller
        fakepulp.insert_repository(repoA)
        fakepulp.insert_repository(repoB)
        # Populate source repository.
        fakepulp.insert_units(repoA, files)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "--pulp-insecure",
                "--udcache-url",
                "https://ud.example.com/",
                "some-filerepo,another-filerepo",
            ],
        )

    # It should record that it copied these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-filerepo",
            "filename": "hello.txt",
            "checksums": {"sha256": "a" * 64},
            "build": None,
            "signing_key": None,
        },
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-filerepo",
            "filename": "with/subdir.json",
            "checksums": {"sha256": "b" * 64},
            "build": None,
            "signing_key": None,
        },
    ]

    # It should have published the copied Pulp repo
    assert [hist.repository.id for hist in fakepulp.publish_history] == [
        "another-filerepo",
    ]

    # It should have flushed the copied UD object
    assert task_instance.udcache_client.flushed_repos == [
        "another-filerepo",
    ]
    assert task_instance.udcache_client.flushed_products == [456]


def test_copy_file_skip_publish(command_tester):
    """Copying a repo with file content while skipping publish succeeds."""

    repoA = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=[],
    )
    repoB = FileRepository(
        id="another-filerepo",
        eng_product_id=456,
        relative_url="another/publish/url",
        mutable_urls=[],
    )

    files = [FileUnit(path="hello.txt", size=123, sha256sum="a" * 64)]

    with FakeCopyRepo() as task_instance:
        fakepulp = task_instance.pulp_client_controller
        fakepulp.insert_repository(repoA)
        fakepulp.insert_repository(repoB)
        # Populate source repository.
        fakepulp.insert_units(repoA, files)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "--skip",
                "foo,publish,bar",
                "some-filerepo,another-filerepo",
            ],
        )

    # It should not have published Pulp repos
    assert task_instance.pulp_client_controller.publish_history == []


def test_copy_yum_repo(command_tester, fake_collector, monkeypatch):
    """Copying a repo with yum content succeeds."""

    repoA = YumRepository(
        id="some-yumrepo",
        relative_url="some/publish/url",
        mutable_urls=["repomd.xml"],
    )
    repoB = YumRepository(
        id="another-yumrepo",
        relative_url="another/publish/url",
        mutable_urls=["repomd.xml"],
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
        ErratumUnit(
            id="RHSA-2021:0672",
            # ...and the advisory already exists in some Pulp, repos, maybe with
            # some overlap
            repository_memberships=[
                "all-rpm-content",
                "all-rpm-content-ff",
                "existing1",
                "existing2",
            ],
        ),
    ]

    with FakeCopyRepo() as task_instance:
        fakepulp = task_instance.pulp_client_controller
        fakepulp.insert_repository(repoA)
        fakepulp.insert_repository(repoB)
        # Populate source repository.
        fakepulp.insert_units(repoA, files)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "some-yumrepo,another-yumrepo",
            ],
        )

    # It should record that it copied these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-yumrepo",
            "filename": "RHSA-2021:0672",
            "checksums": None,
            "signing_key": None,
            "build": None,
        },
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-yumrepo",
            "filename": "bash-1.23-1.test8.x86_64.rpm",
            "checksums": {"sha256": "a" * 64},
            "signing_key": None,
            "build": None,
        },
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-yumrepo",
            "filename": "mymod:s1:123:a1c2:s390x",
            "checksums": None,
            "signing_key": None,
            "build": None,
        },
    ]


def test_copy_container_repo(command_tester):
    """Copying a container image repo is not allowed."""

    with FakeCopyRepo() as task_instance:
        repoA = ContainerImageRepository(id="some-containerrepo")
        repoB = ContainerImageRepository(id="another-containerrepo")

        task_instance.pulp_client_controller.insert_repository(repoA)
        task_instance.pulp_client_controller.insert_repository(repoB)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "some-containerrepo,another-containerrepo",
            ],
        )


def test_copy_repo_multiple_content_types(command_tester, fake_collector):
    """Test copying a Yum repo given multiple content type values."""

    repoA = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repoB = YumRepository(
        id="another-yumrepo",
        relative_url="another/publish/url",
        mutable_urls=["repomd.xml"],
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

    with FakeCopyRepo() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repoA)
        task_instance.pulp_client_controller.insert_repository(repoB)
        task_instance.pulp_client_controller.insert_units(repoA, files)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-copy-repo",
                "--pulp-url",
                "https://pulp.example.com/",
                "--content-type",
                "rpm",
                "--content-type",
                "modulemd",
                "--content-type",
                "iso",
                "--content-type",
                "erratum",
                "some-yumrepo,another-yumrepo",
            ],
        )

    # test the new ClearRepo argument handling for --content-type
    # produces the expected output
    assert task_instance.args.content_type == ["rpm", "modulemd", "iso", "erratum"]

    # It should record that it copied these push items:
    assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-yumrepo",
            "filename": "bash-1.23-1.test8.x86_64.rpm",
            "checksums": {"sha256": "a" * 64},
            "signing_key": None,
            "build": None,
        },
        {
            "state": "PUSHED",
            "origin": "pulp",
            "src": None,
            "dest": "another-yumrepo",
            "filename": "mymod:s1:123:a1c2:s390x",
            "checksums": None,
            "signing_key": None,
            "build": None,
        },
    ]
