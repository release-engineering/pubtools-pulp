from more_executors.futures import f_return
from pubtools.pulplib import (
    Client,
    Criteria,
    FakeController,
    FileRepository,
    FileUnit,
    Matcher,
    ModulemdUnit,
    RpmUnit,
    YumRepository,
)

from pubtools._pulp.tasks.delete import Delete, entry_point
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


class FakeDeletePackages(Delete):
    """clear-repo with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakeDeletePackages, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeDeletePackages, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakeDeletePackages, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None


def test_delete_rpms_without_signing_keys(command_tester):
    """Fails when either signing key or --allow-unsigned is not provided"""

    command_tester.test(
        lambda: entry_point(FakeDeletePackages),
        [
            "test-delete",
            "--pulp-url",
            "https://pulp.example.com/",
            "--file",
            "some.rpm",
            "--repo",
            "some-yumrepo",
        ],
    )


def test_delete_rpms(command_tester, fake_collector, monkeypatch):
    """Deleting RPMs from repos succeeds"""

    repo1 = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repo2 = YumRepository(
        id="other-yumrepo",
        relative_url="other/publish/url",
        mutable_urls=["repomd.xml"],
    )

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
            unit_id="file1_rpm1",
        ),
        RpmUnit(
            name="dash",
            version="2.25",
            release="1.test8",
            arch="x86_64",
            filename="dash-2.25-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="file1_rpm2",
        ),
        ModulemdUnit(
            name="mymod",
            stream="s1",
            version=123,
            context="a1c2",
            arch="s390x",
            unit_id="file1_mod1",
        ),
    ]

    files2 = [
        RpmUnit(
            name="crash",
            version="3.30",
            release="1.test8",
            arch="s390x",
            filename="crash-3.30-1.test8_s390x.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="file2_rpm1",
        )
    ]

    files3 = [
        RpmUnit(
            name="rash",
            version="1.30",
            release="1.test8",
            arch="noarch",
            filename="rash-1.30-1.test8_noarch.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="file3_rpm1",
        )
    ]

    undeleted = [
        RpmUnit(
            name="exist",
            version="1.34",
            release="1.test8",
            arch="noarch",
            filename="exist-1.34-1.test8_noarch.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="undeleted_rpm1",
        )
    ]

    files1.extend(files3)
    files1.extend(undeleted)
    files2.extend(files3)

    with FakeDeletePackages() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo1, files1)
        task_instance.pulp_client_controller.insert_units(repo2, files2)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--repo",
                "some-yumrepo,other-yumrepo",
                "--repo",
                "some-other-repo",
                "--file",
                "bash-1.23-1.test8_x86_64.rpm",
                "--file",
                "dash-2.25-1.test8_x86_64.rpm,crash-3.30-1.test8_s390x.rpm",
                "--file",
                "trash-1.0-1.test8_noarch.rpm,rash-1.30-1.test8_noarch.rpm",
                "--signing-key",
                "aabbcc",
            ],
        )

    # It should record that it removed these push items:
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
            "dest": "other-yumrepo",
            "signing_key": None,
            "filename": "crash-3.30-1.test8.s390x.rpm",
            "state": "DELETED",
            "build": None,
            "checksums": {"sha256": "a" * 64},
        },
        {
            "origin": "pulp",
            "src": None,
            "dest": "some-yumrepo",
            "signing_key": None,
            "filename": "dash-2.25-1.test8.x86_64.rpm",
            "state": "DELETED",
            "build": None,
            "checksums": {"sha256": "a" * 64},
        },
        {
            "origin": "pulp",
            "src": None,
            "dest": "other-yumrepo",
            "signing_key": None,
            "filename": "rash-1.30-1.test8.noarch.rpm",
            "state": "DELETED",
            "build": None,
            "checksums": {"sha256": "a" * 64},
        },
        {
            "origin": "pulp",
            "src": None,
            "dest": "some-yumrepo",
            "signing_key": None,
            "filename": "rash-1.30-1.test8.noarch.rpm",
            "state": "DELETED",
            "build": None,
            "checksums": {"sha256": "a" * 64},
        },
    ]

    # verify whether files were deleted on Pulp
    client = task_instance.pulp_client

    # get the repo where the files were deleted
    repos = sorted(
        list(
            client.search_repository(
                Criteria.with_id(["some-yumrepo", "other-yumrepo"])
            ).result()
        ),
        key=lambda r: r.id,
    )
    assert len(repos) == 2
    r2, r1 = repos

    assert r1.id == repo1.id
    assert r2.id == repo2.id

    # criteria with the unit_ids
    # critera1 for files1 in repo1
    unit_ids = []
    for f in files1:
        unit_ids.append(f.unit_id)
    criteria1 = Criteria.with_field("unit_id", Matcher.in_(unit_ids))
    # critera2 for files2 in repo2
    unit_ids = []
    for f in files2:
        unit_ids.append(f.unit_id)
    criteria2 = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

    # files are not in the repo1 except undeleted rpm and module
    result1 = sorted(
        list(r1.search_content(criteria1).result()), key=lambda v: v.unit_id
    )
    assert len(result1) == 2
    # modulemd in files1
    assert result1[0].unit_id == files1[2].unit_id
    # undeleted file
    assert result1[1].unit_id == undeleted[0].unit_id

    # files are not in repo2
    result2 = list(r2.search_content(criteria1).result())
    assert len(result2) == 0

    # All the files exist on Pulp
    files_search = list(
        client.search_content(Criteria.or_(criteria1, criteria2)).result()
    )
    assert len(files_search) == 6


def test_delete_unsigned_rpms(command_tester, fake_collector, monkeypatch):
    """Deleting unsigned RPMs from repos succeeds"""

    repo = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )

    files = [
        RpmUnit(
            name="signed",
            version="1.23",
            release="1.test8",
            arch="x86_64",
            filename="signed-1.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            unit_id="signed_rpm",
        ),
        RpmUnit(
            name="unsigned",
            version="2.25",
            release="1.test8",
            arch="x86_64",
            filename="unsigned-2.25-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key=None,
            unit_id="unsigned_rpm",
        ),
    ]

    with FakeDeletePackages() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repo)
        task_instance.pulp_client_controller.insert_units(repo, files)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--repo",
                "some-yumrepo",
                "--file",
                "unsigned-2.25-1.test8_x86_64.rpm,signed-1.23-1.test8_x86_64.rpm",
                "--allow-unsigned",
            ],
        )

        # It should record that it removed these push items:
        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "dest": "some-yumrepo",
                "signing_key": None,
                "filename": "unsigned-2.25-1.test8.x86_64.rpm",
                "state": "DELETED",
                "build": None,
                "checksums": {"sha256": "a" * 64},
            }
        ]

        # verify whether files were deleted on Pulp
        client = task_instance.pulp_client

        # get the repo where the files were deleted
        repos = list(
            client.search_repository(Criteria.with_id("some-yumrepo")).result()
        )
        assert len(repos) == 1
        repo = repos[0]

        # criteria with the unit_ids
        unit_ids = []
        for f in files:
            unit_ids.append(f.unit_id)
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        # unsigned RPM is deleted, only signed RPM left in the repo
        result_files = list(repo.search_content(criteria).result())
        assert len(result_files) == 1
        assert files[0].filename == "signed-1.23-1.test8_x86_64.rpm"


def test_delete_modules(command_tester, fake_collector, monkeypatch):
    """Deleting modules and it's artifacts from repos succeeds"""

    repo = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )
    repo2 = YumRepository(
        id="other-yumrepo",
        relative_url="other/publish/url",
        mutable_urls=["repomd.xml"],
    )

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
            provides=[],
            requires=[],
            unit_id="rpm1",
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
            provides=[],
            requires=[],
            unit_id="rpm2",
        ),
        ModulemdUnit(
            name="mymod",
            stream="s1",
            version=123,
            context="a1c2",
            arch="s390x",
            artifacts=[
                "bash-0:1.23-1.test8_x86_64",
                "dash-0:1.23-1.test8_x86_64",
                "smash-0.24-1.test8_x86_64",
            ],
            unit_id="module1",
        ),
    ]

    files2 = [
        RpmUnit(
            name="smash",
            version="0.24",
            release="1.test8",
            arch="x86_64",
            filename="smash-0.24-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            provides=[],
            requires=[],
            unit_id="rpm3",
        ),
        RpmUnit(
            name="rash",
            version="2.23",
            release="1.test8",
            arch="x86_64",
            filename="rash-2.23-1.test8_x86_64.rpm",
            sha256sum="a" * 64,
            md5sum="b" * 32,
            signing_key="aabbcc",
            provides=[],
            requires=[],
            unit_id="rpm4",
        ),
    ]

    with FakeDeletePackages() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repo)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo, files)
        task_instance.pulp_client_controller.insert_units(repo2, files2)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--repo",
                "some-yumrepo",
                "--repo",
                "other-yumrepo",
                "--file",
                "mymod:s1:123:a1c2:s390x",
                "--signing-key",
                "aabbcc",
            ],
        )

        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "some-yumrepo",
                "checksums": {"sha256": "a" * 64},
                "signing_key": None,
                "filename": "bash-1.23-1.test8.x86_64.rpm",
            },
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "some-yumrepo",
                "checksums": {"sha256": "a" * 64},
                "signing_key": None,
                "filename": "dash-1.23-1.test8.x86_64.rpm",
            },
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "some-yumrepo",
                "checksums": None,
                "signing_key": None,
                "filename": "mymod:s1:123:a1c2:s390x",
            },
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "other-yumrepo",
                "checksums": {"sha256": "a" * 64},
                "signing_key": None,
                "filename": "smash-0.24-1.test8.x86_64.rpm",
            },
        ]

        # verify whether files were deleted on Pulp
        client = task_instance.pulp_client

        # get the repos where the files were deleted
        repos = list(
            client.search_repository(Criteria.with_id("some-yumrepo")).result()
        )
        assert len(repos) == 1
        repo = repos[0]

        repos2 = list(
            client.search_repository(Criteria.with_id("other-yumrepo")).result()
        )
        assert len(repos2) == 1
        repo2 = repos2[0]

        # criteria with the unit_ids
        unit_ids = []
        for f in files:
            unit_ids.append(f.unit_id)
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        unit_ids2 = []
        for f in files2:
            unit_ids2.append(f.unit_id)
        criteria2 = Criteria.with_field("unit_id", Matcher.in_(unit_ids2))

        # deleted files are not in "some-yumrepo" repo
        files = list(repo.search_content(criteria).result())
        assert len(files) == 0

        # there's one file in "other-yumrepo" repo as only one was
        # listed in module's artifacts and was deleted with the module
        files2 = list(repo2.search_content(criteria2).result())
        assert len(files2) == 1
        assert files2[0].filename == "rash-2.23-1.test8_x86_64.rpm"

        # same files exist on Pulp as orphans
        files_search = list(client.search_content(criteria).result())
        assert len(files_search) == 3

        files_search = list(client.search_content(criteria2).result())
        assert len(files_search) == 2


def test_delete_files(command_tester, fake_collector, monkeypatch):
    """Deleting files from repos succeeds"""

    repo1 = FileRepository(
        id="some-filerepo",
        eng_product_id=123,
        relative_url="some/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )
    repo2 = FileRepository(
        id="other-filerepo",
        eng_product_id=123,
        relative_url="other/publish/url",
        mutable_urls=["mutable1", "mutable2"],
    )

    files1 = [
        FileUnit(path="hello.iso", size=123, sha256sum="a" * 64, unit_id="files1_f1"),
        FileUnit(path="some.iso", size=454435, sha256sum="b" * 64, unit_id="files1_f2"),
    ]

    files2 = [
        FileUnit(path="other.iso", size=123, sha256sum="a" * 64, unit_id="files2_f1")
    ]

    with FakeDeletePackages() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_repository(repo2)
        task_instance.pulp_client_controller.insert_units(repo1, files1)
        task_instance.pulp_client_controller.insert_units(repo2, files2)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--repo",
                "some-filerepo",
                "--file",
                "some.iso,hello.iso",
                "--file",
                "other.iso",
                "--file",
                "random.txt",
            ],
        )

        # deleted units are collected
        assert sorted(fake_collector.items, key=lambda pi: pi["filename"]) == [
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "some-filerepo",
                "checksums": {"sha256": "a" * 64},
                "signing_key": None,
                "filename": "hello.iso",
            },
            {
                "origin": "pulp",
                "src": None,
                "state": "DELETED",
                "build": None,
                "dest": "some-filerepo",
                "checksums": {"sha256": "b" * 64},
                "signing_key": None,
                "filename": "some.iso",
            },
        ]

        # verify whether files were deleted on Pulp
        client = task_instance.pulp_client

        # get the repo where the files were deleted
        repos = list(
            client.search_repository(Criteria.with_id("some-filerepo")).result()
        )
        assert len(repos) == 1
        repo = repos[0]

        unit_ids = []
        for f in files1:
            unit_ids.append(f.unit_id)
        criteria = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

        # deleted files are not in the repo
        files = list(repo.search_content(criteria).result())
        assert len(files) == 0

        # same files exist on Pulp as orphans
        files_search = list(client.search_content(criteria).result())
        assert len(files_search) == 2


def test_no_file_provided(command_tester):
    """Fails if no files or advisories provided"""

    command_tester.test(
        lambda: entry_point(FakeDeletePackages),
        [
            "test-delete",
            "--pulp-url",
            "https://pulp.example.com/",
            "--repo",
            "some-filerepo",
        ],
    )


def test_no_repo_provided(command_tester):
    """Fails if no repos are provided"""

    command_tester.test(
        lambda: entry_point(FakeDeletePackages),
        [
            "test-delete",
            "--pulp-url",
            "https://pulp.example.com/",
            "--file",
            "some.iso",
        ],
    )


def test_delete_rpms_skip_publish(command_tester, fake_collector, monkeypatch):
    """Deleting RPMs with skip publish succeeds"""

    repo1 = YumRepository(
        id="some-yumrepo", relative_url="some/publish/url", mutable_urls=["repomd.xml"]
    )

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
            unit_id="file1_rpm1",
        ),
        ModulemdUnit(
            name="mymod",
            stream="s1",
            version=123,
            context="a1c2",
            arch="s390x",
            unit_id="file1_mod1",
        ),
    ]

    with FakeDeletePackages() as task_instance:
        task_instance.pulp_client_controller.insert_repository(repo1)
        task_instance.pulp_client_controller.insert_units(repo1, files1)

        # It should run with expected output.
        command_tester.test(
            task_instance.main,
            [
                "test-delete",
                "--pulp-url",
                "https://pulp.example.com/",
                "--repo",
                "some-yumrepo",
                "--file",
                "bash-1.23-1.test8_x86_64.rpm",
                "--signing-key",
                "aabbcc",
                "--skip",
                "publish",
            ],
        )

    # It should record that it removed these push items:
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
    ]

    # verify whether files were deleted on Pulp
    client = task_instance.pulp_client

    # get the repo where the files were deleted
    repos = sorted(
        list(client.search_repository(Criteria.with_id(["some-yumrepo"])).result()),
        key=lambda r: r.id,
    )
    assert len(repos) == 1

    assert repos[0].id == repo1.id

    # criteria with the unit_ids
    # critera1 for files1 in repo1
    unit_ids = []
    for f in files1:
        unit_ids.append(f.unit_id)
    criteria1 = Criteria.with_field("unit_id", Matcher.in_(unit_ids))

    # files are not in the repo1 except module
    result1 = sorted(
        list(repos[0].search_content(criteria1).result()), key=lambda v: v.unit_id
    )
    assert len(result1) == 1
    # modulemd in files1
    assert result1[0].unit_id == files1[1].unit_id

    # All the files exist on Pulp
    files_search = list(client.search_content(criteria1).result())
    assert len(files_search) == 2
