from datetime import datetime

import pytest
from more_executors.futures import f_return

from pubtools.pulplib import FakeController, Client, Distributor, Repository

from pubtools._pulp.ud import UdCacheClient
from pubtools._pulp.tasks.publish import Publish, entry_point


class FakeUdCache(object):
    def __init__(self):
        self.flushed_repos = []

    def flush_repo(self, repo_id):
        self.flushed_repos.append(repo_id)
        return f_return()


class FakePublish(Publish):
    """publish with services overridden for test"""

    def __init__(self, *args, **kwargs):
        super(FakePublish, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()
        self._udcache_client = FakeUdCache()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakePublish, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client

    @property
    def udcache_client(self):
        # Super may or may not give a UD client, depends on arguments
        from_super = super(FakePublish, self).udcache_client
        if from_super:
            # If it did create one, it should be this
            assert isinstance(from_super, UdCacheClient)

        # We'll substitute our own, only if UD client is being used
        return self._udcache_client if from_super else None

    # override to generate consistent repo sequence in the logs
    def publish_with_cache_flush(self, repos, *args, **kwargs):
        return super(FakePublish, self).publish_with_cache_flush(
            sorted(repos), *args, **kwargs
        )


def _add_repo(controller):
    # test repos added to the controller
    dt1 = datetime(2019, 9, 10, 0, 0, 0)
    r1_d1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="repo1",
        last_publish=dt1,
        relative_url="content/unit/1/client",
    )
    r1_d2 = Distributor(
        id="cdn_distributor",
        type_id="rpm_rsync_distributor",
        repo_id="repo1",
        last_publish=dt1,
        relative_url="content/unit/1/client",
    )
    repo1 = Repository(
        id="repo1",
        eng_product_id=101,
        distributors=[r1_d1, r1_d2],
        relative_url="content/unit/1/client",
        mutable_urls=["mutable1", "mutable2"],
    )

    dt2 = datetime(2019, 9, 12, 0, 0, 0)
    d2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="repo2",
        last_publish=dt2,
        relative_url="content/unit/2/client",
    )
    repo2 = Repository(
        id="repo2",
        eng_product_id=102,
        distributors=[d2],
        relative_url="content/unit/2/client",
    )

    dt3 = datetime(2019, 9, 7, 0, 0, 0)
    d3 = Distributor(
        id="cdn_distributor",
        type_id="rpm_rsync_distributor",
        repo_id="repo3",
        last_publish=dt3,
        relative_url="content/unit/3/client",
    )
    repo3 = Repository(
        id="repo3",
        eng_product_id=103,
        distributors=[d3],
        relative_url="content/unit/3/client",
    )

    dt4 = datetime(2019, 9, 9, 0, 0, 0)
    d4 = Distributor(
        id="cdn_distributor",
        type_id="rpm_rsync_distributor",
        repo_id="repo4",
        last_publish=dt4,
        relative_url="content/unit/4/client",
    )
    repo4 = Repository(
        id="repo4",
        # omit eng ID -- UD can't flush this repo
        distributors=[d4],
        relative_url="content/unit/4/client",
        mutable_urls=["mutable1", "mutable2"],
    )

    controller.insert_repository(repo1)
    controller.insert_repository(repo2)
    controller.insert_repository(repo3)
    controller.insert_repository(repo4)


def test_nonexist_repos(command_tester):
    """Fails when the requested repos doesn't exist"""
    command_tester.test(
        lambda: entry_point(FakePublish),
        [
            "test-publish",
            "--pulp-url",
            "https://pulp.example.com",
            "--repo-ids",
            "repo1,repo2",
        ],
    )


def test_no_input_repos(command_tester):
    """Fails if no repos are available to publish"""
    command_tester.test(
        lambda: entry_point(FakePublish),
        ["test-publish", "--pulp-url", "https://pulp.example.com"],
    )


def test_repo_publish_only(command_tester):
    """only publishes the repo provided in the input"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--repo-ids",
                "repo1",
            ],
        )

    # the pulp repo is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo1"]


def test_repo_publish_cache_cleanup(command_tester):
    """publishes the repo provided and cleans up UD cache"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--udcache-url",
                "https://ud.example.com/",
                "--repo-ids",
                "repo1",
            ],
        )

    # pulp repo is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo1"]
    # flushed the UD object
    assert fake_publish.udcache_client.flushed_repos == ["repo1"]


def test_repo_publish_cache_cleanup_skip_ud(command_tester):
    """publishes the repo provided, doesn't clean up UD cache if repo missing eng ID"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--udcache-url",
                "https://ud.example.com/",
                "--repo-ids",
                "repo1,repo4",
            ],
        )

    # pulp repo is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == [
        "repo1",
        "repo4",
    ]
    # should not flush the UD object for repo4 because it's missing an eng ID
    assert fake_publish.udcache_client.flushed_repos == ["repo1"]


def test_publish_url_regex_filtered_repos(command_tester):
    """publishes repos with relative url matching the regex"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--repo-url-regex",
                "/unit/2/",
            ],
        )

    # repo with relative url matching '/unit/2/' is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo2"]


def test_publish_repos_published_before_a_date(command_tester):
    """publishes repos that were published before the given date"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--published-before",
                "2019-09-08",
            ],
        )

    # repo published before 2019-08-09 is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo3"]


def test_publish_repos_published_before_a_datetime(command_tester):
    """publishes repos that were published before the given datetime"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--published-before",
                "2019-09-08T01:00:00Z",
            ],
        )

    # repo published before 2019-08-09T01:00:00Z is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo3"]


def test_publish_repos_not_published_before_a_datetime(command_tester):
    """publishes repos that were published before the given datetime"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--published-before",
                "2019-09-06T23:59:00Z",
            ],
        )

    # No repo should be published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == []


def test_publish_repos_published_before_exception(command_tester):
    """Expect a parser exception when passing a bad date"""
    with pytest.raises(SystemExit) as e:
        with FakePublish() as fake_publish:
            command_tester.test(
                fake_publish.main,
                [
                    "test-publish",
                    "--pulp-url",
                    "https://pulp.example.com",
                    "--published-before",
                    "2019-09-07BADFORMAT01:00:00Z",
                ],
                allow_raise=True,
            )
        assert (
            "published-before date should be in YYYY-mm-ddTHH:MM:SSZ "
            "or YYYY-mm-dd format" in e.traceback
        )
        assert e.value.code == 2


def test_publish_filtered_repos(command_tester):
    """publishes the repos that match both url-regex and published-before filters"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--published-before",
                "2019-09-11",
                "--repo-url-regex",
                "/unit/3/",
            ],
        )

    # repo published before 2019-08-11 and
    # relative url matching '/unit/3/'is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo3"]


def test_publish_filtered_input_repos(command_tester):
    """publishes the provided repos that pass the filter"""
    with FakePublish() as fake_publish:
        fake_pulp = fake_publish.pulp_client_controller
        _add_repo(fake_pulp)

        command_tester.test(
            fake_publish.main,
            [
                "test-publish",
                "--pulp-url",
                "https://pulp.example.com",
                "--published-before",
                "2019-09-11",
                "--repo-url-regex",
                "/unit/3/",
                "--repo-ids",
                "repo1",
                "--repo-ids",
                "repo2,repo3",
            ],
        )

    # provided repos that were published before 2019-08-11 and
    # has relative url matching '/unit/3/'is published
    assert [hist.repository.id for hist in fake_pulp.publish_history] == ["repo3"]
