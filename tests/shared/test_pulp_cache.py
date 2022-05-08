import sys
import attr

from pubtools.pulplib import Client, FakeController, FileRepository
from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import CachingPulpClientService


class TaskWithPulpClient(CachingPulpClientService, PulpTask):
    def __init__(self, *args, **kwargs):
        super(TaskWithPulpClient, self).__init__(*args, **kwargs)
        self.pulp_ctrl = FakeController()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(TaskWithPulpClient, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_ctrl.client


def test_client_caches(monkeypatch):
    """caching_pulp_client caches the result of calls to get_repository"""

    with TaskWithPulpClient() as task:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "",
                "--pulp-url",
                "http://some.url",
            ],
        )

        # Add some repo
        task.pulp_ctrl.insert_repository(FileRepository(id="test-repo"))

        # Let's try getting it via the caching client.
        with task.caching_pulp_client as client:
            repo1 = task.caching_pulp_client.get_repository("test-repo")
            repo2 = task.caching_pulp_client.get_repository("test-repo")

        # Due to the caching, it should give me back *exactly* the same
        # object in both cases.
        assert repo1 is repo2

        # And it should fetch OK
        assert repo1.result().id == "test-repo"


def test_client_no_cache_errors(monkeypatch):
    """caching_pulp_client does not cache failed get_repository calls"""

    with TaskWithPulpClient() as task:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "",
                "--pulp-url",
                "http://some.url",
            ],
        )

        # Try getting a repo *before* it's added to the client.
        repo1 = task.caching_pulp_client.get_repository("test-repo")

        # Now add the repo and get it again.
        task.pulp_ctrl.insert_repository(FileRepository(id="test-repo"))

        repo2 = task.caching_pulp_client.get_repository("test-repo")
        repo3 = task.caching_pulp_client.get_repository("test-repo")

        # The first fetch should fail since the repo didn't exist yet.
        assert repo1.exception()

        # Since it failed, it should not have been cached and returned again.
        assert repo1 is not repo2

        # But caching worked as usual for the next two calls.
        assert repo2 is repo3

        # And those calls succeeded.
        assert repo2.result().id == "test-repo"


def test_update_invalidates(monkeypatch):
    """update_repository should invalidate the cache for that repository"""

    with TaskWithPulpClient() as task:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "",
                "--pulp-url",
                "http://some.url",
            ],
        )

        # Add some repo
        task.pulp_ctrl.insert_repository(
            FileRepository(id="test-repo", product_versions=["a", "b"])
        )

        # Let's try getting it via the caching client.
        repo1 = task.caching_pulp_client.get_repository("test-repo").result()
        repo2 = task.caching_pulp_client.get_repository("test-repo").result()

        # Initially consistent
        assert repo1.product_versions == ["a", "b"]
        assert repo2.product_versions == ["a", "b"]

        # Let's update the repo
        task.caching_pulp_client.update_repository(
            attr.evolve(repo1, product_versions=["new", "versions"])
        ).result()

        # Let's get the repo again...
        repo3 = task.caching_pulp_client.get_repository("test-repo")

        # The cache should have been smart enough to realize it can't
        # return the old cached value since the repo was updated.
        assert repo3.product_versions == ["new", "versions"]
