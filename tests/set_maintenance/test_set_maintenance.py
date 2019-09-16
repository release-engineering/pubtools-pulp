import pytest

from pubtools.pulplib import (
    Client,
    FakeController,
    Distributor,
    Repository,
    FileRepository,
)

from pubtools._pulp.tasks.set_maintenance import SetMaintenanceOn, SetMaintenanceOff
import pubtools._pulp.tasks.set_maintenance.set_maintenance_on


class FakeSetMaintenanceOn(SetMaintenanceOn):
    def __init__(self, *args, **kwargs):
        super(FakeSetMaintenanceOn, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()

    @property
    def pulp_client(self):
        # Super should give a Pulp client
        assert isinstance(super(FakeSetMaintenanceOn, self).pulp_client, Client)
        # But we'll substitute our own
        return self.pulp_client_controller.client


class FakeSetMaintenanceOff(SetMaintenanceOff):
    def __init__(self, *args, **kwargs):
        super(FakeSetMaintenanceOff, self).__init__(*args, **kwargs)
        self.pulp_client_controller = FakeController()

    @property
    def pulp_client(self):
        """Doesn't check if super gives a Pulp client or not:
            1. it's been checked in maintenance on, which has the same client
            2. we need to pre-set maintenance mode before test, check would
               break that since there's no service args passed.
        """
        return self.pulp_client_controller.client


def get_task_instance(on, *repos):

    iso_distributor = Distributor(
        id="iso_distributor",
        type_id="iso_distributor",
        relative_url="root",
        repo_id="redhat-maintenance",
    )
    maint_repo = FileRepository(id="redhat-maintenance", distributors=[iso_distributor])

    if on:
        task_instance = FakeSetMaintenanceOn()
        task_instance.pulp_client_controller.insert_repository(maint_repo)
    else:
        task_instance = FakeSetMaintenanceOff()
        task_instance.pulp_client_controller.insert_repository(maint_repo)
        # if unset maintenance mode, we need to pre-set maintenance first
        report = task_instance.pulp_client.get_maintenance_report().result()
        report = report.add([repo.id for repo in repos])
        task_instance.pulp_client.set_maintenance(report)

    for repo in list(repos):
        task_instance.pulp_client_controller.insert_repository(repo)

    return task_instance


def test_maintenance_on(command_tester):
    """Test set maintenance by passing repo ids."""
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    task_instance = get_task_instance(True, repo1, repo2)

    command_tester.test(
        task_instance.main,
        [
            "test-maintenance-on",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-ids",
            "repo1",
            "repo2",
            "--message",
            "Now in Maintenance",
        ],
    )

    controller = task_instance.pulp_client_controller
    # upload and publish should be called once each
    assert len(controller.upload_history) == 1
    assert controller.upload_history[0].repository.id == "redhat-maintenance"
    assert len(controller.publish_history) == 1
    assert controller.publish_history[0].repository.id == "redhat-maintenance"


def test_maintenance_on_with_regex(command_tester):
    """Test set maintenance by using regex"""
    repo1 = Repository(id="repo1")

    dist1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        relative_url="rhel/7",
        repo_id="repo2",
    )

    repo2 = Repository(id="repo2", distributors=(dist1,))

    task_instance = get_task_instance(True, repo1, repo2)

    command_tester.test(
        task_instance.main,
        [
            "test-maintenance-on",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-url-regex",
            "rhel",
        ],
    )


def test_maintenance_on_with_repo_not_exists(command_tester):
    """Set maintenance to non-existed repo in server will fail"""
    repo1 = Repository(id="repo1")

    task_instance = get_task_instance(True, repo1)

    command_tester.test(
        lambda: pubtools._pulp.tasks.set_maintenance.set_maintenance_on.entry_point(
            lambda: task_instance
        ),
        [
            "test-maintenance-on",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-ids",
            "repo1",
            "repo2",
        ],
    )


def test_maintenance_off(command_tester):
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    task_instance = get_task_instance(False, repo1, repo2)

    command_tester.test(
        task_instance.main,
        [
            "test-maintenance-off",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-ids",
            "repo2",
        ],
    )

    controller = task_instance.pulp_client_controller
    # upload and publish should be called twice each
    assert len(controller.upload_history) == 2
    assert len(controller.publish_history) == 2


def test_maintenance_off_with_regex(command_tester):
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2", relative_url="rhel/7/")

    task_instance = get_task_instance(False, repo1, repo2)

    command_tester.test(
        task_instance.main,
        [
            "test-maintenance-off",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-url-regex",
            "rhel",
        ],
    )


def test_maintenance_off_with_repo_not_in_maintenance(command_tester):
    repo1 = Repository(id="repo1")

    task_instance = get_task_instance(False, repo1)

    command_tester.test(
        task_instance.main,
        [
            "test-maintenance-off",
            "--pulp-url",
            "http://some.url",
            "--verbose",
            "--repo-ids",
            "repo1",
            "repo2",
        ],
    )
