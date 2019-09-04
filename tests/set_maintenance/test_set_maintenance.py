import pytest
from mock import patch

from pubtools.pulplib import (
    FakeController,
    Distributor,
    Repository,
    FileRepository,
    Task,
    PulpException,
)

from pubtools._pulp.tasks.set_maintenance import SetMaintenanceOn, SetMaintenanceOff


@pytest.fixture
def mock_on_logger():
    with patch(
        "pubtools._pulp.tasks.set_maintenance.set_maintenance_on.LOG"
    ) as mocked_info:
        yield mocked_info


@pytest.fixture
def mock_off_logger():
    with patch(
        "pubtools._pulp.tasks.set_maintenance.set_maintenance_off.LOG"
    ) as mocked_info:
        yield mocked_info


def maintenance_repo():
    iso_distributor = Distributor(id="iso_distributor", type_id="iso_distributor")
    repo = FileRepository(id="redhat-maintenance", distributors=[iso_distributor])

    return repo


def _get_fake_controller(*args):
    controller = FakeController()
    for repo in args:
        controller.insert_repository(repo)
    controller.insert_repository(maintenance_repo())
    return controller


def _patch_pulp_client(client):
    return patch("pubtools._pulp.services.PulpClientService.pulp_client", client)


def _run_test(cmd_args, on, *repos):
    controller = _get_fake_controller(*repos)
    if on:
        s_m = SetMaintenanceOn()
    else:
        s_m = SetMaintenanceOff()
    arg = ["", "--pulp-url", "http://some.url", "--verbose"]
    arg.extend(cmd_args)

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            s_m.main()
    return controller


def test_maintenance_on(mock_on_logger):
    """Test set maintenance by passing repo ids."""
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    cmd_args = ["--repo-id", "repo1", "repo2", "--message", "Now in Miantenance"]

    controller = _run_test(cmd_args, True, repo1, repo2)

    # upload and publish should be called once each
    assert len(controller.upload_history) == 1
    assert controller.upload_history[0].repository.id == "redhat-maintenance"
    assert len(controller.publish_history) == 1
    assert controller.publish_history[0].repository.id == "redhat-maintenance"

    # logged message should indicate both repos are set to maintenance
    mock_on_logger.info.assert_called_with(
        "Setting following repos to Maintenance Mode: \n%s", "repo1\nrepo2"
    )


def test_maintenance_on_with_regex(mock_on_logger):
    """Test set maintenance by using regex"""
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    cmd_args = ["--repo-regex", "repo1"]

    controller = _run_test(cmd_args, True, repo1, repo2)

    # only repo1 should be set to maintenance
    mock_on_logger.info.assert_called_with(
        "Setting following repos to Maintenance Mode: \n%s", "repo1"
    )


def test_maintenance_on_with_repo_not_exists():
    """Set maintenance to non-existed repo in server will fail"""
    repo1 = Repository(id="repo1")
    cmd_args = ["--repo-id", "repo2"]

    with pytest.raises(PulpException):
        _run_test(cmd_args, True, repo1)


def test_maintenance_off(mock_off_logger):
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    # set maintenance first, get the controller
    cmd_args = ["--repo-id", "repo1", "repo2"]
    controller = _run_test(cmd_args, True, repo1, repo2)

    args = [
        "",
        "--pulp-url",
        "http://some.url",
        "--verbose",
        "--repo-id",
        "repo1",
        "repo2",
    ]
    s_m = SetMaintenanceOff()

    # remove repos from maintenance mode by id
    with patch("sys.argv", args):
        with _patch_pulp_client(controller.client):
            s_m.main()

    mock_off_logger.info.assert_called_with(
        "Following repositories will be removed from Maintenance Mode: \n%s",
        "repo1\nrepo2",
    )

    # upload and publish should be called twice eachassert len(controller.upload_history) == 1
    assert len(controller.upload_history) == 2
    assert len(controller.publish_history) == 2


def test_maintenance_off_with_regex(mock_off_logger):
    repo1 = Repository(id="repo1")
    repo2 = Repository(id="repo2")

    # set maintenance first, get the controller
    cmd_args = ["--repo-id", "repo1", "repo2"]
    controller = _run_test(cmd_args, True, repo1, repo2)

    args = ["", "--pulp-url", "http://some.url", "--verbose", "--repo-regex", "repo1"]
    s_m = SetMaintenanceOff()

    # remove repos from maintenance mode by id
    with patch("sys.argv", args):
        with _patch_pulp_client(controller.client):
            s_m.main()

    mock_off_logger.info.assert_called_with(
        "Following repositories will be removed from Maintenance Mode: \n%s", "repo1"
    )


def test_maintenance_off_with_repo_not_in_maintenance(mock_off_logger):
    controller = _get_fake_controller()
    args = ["", "--pulp-url", "http://some.url", "--verbose", "--repo-id", "repo1"]
    s_m = SetMaintenanceOff()

    # remove repos from maintenance mode by id
    with patch("sys.argv", args):
        with _patch_pulp_client(controller.client):
            s_m.main()

    mock_off_logger.warn.assert_called_with(
        "Repository %s is not in Maintenance Mode", "repo1"
    )
