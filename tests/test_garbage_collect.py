import pytest
import attr
import datetime
from mock import Mock, patch
from more_executors.futures import f_return

from pubtools.pulplib import (
    FakeController,
    Repository,
    YumRepository,
    Task,
    InvalidDataException,
)
import pubtools.pulplib._impl.model.attr as pp_attr
import pubtools.pulplib._impl.compat_attr as attr

from pubtools._pulp.tasks.garbage_collect import GarbageCollect, entry_point


@pytest.fixture
def mock_logger():
    with patch("pubtools._pulp.tasks.garbage_collect.LOG") as mocked_info:
        yield mocked_info


def _get_time_created(d=0, h=0, s=0):
    return (
        datetime.datetime.utcnow() - datetime.timedelta(days=d, hours=h, seconds=s)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_fake_controller(*args):
    @attr.s(kw_only=True, frozen=True)
    class TestRepo(Repository):
        is_temp_repo = pp_attr.pulp_attrib(
            default=None, type=str, pulp_field="notes.pub_temp_repo"
        )

    controller = FakeController()
    for repo in args:
        yum_repo = TestRepo.from_data(repo)
        controller.insert_repository(yum_repo)
    return controller


def _run_test(*repos):
    controller = _get_fake_controller(*repos)
    gc = GarbageCollect()
    arg = ["", "--url", "http://some.url", "--verbose"]

    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.pulp_client", controller.client):
            gc.main()
    return controller


def test_add_args():
    """adds the arg to the PulpTask parser """
    gc = GarbageCollect()
    arg = ["", "--url", "http://some.url", "--verbose", "--gc-threshold", "7"]

    with patch("sys.argv", arg):
        gc_args = gc.args

    assert hasattr(gc_args, "gc_threshold")
    assert gc_args.gc_threshold == 7


def test_garbage_collect():
    """deletes the repo that confirms to garbage collect criteria"""
    repo1 = {
        "id": "rhel-test-garbage-collect-7-days-old",
        "notes": {"pub_temp_repo": True, "created": _get_time_created(7)},
    }

    repo2 = {
        "id": "rhel-test-garbage-collect-3-days-old",
        "notes": {"pub_temp_repo": True, "created": _get_time_created(3)},
    }

    controller = _run_test(repo1, repo2)
    assert len(controller.repositories) == 1
    assert controller.repositories[0].id == "rhel-test-garbage-collect-3-days-old"


def test_gc_no_repo_found(mock_logger):
    """checks no repo returned when age of repo less than gc limit"""
    repo = {
        "id": "rhel-test-garbage-collect-3-days-old",
        "notes": {"pub_temp_repo": True, "created": _get_time_created(3)},
    }

    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_no_created_date(mock_logger):
    """no repo returned for gc when creatd date is missing"""
    repo = {"id": "rhel-test-garbage-collect", "notes": {"pub_temp_repo": True}}

    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_no_temp_repo_note(mock_logger):
    """repo not returned for gc when pub_temp_repo note is missing"""
    repo = {
        "id": "rhel-test-garbage-collect",
        "notes": {"created": _get_time_created(7)},
    }

    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_error(mock_logger):
    """logs error when repo delete task returns an error reponse"""
    repo = {
        "id": "rhel-test-garbage-collect-7-days-old",
        "notes": {"pub_temp_repo": True, "created": _get_time_created(7)},
    }

    controller = _get_fake_controller(repo)
    gc = GarbageCollect()
    arg = ["", "--url", "http://some.url", "--verbose"]

    with patch("sys.argv", arg):
        with patch.object(controller.client, "_delete_repository") as repo_delete:
            with patch("pubtools._pulp.task.PulpTask.pulp_client", controller.client):
                repo_delete.return_value = f_return(
                    [
                        Task(
                            id="12334",
                            completed=True,
                            succeeded=False,
                            error_summary="Error occured",
                        )
                    ]
                )
                gc.main()

    mock_logger.error.assert_any_call("Error occured")


def test_entry_point(mock_logger):
    """check entry point does gc as expected"""

    created_time = _get_time_created(7)
    dt_created_time = datetime.datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%SZ")
    repo = {
        "id": "rhel-test-garbage-collect-7-days-old",
        "notes": {"pub_temp_repo": True, "created": created_time},
    }

    controller = _get_fake_controller(repo)
    arg = ["", "--url", "http://some.url", "--verbose"]

    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.pulp_client", controller.client):
            entry_point()

    mock_logger.info.assert_any_call(
        "Deleting %s (created on %s)",
        "rhel-test-garbage-collect-7-days-old",
        dt_created_time,
    )
    mock_logger.info.assert_any_call("Temporary repo(s) deletion completed")
