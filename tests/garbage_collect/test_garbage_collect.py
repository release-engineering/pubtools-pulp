import os

import pytest
import datetime
from mock import Mock, patch
from more_executors.futures import f_return

from pubtools.pulplib import (
    FakeController,
    Repository,
    RpmUnit,
    RpmDependency,
    YumRepository,
    Task,
    InvalidDataException,
)

from pubtools._pulp.tasks.garbage_collect import GarbageCollect, entry_point
import pubtools._pulp.tasks.garbage_collect as gc_module


@pytest.fixture
def mock_logger():
    with patch("pubtools._pulp.tasks.garbage_collect.LOG") as mocked_info:
        yield mocked_info


def _get_created(d=0, h=0, s=0):
    return datetime.datetime.utcnow() - datetime.timedelta(days=d, hours=h, seconds=s)


def _get_fake_controller(*args):
    controller = FakeController()
    for repo in args:
        controller.insert_repository(repo)
    return controller


def _patch_pulp_client(client):
    return patch("pubtools._pulp.services.PulpClientService.pulp_client", client)


def _run_test(*repos):
    controller = _get_fake_controller(*repos)
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            gc.main()
    return controller


def test_add_args():
    """adds the arg to the PulpTask parser"""
    gc = GarbageCollect()
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "--gc-threshold",
        "7",
        "--arc-threshold",
        "90",
    ]

    with patch("sys.argv", arg):
        gc_args = gc.args

    assert hasattr(gc_args, "gc_threshold")
    assert gc_args.gc_threshold == 7
    assert hasattr(gc_args, "arc_threshold")
    assert gc_args.arc_threshold == 90


def test_garbage_collect():
    """deletes the repo that confirms to garbage collect criteria"""
    repo1 = Repository(
        id="rhel-test-garbage-collect-7-days-old",
        created=_get_created(7),
        is_temporary=True,
    )
    repo2 = Repository(
        id="rhel-test-garbage-collect-3-days-old",
        created=_get_created(3),
        is_temporary=True,
    )
    controller = _run_test(repo1, repo2)
    assert len(controller.repositories) == 1
    assert controller.repositories[0].id == "rhel-test-garbage-collect-3-days-old"


def test_gc_no_repo_found(mock_logger):
    """checks no repo returned when age of repo less than gc limit"""
    repo = Repository(
        id="rhel-test-garbage-collect-3-days-old",
        created=_get_created(3),
        is_temporary=True,
    )
    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_no_created_date(mock_logger):
    """no repo returned for gc when creatd date is missing"""
    repo = Repository(id="rhel-test-garbage-collect", is_temporary=True)

    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_no_temp_repo_note(mock_logger):
    """repo not returned for gc when pub_temp_repo note is missing"""
    repo = Repository(id="rhel-test-garbage-collect", created=_get_created(7))
    _run_test(repo)
    mock_logger.info.assert_any_call("No repo(s) found older than %s day(s)", 5)


def test_gc_error(mock_logger):
    """logs error when repo delete task returns an error reponse"""
    repo = Repository(
        id="rhel-test-garbage-collect-7-days-old",
        created=_get_created(7),
        is_temporary=True,
    )
    controller = _get_fake_controller(repo)
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with patch.object(controller.client, "_delete_repository") as repo_delete:
            with _patch_pulp_client(controller.client):
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
    created_time = _get_created(7)
    repo = Repository(
        id="rhel-test-garbage-collect-7-days-old",
        created=created_time,
        is_temporary=True,
    )
    controller = _get_fake_controller(repo)
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            entry_point()

    mock_logger.info.assert_any_call(
        "Deleting %s (created on %s)",
        "rhel-test-garbage-collect-7-days-old",
        created_time,
    )
    mock_logger.info.assert_any_call("Temporary repo(s) deletion completed")


def test_add_arc_args():
    """adds the arg to the PulpTask parser"""
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url", "--arc-threshold", "7"]

    with patch("sys.argv", arg):
        gc_args = gc.args

    assert hasattr(gc_args, "arc_threshold")
    assert gc_args.arc_threshold == 7


def test_arc_garbage_collect(mock_logger):
    """deletes all-rpm-content content that confirms to garbage collect criteria"""
    repo = Repository(
        id="all-rpm-content",
        created=_get_created(7),
    )
    controller = _get_fake_controller(repo)
    client = controller.client
    assert list(client.search_content()) == []

    all_rpm_content = client.get_repository("all-rpm-content").result()
    rpm1 = RpmUnit(
        cdn_published=datetime.datetime.utcnow(),
        arch="src",
        filename="test-arc01-1.0-1.src.rpm",
        name="test-arc01",
        version="1.0",
        release="1",
        content_type_id="rpm",
        unit_id="gc_arc_01",
    )
    rpm2 = RpmUnit(
        cdn_published=datetime.datetime.utcnow() - datetime.timedelta(days=190),
        arch="src",
        filename="test-arc02-1.0-1.src.rpm",
        name="test-arc02",
        version="1.0",
        release="1",
        content_type_id="rpm",
        unit_id="gc_arc_02",
    )
    rpm3 = RpmUnit(
        cdn_published=datetime.datetime.utcnow() - datetime.timedelta(days=195),
        arch="src",
        filename="test-arc03-1.0-1.src.rpm",
        name="test-arc03",
        version="1.0",
        release="1",
        content_type_id="rpm",
        unit_id="gc_arc_03",
    )
    controller.insert_units(all_rpm_content, [rpm1, rpm2, rpm3])
    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 3
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            gc.main()

    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 1
    mock_logger.info.assert_any_call("Old all-rpm-content deleted: %s", rpm2.name)


def test_arc_garbage_collect_in_batches(mock_logger, monkeypatch):
    """deletes relevant all-rpm-content content in batches"""
    monkeypatch.setattr(gc_module, "UNASSOCIATE_BATCH_LIMIT", 5)
    repo = Repository(
        id="all-rpm-content",
        created=_get_created(7),
    )
    controller = _get_fake_controller(repo)
    client = controller.client
    assert list(client.search_content()) == []

    all_rpm_content = client.get_repository("all-rpm-content").result()
    new_rpms = [
        RpmUnit(
            cdn_published=datetime.datetime.utcnow(),
            arch="src",
            filename="test-arc-new%02d-1.0-1.src.rpm" % i,
            name="test-arc-new%02d" % i,
            version="1.0",
            release="1",
            content_type_id="rpm",
            unit_id="gc_arc_new%02d" % i,
        )
        for i in range(0, 10)
    ]
    old_rpms = [
        RpmUnit(
            cdn_published=datetime.datetime.utcnow() - datetime.timedelta(days=190),
            arch="src",
            filename="test-arc-old%02d-1.0-1.src.rpm" % i,
            name="test-arc-old%02d" % i,
            version="1.0",
            release="1",
            content_type_id="rpm",
            unit_id="gc_arc_old%02d" % i,
        )
        for i in range(0, 23)
    ]

    controller.insert_units(all_rpm_content, new_rpms + old_rpms)
    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 33
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            gc.main()
    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 10
    assert (
        len(
            [
                call
                for call in mock_logger.debug.call_args_list
                if "Submitting batch for deletion" in call.args
            ]
        )
        == 5
    )


def test_arc_garbage_collect_0items(mock_logger):
    """no content deleted from all-rpm-content"""
    repo = Repository(
        id="all-rpm-content",
        created=_get_created(7),
    )
    controller = _get_fake_controller(repo)
    client = controller.client
    assert list(client.search_content()) == []

    all_rpm_content = client.get_repository("all-rpm-content").result()
    existing_rpm1 = RpmUnit(
        cdn_published=datetime.datetime.utcnow(),
        arch="src",
        filename="test-arc01-1.0-1.src.rpm",
        name="test-arc01",
        version="1.0",
        release="1",
        content_type_id="rpm",
        unit_id="gc_arc_01",
    )
    controller.insert_units(all_rpm_content, [existing_rpm1])
    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 1
    gc = GarbageCollect()
    arg = ["", "--pulp-url", "http://some.url"]

    with patch("sys.argv", arg):
        with _patch_pulp_client(controller.client):
            gc.main()
    updated_rpm = list(client.get_repository("all-rpm-content").search_content())
    assert len(updated_rpm) == 1
    mock_logger.info.assert_any_call("No all-rpm-content found older than %s", 30)
