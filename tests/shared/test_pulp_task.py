import sys
import pytest

from mock import Mock, patch

from pubtools.pulplib import Client
from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService


class TaskWithPulpClient(PulpTask, PulpClientService):
    pass


def test_task_run():
    """raises if run() is not implemeted"""
    task = PulpTask()
    with pytest.raises(NotImplementedError):
        task.run()


def test_init_args():
    """Checks whether the args from cli are available for the task"""
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "--debug"]
    with patch("sys.argv", arg):
        task_args = task.args

    cli_args = ["pulp_url", "pulp_user", "pulp_password", "debug"]
    for a in cli_args:
        assert hasattr(task_args, a)


def test_pulp_client():
    """Checks that the client in the task is an instance of pubtools.pulplib.Client"""
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "--pulp-user", "user"]
    with patch("sys.argv", arg):
        client = task.pulp_client

    assert isinstance(client, Client)


def test_pulp_fake_client():
    """Checks that a fake client is created if --pulp-fake is given"""
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "https://pulp.example.com/", "--pulp-fake"]
    with patch("sys.argv", arg):
        client = task.pulp_client

    # Fake client doesn't advertise itself in any obvious way.
    # Just do some rough checks...
    assert "Fake" in type(client).__name__

    # Should be able to use the API even though it's obviously not connected
    # to a real Pulp server
    assert "rpm" in client.get_content_type_ids().result()
    assert list(client.search_repository().result()) == []


def test_main():
    """Checks main returns without exception when invoked with minimal args
    assuming run() and add_args() are implemented
    """
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "-d"]
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            assert task.main() == 0


def test_description():
    """description is initialized from subclass docstring, de-dented."""

    class MyTask(PulpTask):
        """This is an example task subclass.

        It has a realistic multi-line doc string:

            ...and may have several levels of indent.
        """

    assert MyTask().description == (
        "This is an example task subclass.\n\n"
        "It has a realistic multi-line doc string:\n\n"
        "    ...and may have several levels of indent."
    )


@pytest.mark.parametrize(
    "throttle", [None, 8], ids=("throttle_from_env", "throttle_option")
)
def test_pulp_throttle(monkeypatch, throttle):
    """Checks main returns without exception when invoked with --pulp-throttle arg
    or PULP_THROTTLE value from environment variable, and checks whether the arg is
    correctly promoted to pulp_client.
    """
    pulp_throttle = 7
    monkeypatch.setenv("PULP_THROTTLE", pulp_throttle)
    task = TaskWithPulpClient()
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "-d",
    ]
    if throttle:
        arg.extend(
            [
                "--pulp-throttle",
                str(throttle),
            ]
        )
        pulp_throttle = throttle

    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            assert task.main() == 0
            assert task.args.pulp_throttle == throttle
            assert (
                task.pulp_client._task_executor._delegate._throttle() == pulp_throttle
            )


@pytest.mark.parametrize(
    "throttle, exception",
    [(None, ValueError), ("xyz", SystemExit)],
    ids=("from_env", "from_option"),
)
def test_pulp_throttle_invalid(monkeypatch, throttle, exception):
    """Checks main raises SystemExit when a non-int string is passed with --pulp-throttle
    or ValueError when PULP_THROTTLE env variable is non-it.
    """
    monkeypatch.setenv("PULP_THROTTLE", "abc")
    task = TaskWithPulpClient()
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "-d",
    ]
    if throttle:
        arg.extend(["--pulp-throttle", "xyz"])
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            with pytest.raises(exception):
                task.main()
                assert task.pulp_client is None


def test_pulp_throttle_negative():
    """Checks main raises SystemExit when a negative int is passed with --pulp-throttle."""
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "-d", "--pulp-throttle", "-1"]
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            with pytest.raises(SystemExit):
                task.main()
