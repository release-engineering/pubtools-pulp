import sys
import pytest

from mock import Mock, patch

from pubtools.pulplib import Client
from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService


class TaskWithPulpClient(PulpTask, PulpClientService):
    pass


def test_task_run():
    """ raises if run() is not implemeted"""
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


def test_pulp_throttle():
    """Checks main returns without exception when invoked with minimal args
    assuming run() and add_args() are implemented
    """
    pulp_throttle = 7
    task = TaskWithPulpClient()
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "-d",
        "--pulp-throttle",
        str(pulp_throttle),
    ]
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            assert task.main() == 0
            assert task.args.pulp_throttle == pulp_throttle
            assert (
                task.pulp_client._task_executor._delegate._throttle() == pulp_throttle
            )


def test_pulp_throttle_invalid():
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "-d", "--pulp-throttle", "xyz"]
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            with pytest.raises(SystemExit):
                task.main()


def test_pulp_throttle_negative():
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "-d", "--pulp-throttle", "-1"]
    with patch("sys.argv", arg):
        with patch("pubtools._pulp.task.PulpTask.run"):
            with pytest.raises(SystemExit):
                task.main()
