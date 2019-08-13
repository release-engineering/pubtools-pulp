import sys
import pytest

from mock import Mock, patch

from pubtools.pulplib import Client
from pubtools.pulp.task import PulpTask


@pytest.fixture
def p_add_args():
    with patch("pubtools.pulp.task.PulpTask.add_args") as p_args:
        yield p_args


def test_task_run():
    """ raises if run() is not implemeted"""
    task = PulpTask()
    with pytest.raises(NotImplementedError):
        task.run()


def test_init_args(p_add_args):
    """Checks whether the args from cli are available for the task"""
    task = PulpTask()
    arg = ["", "--url", "http://some.url", "--verbose", "--debug"]
    with patch("sys.argv", arg):
        task_args = task.args

    cli_args = ["url", "user", "password", "verbose", "debug"]
    for a in cli_args:
        assert hasattr(task_args, a)


def test_pulp_client(p_add_args):
    """Checks that the client in the task is an instance of pubtools.pulplib.Client"""
    task = PulpTask()
    arg = ["", "--url", "http://some.url", "--user", "user"]
    with patch("sys.argv", arg):
        client = task.pulp_client

    assert isinstance(client, Client)


def test_main(p_add_args):
    """Checks main returns without exception when invoked with minimal args
        assuming run() and add_args() are implemented
    """
    task = PulpTask()
    arg = ["", "--url", "http://some.url", "--verbose", "--debug"]
    with patch("sys.argv", arg):
        with patch("pubtools.pulp.task.PulpTask.run"):
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
