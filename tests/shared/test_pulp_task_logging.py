import logging
import pytest
import sys

from pubtools._pulp.task import PulpTask


class MyTask(PulpTask):
    def run(self):
        # nothing to do
        pass


def simple_basic_config(level, **_kwargs):
    """Like logging.basicConfig except:

    - it only sets the level, ignores other arguments
    - it works every time (instead of only once per process)
    """
    logging.getLogger().setLevel(level)


@pytest.fixture(autouse=True)
def clean_root_logger(monkeypatch):
    """Hijack logging.basicConfig and reset root logger level around tests."""

    monkeypatch.setattr(logging, "basicConfig", simple_basic_config)

    root = logging.getLogger()
    level = root.level
    yield
    root.setLevel(level)


# All loggers below are forced to NOTSET before being yielded, because
# other tests might have already adjusted their level


@pytest.fixture
def tier1_logger():
    """The logger for this project."""

    out = logging.getLogger("pubtools.pulp")
    level = out.level
    out.setLevel(logging.NOTSET)
    yield out
    out.setLevel(level)


@pytest.fixture
def tier2_logger():
    """A logger from the same family of projects."""

    out = logging.getLogger("pubtools.some-pubtools-project")
    level = out.level
    out.setLevel(logging.NOTSET)
    yield out
    out.setLevel(level)


@pytest.fixture
def tier3_logger():
    """A completely foreign logger from an unrelated project."""

    out = logging.getLogger("some-foreign-logger")
    level = out.level
    out.setLevel(logging.NOTSET)
    yield out
    out.setLevel(level)


def test_default_logs(tier1_logger, tier2_logger, tier3_logger):
    """All loggers use INFO by default."""

    task = MyTask()
    sys.argv = ["my-task"]
    task.main()

    assert tier1_logger.getEffectiveLevel() == logging.INFO
    assert tier2_logger.getEffectiveLevel() == logging.INFO
    assert tier3_logger.getEffectiveLevel() == logging.INFO


def test_debug1_logs(tier1_logger, tier2_logger, tier3_logger):
    """Tier 1 loggers use DEBUG if --debug is provided"""

    task = MyTask()
    sys.argv = ["my-task", "--debug"]
    task.main()

    assert tier1_logger.getEffectiveLevel() == logging.DEBUG
    assert tier2_logger.getEffectiveLevel() == logging.INFO
    assert tier3_logger.getEffectiveLevel() == logging.INFO


def test_debug2_logs(tier1_logger, tier2_logger, tier3_logger):
    """Tier 1 & 2 loggers use DEBUG if --debug is provided twice."""

    task = MyTask()
    sys.argv = ["my-task", "-dd"]
    task.main()

    assert tier1_logger.getEffectiveLevel() == logging.DEBUG
    assert tier2_logger.getEffectiveLevel() == logging.DEBUG
    assert tier3_logger.getEffectiveLevel() == logging.INFO


def test_debug3_logs(tier1_logger, tier2_logger, tier3_logger):
    """All loggers use DEBUG if --debug is provided thrice."""

    task = MyTask()
    sys.argv = ["my-task", "--debug", "-d", "--debug"]
    task.main()

    assert tier1_logger.getEffectiveLevel() == logging.DEBUG
    assert tier2_logger.getEffectiveLevel() == logging.DEBUG
    assert tier3_logger.getEffectiveLevel() == logging.DEBUG
