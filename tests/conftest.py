import sys
import os

import requests_mock
import pytest

from pushcollector import Collector

from .command import CommandTester
from .collector import FakeCollector


@pytest.fixture(autouse=True)
def save_argv():
    """Saves and restores sys.argv around each test.

    This is an autouse fixture, so tests can freely modify
    sys.argv without concern.
    """
    orig_argv = sys.argv[:]
    yield
    sys.argv[:] = orig_argv


@pytest.fixture(autouse=True)
def home_tmpdir(tmpdir, monkeypatch):
    """Points HOME environment variable underneath tmpdir
    for the duration of tests.

    This is an autouse fixture because certain used libraries
    and our own pulp fake are influenced by files under $HOME,
    and for tests which actually need it, we should explicitly
    set up anything needed there instead of inheriting the
    user's environment.
    """
    homedir = str(tmpdir.mkdir("home"))
    monkeypatch.setenv("HOME", homedir)


@pytest.fixture(autouse=True)
def requests_mocker():
    """Mock all requests.

    This is an autouse fixture so that tests can't accidentally
    perform real requests without being noticed.
    """
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture(autouse=True)
def fake_collector():
    """Install fake in-memory backend for pushcollector library.
    Recorded push items can be tested via this instance.

    This is an autouse fixture so that all tests will automatically
    use the fake backend.
    """
    collector = FakeCollector()

    Collector.register_backend("pubtools-pulp-test", lambda: collector)
    Collector.set_default_backend("pubtools-pulp-test")

    yield collector

    Collector.set_default_backend(None)


@pytest.fixture
def data_path():
    """Returns path to the tests/data dir used to store extra files for testing."""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def command_tester(request, caplog):
    """Yields a configured instance of CommandTester class for
    running commands and testing output against expected.
    """
    yield CommandTester(request.node, caplog)
