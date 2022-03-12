import pytest

from pushsource import Source

from .fake_source import FakeSource


@pytest.fixture()
def fake_source():
    Source.register_backend("fake", FakeSource)
