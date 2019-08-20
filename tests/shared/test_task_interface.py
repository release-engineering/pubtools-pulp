"""Tests ensuring all task modules have a consistent interface."""
import argparse
import sys
import pytest


@pytest.fixture(params=["pubtools._pulp.tasks.garbage_collect"])
def task_module(request):
    __import__(request.param)
    return sys.modules[request.param]


def test_doc_parser(task_module):
    """Every task module should have a doc_parser which can be called
    with no arguments and returns an ArgumentParser.

    This supports the generation of docs from argument parsers."""
    fn = getattr(task_module, "doc_parser")
    parser = fn()
    assert isinstance(parser, argparse.ArgumentParser)


def test_entry_point(task_module):
    """Every task module should have a callable entry_point."""
    fn = getattr(task_module, "entry_point")
    assert callable(fn)
