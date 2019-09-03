from concurrent.futures import Future
import logging
import sys

import pytest

from pubtools._pulp.task import PulpTask

step = PulpTask.step


class SimulatedError(RuntimeError):
    pass


class FakeTask(object):
    @property
    def args(self):
        # Not a real task, no args from user
        return object()

    @step("fail if neq")
    def fail_if_neq(self, x, y):
        if x != y:
            raise SimulatedError()

    @step("future in-out")
    def future_in_out(self, f, fail=False):
        if fail:
            raise SimulatedError()
        return Future()

    @step("exit with code")
    def exit_with_code(self, code):
        sys.exit(code)

    @step("future in-out list")
    def future_in_out_list(self, f):
        return [Future(), Future()]


def test_success(caplog):
    """Plain blocking step should log when entered/exited"""

    caplog.set_level(logging.INFO)
    task = FakeTask()
    task.fail_if_neq(1, 1)

    assert caplog.messages == ["fail if neq: started", "fail if neq: finished"]


def test_fail(caplog):
    """Plain blocking step should log when entered/failed"""

    caplog.set_level(logging.INFO)
    task = FakeTask()

    with pytest.raises(SimulatedError):
        task.fail_if_neq(1, 2)

    assert caplog.messages == ["fail if neq: started", "fail if neq: failed"]


def test_future_logging(caplog):
    """Step taking/returning future should log when futures progress"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    in_fs = [Future(), Future()]
    out_f = task.future_in_out(in_fs)

    # The step shouldn't be counted as entered yet.
    assert caplog.messages == []

    # If *any* input future is resolved, then the step counts as started,
    # but not yet finished.
    in_fs[0].set_result(None)
    assert caplog.messages == ["future in-out: started"]

    # There should not be duplicate logs when other input futures resolve.
    in_fs[1].set_result(None)
    assert caplog.messages == ["future in-out: started"]

    # If the output future is resolved, then the step counts as finished.
    out_f.set_result(None)
    assert caplog.messages == ["future in-out: started", "future in-out: finished"]


def test_future_output_failed(caplog):
    """Step returning future should log when output fails"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    in_f = Future()
    in_f.set_result("abc")

    out_f = task.future_in_out(in_f)

    # It's now in progress.
    assert caplog.messages == ["future in-out: started"]

    # If the output future is failed, then step is considered failed
    out_f.set_exception(SimulatedError())
    assert caplog.messages == ["future in-out: started", "future in-out: failed"]


def test_future_list_failed(caplog):
    """Step returning list of futures should log when any fails"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    in_f = Future()
    out_fs = task.future_in_out_list(in_f)

    # No logs yet.
    assert caplog.messages == []

    # If the output future is failed, then step is immediately considered failed
    out_fs[0].set_exception(SimulatedError())
    assert caplog.messages == [
        "future in-out list: started",
        "future in-out list: failed",
    ]

    # Nothing changes if another future completes.
    out_fs[1].set_result(None)
    assert caplog.messages == [
        "future in-out list: started",
        "future in-out list: failed",
    ]


def test_future_fails_not_started(caplog):
    """Step which immediately fails given incomplete futures should have coherent logs"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    in_f = Future()
    with pytest.raises(SimulatedError):
        task.future_in_out(in_f, fail=True)

    # Although it takes a future which is not resolved yet,
    # it's immediately marked as both started & failed due
    # to the exception being raised
    assert caplog.messages == ["future in-out: started", "future in-out: failed"]

    # Input future being resolved doesn't change the logs at all.
    in_f.set_result(None)
    assert caplog.messages == ["future in-out: started", "future in-out: failed"]


def test_exit_success(caplog):
    """Step exiting successfully is considered finished"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    with pytest.raises(SystemExit):
        task.exit_with_code(0)

    assert caplog.messages == ["exit with code: started", "exit with code: finished"]


def test_exit_fail(caplog):
    """Step exiting unsuccessfully is considered failed"""

    caplog.set_level(logging.INFO)

    task = FakeTask()

    with pytest.raises(SystemExit):
        task.exit_with_code(123)

    assert caplog.messages == ["exit with code: started", "exit with code: failed"]
