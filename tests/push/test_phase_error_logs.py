import logging

from more_executors.futures import f_return_error

from pubtools._pulp.tasks.push.phase import Context, Phase


class EmptyPhase(Phase):
    # A Phase implementation which doesn't do anything.

    def run(self):
        pass


class ImmediateErrorPhase(Phase):
    # A Phase implementation which always raises some error directly
    # from within run().

    def run(self):
        raise RuntimeError("simulated immediate error")


class AsyncErrorPhase(Phase):
    # A Phase implementation which tries put_future_output with a
    # future always raising an error.

    def run(self):
        exc = RuntimeError("simulated async error")
        self.put_future_output(f_return_error(exc))


def test_immediate_raise(caplog):
    """Immediate raise in phase's run() will be logged as a fatal error."""

    caplog.set_level(logging.INFO)

    ctx = Context()

    phase = ImmediateErrorPhase(ctx, name="test-phase")

    # Let it run
    with phase:
        pass

    # It should have flagged an error
    assert ctx.has_error

    # It should have logged the exception
    assert "test-phase: fatal error occurred" in caplog.text
    assert "simulated immediate error" in caplog.text


def test_async_raise(caplog):
    """Async raise via put_future_output will be logged as a fatal error."""

    caplog.set_level(logging.INFO)

    ctx = Context()

    phase = AsyncErrorPhase(ctx, name="test-phase")

    # Let it run
    with phase:
        pass

    # It should have flagged an error
    assert ctx.has_error

    # It should have logged the exception
    assert "test-phase: fatal error occurred" in caplog.text
    assert "simulated async error" in caplog.text


def test_raise_in_with_block(caplog):
    """Immediate raise within phase's with block will set context failed."""

    caplog.set_level(logging.INFO)

    ctx = Context()

    phase = EmptyPhase(ctx, name="test-phase")
    error = RuntimeError("error from within")

    # Let it run
    try:
        with phase:
            # Now raise from within.
            #
            # It might seem like this can't happen in practice because the with block
            # in the push command is empty. Actually, it can happen e.g. due to
            # KeyboardInterrupt or other signals.
            raise error
    except RuntimeError:
        pass

    # It should have flagged an error on the context
    assert ctx.has_error
    assert ctx.error_phase == "test-phase"
    assert ctx.error_exception is error
