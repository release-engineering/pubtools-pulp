import time

from concurrent.futures import ThreadPoolExecutor
from more_executors.futures import f_return

from pubtools._pulp.tasks.push.phase import Context, Phase, context


def return_later(value, delay=0.2):
    """Sleep a little while, then return a value. Simulates
    a slow operation."""
    time.sleep(delay)
    return value


class ManyFutureOutputsPhase(Phase):
    """A phase which will set up a large number of async outputs."""

    # How many items we'll try pushing.
    #
    # Note our docs say we should be able to handle up to 100,000,
    # but that makes the test a bit too slow. In fact 1,000 is enough
    # to hit the recursion limit which originally motivated this test.
    PUT_COUNT = 10000

    def __init__(self, *args, **kwargs):
        self.executor = kwargs.pop("executor")
        super(ManyFutureOutputsPhase, self).__init__(*args, **kwargs)

    def run(self):
        # Firstly put an item which will take a little while to resolve.
        self.put_future_output(self.executor.submit(return_later, 0))

        # Now just put a ton of other outputs.
        # The point here is that, because the first future needs some time
        # to resolve, we are potentially going to build up a chain of
        # thousands of futures.
        for i in range(1, self.PUT_COUNT):
            self.put_future_output(f_return(i))


def test_future_output_limits(monkeypatch):
    """Verify that put_future_output scales to thousands of futures OK."""

    # Because we haven't set up anything to read from the phase's output queue
    # while it's in progress, we need to ensure that QUEUE_SIZE is larger than
    # the PUT_COUNT or we'll deadlock.
    monkeypatch.setattr(context, "QUEUE_SIZE", ManyFutureOutputsPhase.PUT_COUNT * 2)

    ctx = Context()

    with ThreadPoolExecutor() as executor:
        phase = ManyFutureOutputsPhase(context=ctx, in_queue=None, executor=executor)

        # Try running the phase - it should do PUT_COUNT puts onto its
        # own output queue.
        with phase:
            pass

    # Now let's see what arrived in the queue.
    outputs = []
    while True:
        out = phase.out_queue.get()

        # Should not have encountered an error
        assert out is not phase.ERROR

        if out is phase.FINISHED:
            break

        outputs.append(out)

    # Order is not guaranteed, but every output should successfully make
    # it through.
    assert sorted(outputs) == sorted(range(0, phase.PUT_COUNT))
