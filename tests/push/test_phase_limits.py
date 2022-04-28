import time
from threading import Lock, Thread

from concurrent.futures import ThreadPoolExecutor
from more_executors.futures import f_return

from pubtools._pulp.tasks.push.phase import Context, Phase, context, base


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


class CountingPhase(Phase):
    """A phase which puts many items while keeping track of the maximum
    number of future puts."""

    PUT_COUNT = 100

    def __init__(self, *args, **kwargs):
        self.executor = kwargs.pop("executor")
        self.max_future_puts = 0
        super(CountingPhase, self).__init__(*args, **kwargs)

    def put_future_outputs(self, *args, **kwargs):
        super(CountingPhase, self).put_future_outputs(*args, **kwargs)
        self.max_future_puts = max(self.max_future_puts, len(self._Phase__future_puts))

    def run(self):
        for i in range(0, self.PUT_COUNT):
            self.put_future_output(self.executor.submit(return_later, i, 0.001))


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


def test_future_output_bounded(monkeypatch):
    """Verify that put_future_output applies a limit on amount of futures enqueued."""

    # Make the queue size much less than the PUT_COUNT.
    monkeypatch.setattr(context, "QUEUE_SIZE", 10)
    monkeypatch.setattr(base, "QUEUE_SIZE", context.QUEUE_SIZE)

    outputs = []

    # We are going to have to set up a consumer of the phase's output queue to
    # prevent deadlock.
    def read_queue(queue):
        while True:
            out = queue.get()
            outputs.append(out)
            if out in (phase.ERROR, phase.FINISHED):
                break

            # As we are trying to see what happens if the producer is faster
            # than the consumer, we need the reader to be a bit slow.
            time.sleep(0.01)

    ctx = Context()

    with ThreadPoolExecutor() as executor:
        phase = CountingPhase(context=ctx, in_queue=None, executor=executor)

        reader = Thread(target=read_queue, args=(phase.out_queue,))
        reader.start()

        # Try running the phase - it should do PUT_COUNT puts onto its
        # own output queue. It should not deadlock since we have a reader.
        with phase:
            pass

        reader.join(30.0)
        assert not reader.is_alive()

    # It should have completed normally
    assert outputs[-1] is phase.FINISHED

    # While running, the max pending future_puts should never be allowed
    # to exceed this value
    assert phase.max_future_puts <= context.QUEUE_SIZE

    # (and sanity check that it's more than 1 as it otherwise means our
    # test didn't really test what it was supposed to)
    assert phase.max_future_puts > 1
