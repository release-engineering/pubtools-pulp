import pytest
from concurrent.futures import ThreadPoolExecutor
from more_executors import f_return
import time
from pubtools._pulp.tasks.push.phase import Context
from pubtools._pulp.tasks.push.phase.buffer import OutputBuffer
from pubtools._pulp.tasks.push.phase.errors import PhaseInterrupted


class FakeQueue(object):
    def __init__(self):
        self.items = []

    def put(self, item, blocking=True, timeout=None):
        self.items.append(item)


def return_soon(x, delay=0.1):
    time.sleep(delay)
    return x


def test_flushes_at_threshold():
    """OutputBuffer flushes items to queue when item count hits threshold."""
    queue = FakeQueue()
    context = Context()
    buf = OutputBuffer(queue, context, flush_threshold=13)

    # First put 12 items...
    for i in range(0, 12):
        buf.write(i)

    # ...and nothing should be in the queue yet
    assert queue.items == []

    # Then write some more
    buf.write(12)
    buf.write(13)
    buf.write(14)

    # The first write after hitting the threshold should have triggered flush,
    # so there's now a single batch
    assert queue.items == [[i for i in range(0, 13)]]

    # Check also after explicit flush to ensure we didn't miss anything...
    buf.flush()
    assert queue.items == [[i for i in range(0, 13)], [13, 14]]


def test_flushes_at_timeout():
    """OutputBuffer flushes items to queue when sufficient time passes."""
    queue = FakeQueue()
    context = Context()
    buf = OutputBuffer(queue, context, flush_threshold=100, flush_interval=0.2)

    # First put some items...
    for i in range(0, 10):
        buf.write(i)

    # ...and nothing should be in the queue yet
    assert queue.items == []

    # But now sleep a little bit
    time.sleep(0.25)

    # Then write some more
    buf.write(10)
    buf.write(11)
    buf.write(12)

    # The first write after interval passed should have triggered a flush
    assert queue.items == [[i for i in range(0, 11)]]

    # Check also after explicit flush to ensure we didn't miss anything...
    buf.flush()
    assert queue.items == [[i for i in range(0, 11)], [11, 12]]


def test_limits_futures():
    """OutputBuffer never keeps more than configured limit of futures."""
    queue = FakeQueue()
    context = Context()
    buf = OutputBuffer(
        queue, context, flush_threshold=100, flush_interval=60, max_futures=4
    )

    # Peek at internal state of buffer to ensure futures not exceeding limit
    def check_future_count():
        assert len(buf._OutputBuffer__pending_futures) <= 4

    with ThreadPoolExecutor(max_workers=2) as executor:
        for i in range(0, 10):
            # attempt more writes than max_futures; it should block in order
            # to stay under the limit
            buf.write_future(executor.submit(return_soon, i))
            check_future_count()

        buf.flush()
        check_future_count()

    # Check what we got in the queue. Since we used futures, the order and
    # batching is not guaranteed, so collect & sort all.
    all_items = []
    for batch in queue.items:
        # There should never be a useless empty batch.
        assert batch
        all_items.extend(batch)

    # Should have got all the expected numbers.
    assert sorted(all_items) == [i for i in range(0, 10)]


def test_implicit_flush_handles_futures():
    """When an implicit flush occurs, any completed futures are yielded, but futures
    still running are allowed to continue running.
    """
    queue = FakeQueue()
    context = Context()
    buf = OutputBuffer(queue, context, flush_threshold=5)

    with ThreadPoolExecutor(max_workers=1) as executor:

        # Make the buffer almost full...
        buf.write(0)
        buf.write(1)
        buf.write(2)
        buf.write(3)

        # Stick in some futures too, a mix of some already done and
        # some which need more time
        buf.write_future(f_return(10))
        buf.write_future(f_return(11))
        buf.write_future(executor.submit(return_soon, 12))
        buf.write_future(executor.submit(return_soon, 13))

        # Still nothing in queue
        assert queue.items == []

        # But if I write one more item...
        buf.write(4)

        # That should cause an implicit flush, which gives me:
        # - all of the explicitly written items
        # - and the completed futures too
        # - but it didn't wait for the running futures
        assert queue.items == [[0, 1, 2, 3, 4, 10, 11]]

        # The running futures aren't lost though; explicit flush will find them.
        buf.flush()
        assert queue.items == [[0, 1, 2, 3, 4, 10, 11], [12, 13]]


def test_flush_no_queue():
    """OutputBuffer allows calling flush() when there is no queue."""
    context = Context()
    buf = OutputBuffer(None, context)
    buf.flush()


def test_future_batch():
    """write_future_batch passes returned batch through to queue."""
    queue = FakeQueue()
    context = Context()
    buf = OutputBuffer(
        queue, context, flush_threshold=100, flush_interval=60, max_futures=4
    )

    # Try mixing some write_future and write_future_batch.
    # The only difference is that write_future_batch indicates that you will
    # return a list.
    buf.write_future(f_return(0))
    buf.write_future(f_return(1))
    buf.write_future_batch(f_return([2, 3, 4]))
    buf.write_future(f_return(5))

    # Flush them all...
    buf.flush()

    # And the individual items should be combined with the batch as you'd expect
    # (but order is not guaranteed)
    assert len(queue.items) == 1
    assert sorted(queue.items[0]) == [0, 1, 2, 3, 4, 5]


def test_can_interrupt_flush():
    """flush() can become interrupted if context fails."""
    queue = FakeQueue()
    context = Context()

    # Make the context interrupt faster than usual so test doesn't have to wait long
    context.interrupt_interval = 0.1

    buf = OutputBuffer(
        queue, context, flush_threshold=100, flush_interval=60, max_futures=4
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Let's say that we enqueue a future needing 1 second...
        buf.write_future(executor.submit(return_soon, 3.141, delay=1))

        # But meanwhile the context fails sooner than that
        def set_error_soon():
            time.sleep(0.1)
            context.set_error()

        executor.submit(set_error_soon)

        # We can try to flush, but it should tell us we've been interrupted by
        # the context failing.
        with pytest.raises(PhaseInterrupted) as exc:
            buf.flush()

        # It should tell us what was happening at time of interruption.
        assert "Interrupted while waiting for completion of futures" in str(exc.value)
