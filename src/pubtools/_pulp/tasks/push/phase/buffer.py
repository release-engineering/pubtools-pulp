import threading
import concurrent.futures
from collections import namedtuple

from more_executors import f_map

try:
    from time import monotonic
except ImportError:  # pragma: no cover
    from monotonic import monotonic


# Internal class to keep track of whether a value represents a single item
# or a batch of items.
Batch = namedtuple("Batch", ["items"])


def wait_any(fs, timeout=None, return_when=concurrent.futures.ALL_COMPLETED):
    # concurrent.futures.wait wrapper with slightly different behavior:
    # raises, rather than returns, if no futures completed within timeout.
    #
    # The point is that this works better with context.interruptible, as it
    # makes the behavior consistent with queue.put, future.result etc.
    done, not_done = concurrent.futures.wait(
        fs, timeout=timeout, return_when=return_when
    )
    if not done:
        raise concurrent.futures.TimeoutError()
    return (done, not_done)


class OutputBuffer(object):
    """A buffer holding output from a single phase.

    An output buffer can hold a batch of items not yet sent to the phase's output
    queue, as well as some item-producing futures. Items are sent when the
    buffer is flushed, either explicitly or implicitly according to size & time
    thresholds.
    """

    def __init__(
        self,
        queue,
        context,
        thread=None,
        flush_threshold=100,
        flush_interval=5.0,
        max_futures=10,
    ):
        """Constructs a new buffer.

        Arguments:
            queue (Queue)
                Output queue for the buffer. Flush will write lists of items to
                this queue.

            context (Context)
                Context associated with the buffer.

                The context is used to ensure that any blocking operation can be
                interrupted if the context encounters an error.

            thread (Thread)
                The thread expected to place items on the buffer.

                This argument is used only to help ensure correct usage of
                the buffer. It's expected that all writes to the buffer should
                happen from within each phase's main thread (and not, for example,
                by any future-attached callbacks which might run in other threads).
                By passing the expected thread in this argument, this will be
                checked at runtime to protect against programmer error.

            flush_threshold (int)
                If the number of items on the buffer exceeds this value, a
                write will trigger an implicit flush.

            flush_interval (float)
                If this number of seconds have passed since the last flush,
                a write will trigger an implicit flush.

            max_futures (int)
                Maximum number of futures allowed to be outstanding on the
                buffer.

                Assuming that each future represents some fairly expensive
                operation, and noting that a single future is allowed to
                produce a batch of multiple items, this value should be kept
                quite small.
        """
        self.queue = queue
        self.thread = thread or threading.current_thread()
        self.flush_threshold = flush_threshold
        self.flush_interval = flush_interval
        self.max_futures = max_futures
        self.context = context

        self.__futures_wait = context.interruptible(
            fn=wait_any, msg="waiting for completion of futures"
        )

        self.__pending_items = []
        self.__pending_futures = []
        self.__last_flush = None

    def __check_can_write(self):
        # Performs various checks prior to each write.

        # If context is already in error state, we should stop immediately.
        self.context.raise_if_interrupted("writing to output buffer")

        # Phases without a queue are valid, but it means put() must never be called.
        assert self.queue, "BUG: write attempted by phase with no output queue"

        # We should only be interacted with by the phase's main thread.
        assert (
            threading.current_thread() is self.thread
        ), "BUG: writes happening from wrong thread %s (expected: %s)" % (
            threading.current_thread(),
            self.thread,
        )

    def write(self, item):
        """Add a single item into the buffer.

        May flush or block if buffer is full.
        """
        self.__check_can_write()
        self.__pending_items.append(item)
        self.__maybe_flush()

    def write_future(self, item_f):
        """Add a future returning a single item into the buffer.

        May flush or block if buffer is full.
        """
        self.__check_can_write()
        self.__ensure_futures_lt(self.max_futures)
        self.__pending_futures.append(item_f)
        self.__maybe_flush()

    def write_future_batch(self, item_f):
        """Add a future returning a list of items into the buffer.

        May flush or block if buffer is full.
        """
        self.__check_can_write()
        self.__ensure_futures_lt(self.max_futures)
        self.__pending_futures.append(f_map(item_f, Batch))
        self.__maybe_flush()

    def cancel(self):
        """Cancel any outstanding work on this buffer:

        - any pending unwritten items are discarded
        - any pending Futures are cancelled (if possible), then discarded
        """
        for f in self.__pending_futures:
            f.cancel()
        self.__pending_items = []
        self.__pending_futures = []

    def flush(self, await_futures=True):
        """Flush outstanding work on this buffer:

        - Any pending items are written to the output queue, as a single batch

        - If await_futures is True (which it generally should be if a
          phase is completing), then all pending futures are also awaited.
          Otherwise, completed futures are flushed but running futures are not
          awaited.
        """
        if not self.queue:
            return

        self.__check_can_write()

        if await_futures:
            self.__ensure_futures_lt(1)
        else:
            self.__handle_any_done_futures()

        self.queue.put(self.__pending_items[:])

        self.__pending_items = []
        self.__last_flush = monotonic()

    def __ensure_futures_lt(self, value):
        # Ensure the number of pending futures is less than 'value', blocking if
        # necessary until this is true. Used both when adding new futures
        # and when flushing all futures.
        while len(self.__pending_futures) >= value:
            done, not_done = self.__futures_wait(
                self.__pending_futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in done:
                self.__handle_done_future(f)
            self.__pending_futures[:] = not_done

    def __maybe_flush(self):
        # Called on each write. Flush, or do not, based on the current state
        # of the buffer.

        # First call to maybe_flush starts the clock if it's not already running.
        if self.__last_flush is None:
            self.__last_flush = monotonic()

        if (
            len(self.__pending_items) >= self.flush_threshold
            or (monotonic() - self.__last_flush) > self.flush_interval
        ):
            self.flush(await_futures=False)

    def __handle_done_future(self, f):
        # Handle a single completed future.

        # If the future has failed, the error is allowed to propagate from
        # here all the way up to the phase's run() method.
        result = f.result()

        if isinstance(result, Batch):
            self.__pending_items.extend(result.items)
        else:
            self.__pending_items.append(result)

    def __handle_any_done_futures(self):
        # Handle any futures if they are already done, but don't wait for
        # running futures to complete.
        new_pending_futures = []

        for f in self.__pending_futures:
            if f.done():
                self.__handle_done_future(f)
            else:
                new_pending_futures.append(f)

        self.__pending_futures[:] = new_pending_futures
