import logging

from pushsource import ModuleMdPushItem
from threading import Event

try:
    from time import monotonic
except ImportError:  # pragma: no cover
    from monotonic import monotonic
import concurrent.futures
from collections import defaultdict
from queue import Queue, Full, Empty


from .errors import PhaseInterrupted

LOG = logging.getLogger("pubtools.pulp")


class ContextQueue(object):
    """A Queue wrapper integrating with some Context features:

    - makes get() and put() interruptible if context has failed
    - can have callbacks installed to monitor put & get
      (e.g. for progress tracking)
    """

    def __init__(self, context, **kwargs):
        self._delegate = Queue(**kwargs)
        self.qsize = self._delegate.qsize

        self.before_put = []
        """Callbacks invoked prior to any put()."""

        self.after_put = []
        """Callbacks invoked after a successful put()."""

        self.after_get = []
        """Callbacks invoked after a successful get()."""

        self.__interruptible_put = context.interruptible(
            self._delegate.put, msg="writing to queue"
        )
        self.__interruptible_get = context.interruptible(
            self._delegate.get, msg="reading from queue"
        )

    def put(self, item, block=True, timeout=None):
        for cb in self.before_put:
            cb(item)
        self.__interruptible_put(item, block=block, timeout=timeout)
        for cb in self.after_put:
            cb(item)

    def get(self, block=True, timeout=None):
        out = self.__interruptible_get(block=block, timeout=timeout)
        for cb in self.after_get:
            cb(out)
        return out


class ItemInfo(object):
    """Holds aggregate info on all push items involved in the context.

    This object is used to make out-of-band info available across phases
    beyond just the items passed via queues.
    """

    def __init__(self):
        self.items_known = Event()
        """An event which becomes True during push only after all push
        items have been encountered. Other attributes on this object may
        represent incomplete information until this event becomes True.
        """

        self.items_count = 0
        """How many items are in the push, in total."""

        self.modulemd_count_per_dest = defaultdict(int)
        """How many modulemd items exist per destination in the push."""

    def add_item(self, item):
        """Record an item on this object.

        This should be done only once per item (i.e. do not re-add items
        as state is updated).
        """
        self.items_count += 1

        if isinstance(item.pushsource_item, ModuleMdPushItem):
            for dest in item.pushsource_item.dest:
                self.modulemd_count_per_dest[dest] += 1


class Context(object):
    """A context object shared across all phases.

    The main purpose of the context object is to group all phases
    under a single execution context which can be stopped on demand
    (i.e. if an error occurs). It may also be used to share a small
    amount of out-of-band mutable state between phases.
    """

    def __init__(self):
        self._error = Event()

        self.error_phase = None
        """Name of phase which encountered a fatal error, if one has occurred."""

        self.error_exception = None
        """The exception causing a fatal error, if one has occurred."""

        self.item_info = ItemInfo()
        """Records aggregate info on items in the push."""

        self.interrupt_interval = 5.0
        """Default value of 'interval' for interruptible method.

        This exists only so that it can be overridden from tests.
        """

        self.progress_infos = []
        """All ProgressInfo objects participating in progress tracking for this
        context.

        These should be arranged in display order.
        """

    @property
    def has_error(self):
        """True if and only if the context is in the error state.

        If `has_error` is true, phases should stop processing ASAP.
        """
        return self._error.is_set()

    def set_error(
        self, phase="<unknown phase>", exception=RuntimeError("unknown error")
    ):
        """Set the context into the error state, indicating that a fatal
        error has occurred.
        """
        if not self.has_error:
            self.error_phase = phase
            self.error_exception = exception
            self._error.set()

    def raise_if_interrupted(self, msg):
        """A convenience method to raise an exception if the context has an error.

        'msg' should mention the operation currently being attempted.
        """
        if self.has_error:
            raise PhaseInterrupted("Interrupted while %s" % msg)

    def new_queue(self, **kwargs):
        """Create and return a new Queue.

        The Queue is associated with this context such that, if the context
        enters the error state, any queue operations will be interrupted.
        """
        return ContextQueue(context=self, **kwargs)

    def interruptible(
        self,
        fn,
        msg="performing a blocking operation",
        interval=None,
        timeout_exceptions=(Full, Empty, concurrent.futures.TimeoutError),
    ):
        """Wraps a blocking timeout-accepting function to be interruptible.

        Given a blocking function with a timeout (e.g. queue.put, future.result),
        this method will return a new function with the added behavior that the
        operation can be interrupted if this context enters the error state.

        Under the hood, this works by calling the given function in a loop with
        a smaller timeout than that provided by the caller. This is not ideal
        for efficiency or latency, but is a necessary workaround for the lack of
        an API for a python thread to wait on multiple locks at once.

        Arguments:

            fn (callable)
                Any callable which accepts a 'timeout' argument.

            msg (str)
                Optional message to be included in any raised exceptions on interrupt.
                Use this to provide a bit of context on what the callable is doing.

            interval (float)
                How much time between checks for interruption.

            timeout_exceptions (tuple)
                Exception class(es) to be interpreted as a timeout from fn().
        """
        interval = interval or self.interrupt_interval

        def out(*args, **kwargs):
            self.raise_if_interrupted(msg)

            timeout = kwargs.get("timeout")
            inner_interval = interval
            if timeout is not None:
                inner_interval = min(interval, timeout)

            start_oper = monotonic()

            kwargs["timeout"] = inner_interval

            while True:
                try:
                    # Try the operation for a few seconds.
                    return fn(*args, **kwargs)
                except timeout_exceptions:
                    # interrupted => give up
                    self.raise_if_interrupted(msg)

                    # past the deadline => give up
                    if timeout is not None and monotonic() - start_oper > timeout:
                        # re-raise whatever exception we already got.
                        raise

                    # In any other case we spin and try again soon.

        return out
