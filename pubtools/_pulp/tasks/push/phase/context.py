# -*- coding: utf-8 -*-

import logging
from collections import namedtuple

from threading import Lock, Event

from six.moves.queue import Queue

from .base import Phase, QUEUE_SIZE

# u here is not redundant since we still support py2...
# pylint: disable=redundant-u-string-prefix

LOG = logging.getLogger("pubtools.pulp")

QueueCounts = namedtuple("QueueCounts", ["put", "get", "done"])


class CountingQueue(object):
    """A Queue wrapper adding some counting & progress reporting features."""

    def __init__(self, **kwargs):
        # Name is expected to be set after construction, as phases are wired together.
        self.name = "<unknown queue>"
        self._get_count = 0
        self._put_count = 0
        self._done_count = 0
        self._lock = Lock()
        self._delegate = Queue(**kwargs)
        self.qsize = self._delegate.qsize

    def _incr_get(self):
        with self._lock:
            self._get_count += 1

    def _incr_put(self):
        with self._lock:
            self._put_count += 1

    def _incr_done(self):
        with self._lock:
            self._done_count += 1

    @property
    def counts(self):
        """Returns a QueueCounts tuple for the current state of this queue.

        Can be used to estimate the progress of a phase reading from this
        queue.
        """
        with self._lock:
            return QueueCounts(self._put_count, self._get_count, self._done_count)

    # The following methods are API-compatible with the standard Queue
    # methods, but simultaneously update our counts.

    def put(self, item, *args, **kwargs):
        self._delegate.put(item, *args, **kwargs)
        if item not in (Phase.ERROR, Phase.FINISHED):
            self._incr_put()

    def get(self, *args, **kwargs):
        out = self._delegate.get(*args, **kwargs)
        if out not in (Phase.ERROR, Phase.FINISHED):
            self._incr_get()
        return out

    def task_done(self):
        self._delegate.task_done()
        self._incr_done()


class Context(object):
    """A context object shared across all phases.

    The main purpose of the context object is to group all phases
    under a single execution context which can be stopped on demand
    (i.e. if an error occurs). It may also be used to share a small
    amount of out-of-band mutable state between phases.
    """

    def __init__(self):
        self._queues = []
        self._error = False

        """An event which becomes True during push only after all push
        items have been encountered (so that the total number of push items
        is known).
        """
        self.items_known = Event()

        """Total number of push items.

        This value is only valid after self.items_known has been set to True.
        Prior to that, it is possible that more items are still being loaded.
        """
        self.items_count = None

    @property
    def has_error(self):
        """True if and only if the context is in the error state.

        If `has_error` is true, phases should stop processing ASAP.
        """
        return self._error

    def set_error(self):
        """Set the context into the error state, indicating that a fatal
        error has occurred.
        """
        self._error = True
        for queue in self._queues:
            queue.put(Phase.ERROR)

    def new_queue(self, counting=True, **kwargs):
        """Create and return a new Queue.

        The Queue is associated with this context such that, if the context
        enters the error state, the queue will receive an ERROR object.

        If counting is True, get/put counts will be recorded for the queue
        and the queue will participate in progress logging. This should be
        disabled for unusual cases.
        """
        if "maxsize" not in kwargs:
            kwargs["maxsize"] = QUEUE_SIZE

        if counting:
            out = CountingQueue(**kwargs)
        else:
            out = Queue(**kwargs)
        self._queues.append(out)
        return out

    @property
    def queue_counts(self):
        """Get current item counts in every counting queue.

        Returns a list of tuple("queue name", tuple(put, get, done)).
        """
        out = []

        for queue in self._queues:
            if not isinstance(queue, CountingQueue):
                # A queue which does not make sense for counting/progress,
                # such as the one used by Collect phase.
                continue

            counts = queue.counts
            out.append((queue.name, counts))

        return out
