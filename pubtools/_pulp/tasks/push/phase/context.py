# -*- coding: utf-8 -*-

import os
import logging
import shutil
from collections import namedtuple

from contextlib import contextmanager

from threading import Lock, Thread, Event

from six.moves.queue import Queue

from .base import Phase

# u here is not redundant since we still support py2...
# pylint: disable=redundant-u-string-prefix

LOG = logging.getLogger("pubtools.pulp")

# How long, in seconds, between our logging of current phase progress.
PROGRESS_INTERVAL = int(os.getenv("PUBTOOLS_PULP_PROGRESS_INTERVAL") or "600")


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
        if counting:
            out = CountingQueue(**kwargs)
        else:
            out = Queue(**kwargs)
        self._queues.append(out)
        return out

    def dump_progress(self, width=None):
        """Output a log with progress info for each queue associated with the
        context.

        The log has a visual component (progress bars) and also a structured
        event logged via 'extra'.
        """

        if width is None:
            width = int(os.environ.get("COLUMNS") or "80")

            # Conditional due to py2
            if hasattr(shutil, "get_terminal_size"):
                (width, _) = shutil.get_terminal_size()

        snapshot = []
        max_namelen = 0
        max_count = 1

        for queue in self._queues:
            if not isinstance(queue, CountingQueue):
                # A queue which does not make sense for counting/progress,
                # such as the one used by Collect phase.
                continue

            max_namelen = max(max_namelen, len(queue.name))
            counts = queue.counts
            snapshot.append((queue.name, counts))
            max_count = max(max_count, *counts)

        template_str = "[ %%%ds | %%s%%s%%s%%s ]" % max_namelen
        bar_width = width - max_namelen - 10

        # We will create both human-oriented strings and machine-oriented
        # structured metrics (logged via 'extra'). In practice this can be
        # gathered into JSONL logs.
        formatted_strs = []
        event = {"type": "progress-report", "phases": []}

        for (name, counts) in snapshot:
            # We want to draw a progress bar like this:
            #
            # "▇▇▇▇▄▄▄▄▄▁▁▁▁              "
            #
            # The bar fills in from left to right.
            #
            # '▇' => done processing
            # '▄' => currently in progress
            # '▁' => waiting in queue
            # ' ' => items not yet arrived in queue (estimated)
            #

            # Proportions of the bar from left to right
            part1 = float(counts.done) / max_count
            part2 = float(counts.get - counts.done) / max_count
            part3 = float(counts.put - counts.get) / max_count
            part4 = 1.0 - part3 - part2 - part1

            # How much is that in character count?
            part1 = int(part1 * bar_width)
            part2 = int(part2 * bar_width)
            part3 = int(part3 * bar_width)
            part4 = int(part4 * bar_width)

            # FIXME: code below uses fmt off/on to allow u string literals
            # with black. Drop this when python2 support goes away!
            # fmt: off
            bar1 = u"▇" * part1
            bar2 = u"▄" * part2
            bar3 = u"▁" * part3
            bar4 = u" " * part4
            # fmt: on

            # Seeing as we've truncated downwards to get integers, we may
            # need to pad a few more spaces to fill out the bar
            while len(bar1 + bar2 + bar3 + bar4) < bar_width:
                bar4 = bar4 + " "

            formatted_strs.append(template_str % (name, bar1, bar2, bar3, bar4))

            # Add counts to the structured event as well.
            event["phases"].append(
                {
                    "name": name,
                    "in-queue": counts.put - counts.get,
                    "in-progress": counts.get - counts.done,
                    "done": counts.done,
                    "total": max_count,
                }
            )

        LOG.info("Progress:\n  %s", "\n  ".join(formatted_strs), extra={"event": event})

    @contextmanager
    def progress_logger(self, interval=PROGRESS_INTERVAL):
        """A context manager for periodically logging the progress of all queues.

        While the context is open, progress will be logged periodically via
        dump_progress. This logging ends once the context is closed.
        """

        if interval <= 0:
            # Allows the feature to be entirely disabled.
            yield
            return

        stop_logging = Event()

        def loop():
            while not stop_logging.is_set():
                self.dump_progress()
                stop_logging.wait(timeout=interval)

        thread = Thread(name="progress-logger", target=loop)
        thread.daemon = True

        thread.start()
        try:
            yield
        finally:
            stop_logging.set()
            thread.join()
            self.dump_progress()
