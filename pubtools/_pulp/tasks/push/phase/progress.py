# -*- coding: utf-8 -*-

import logging
import os
from threading import Event, Thread
import shutil
from contextlib import contextmanager

# u here is not redundant since we still support py2...
# pylint: disable=redundant-u-string-prefix

LOG = logging.getLogger("pubtools.pulp")

# How long, in seconds, between our logging of current phase progress.
PROGRESS_INTERVAL = int(os.getenv("PUBTOOLS_PULP_PROGRESS_INTERVAL") or "300")


class ProgressInfo(object):
    """Records progress info for a single phase."""

    def __init__(self, name, in_count=0, out_count=0):
        self.name = name
        """Name of the associated phase or step."""

        self.in_count = in_count
        """How many items has this phase read from its input queue?"""

        self.out_count = out_count
        """How many items has this phase written to its output queue?"""

    def incr_in(self):
        self.in_count += 1

    def incr_out(self):
        self.out_count += 1

    @property
    def inprogress_count(self):
        """How many items are currently in progress, i.e. have been read
        from the input queue and not yet sent to the output?
        """
        return self.in_count - self.out_count

    def copy(self):
        return ProgressInfo(
            name=self.name, in_count=self.in_count, out_count=self.out_count
        )


class ProgressLogger(object):
    """A helper to periodically log a context's progress info."""

    def __init__(self, context):
        self.ctx = context

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

        item_info = self.ctx.item_info
        snapshot = []
        max_namelen = 0
        max_count = 1
        items_known = item_info.items_known.is_set()

        # Take a snapshot so that the counts don't update during our
        # calculations.
        snapshot = [pi.copy() for pi in self.ctx.progress_infos]

        for pi in snapshot:
            max_namelen = max(max_namelen, len(pi.name))
            max_count = max(max_count, pi.in_count)

        # When reporting progress, we may or may not know exactly how many
        # items we're dealing with, depending how far we are in the push.
        # If we know the exact count, use it.
        if items_known:
            max_count = item_info.items_count or 1

        template_str = "[ %%%ds | %%s%%s%%s ]" % max_namelen
        bar_width = max(width - max_namelen - 10, 10)

        # We will create both human-oriented strings and machine-oriented
        # structured metrics (logged via 'extra'). In practice this can be
        # gathered into JSONL logs.
        formatted_strs = []
        event = {"type": "progress-report", "phases": []}

        for pi in snapshot:
            # We want to draw a progress bar like this:
            #
            # "███████▒▒▒▒▒▒▒▒        "
            #
            # The bar fills in from left to right.
            #
            # '█' => done processing
            # '▒' => currently in progress
            # ' ' => items not yet arrived in queue (estimated)
            #

            # Proportions of the bar from left to right
            part1 = float(pi.out_count) / max_count
            part2 = float(pi.inprogress_count) / max_count
            part3 = 1.0 - part2 - part1

            # How much is that in character count?
            part1 = int(part1 * bar_width)
            part2 = int(part2 * bar_width)
            part3 = int(part3 * bar_width)

            # FIXME: code below uses fmt off/on to allow u string literals
            # with black. Drop this when python2 support goes away!
            # fmt: off
            bar1 = u"█" * part1
            bar2 = u"▒" * part2
            bar3 = u" " * part3
            # fmt: on

            # Seeing as we've truncated downwards to get integers, we may
            # need to pad a few more spaces to fill out the bar
            while len(bar1 + bar2 + bar3) < bar_width:
                bar3 = bar3 + " "

            bar_str = template_str % (pi.name, bar1, bar2, bar3)

            # If we don't know the exact item counts, we'd better indicate this
            # since the progress bar can otherwise be rather misleading. We do
            # this by pasting '???' near the end.
            if not items_known:
                bar_str = bar_str[:-6] + " ??? ]"

            formatted_strs.append(bar_str)

            # Add counts to the structured event as well.
            event["phases"].append(
                {
                    "name": pi.name,
                    "in-progress": pi.inprogress_count,
                    "done": pi.out_count,
                    "total": max_count,
                }
            )

        LOG.info("Progress:\n  %s", "\n  ".join(formatted_strs), extra={"event": event})

    @classmethod
    @contextmanager
    def for_context(cls, ctx, interval=PROGRESS_INTERVAL):
        """A context manager for periodically logging the progress of a context.

        While the context is open, progress will be logged periodically via
        dump_progress. This logging ends once the context is closed.
        """

        if interval <= 0:
            # Allows the feature to be entirely disabled.
            yield
            return

        stop_logging = Event()

        logger = cls(ctx)

        def loop():
            while not stop_logging.is_set():
                logger.dump_progress()
                stop_logging.wait(timeout=interval)

        thread = Thread(name="progress-logger", target=loop)
        thread.daemon = True

        thread.start()
        try:
            yield
        finally:
            stop_logging.set()
            thread.join()
            logger.dump_progress()
