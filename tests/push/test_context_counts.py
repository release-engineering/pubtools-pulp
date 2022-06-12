# -*- coding: utf-8 -*-

import logging
import textwrap
import threading

from threading import Semaphore

from pubtools._pulp.tasks.push.phase import Context, Phase, ProgressLogger, constants
from pubtools._pulp.tasks.push.contextlib_compat import exitstack


class SynchronizedPhase(Phase):
    # A Phase implementation which processes items in batches
    # of a specified size, and which only makes progress when
    # we increment a semaphore so that the test has fine-grained
    # control over the progress.
    def __init__(self, in_sem, out_sem, batch_size, *args, **kwargs):
        self.in_sem = in_sem
        self.out_sem = out_sem
        self.batch_size = batch_size
        super(SynchronizedPhase, self).__init__(*args, **kwargs)

        # Arrange for a flush on every write.
        self.out_writer.flush_threshold = 1

    def run(self):
        for items in self.iter_input_batched(
            batch_size=self.batch_size,
        ):
            for item in items:
                self.in_sem.acquire()
                self.put_output(item)
                self.out_sem.release()


def test_context_counts(caplog):
    """Progress info is registered with context by default, using queue operations."""

    ctx = Context()
    progress_infos = ctx.progress_infos
    progress_logger = ProgressLogger(ctx)

    in_sem1 = Semaphore(0)
    in_sem2 = Semaphore(0)
    in_sem3 = Semaphore(0)
    out_sem1 = Semaphore(0)
    out_sem2 = Semaphore(0)
    out_sem3 = Semaphore(0)

    q1 = ctx.new_queue()
    q2 = ctx.new_queue()
    q3 = ctx.new_queue()
    q4 = ctx.new_queue()

    p1 = SynchronizedPhase(
        in_sem1, out_sem1, 10, context=ctx, in_queue=q1, out_queue=q2, name="phase 1"
    )
    p2 = SynchronizedPhase(
        in_sem2, out_sem2, 10, context=ctx, in_queue=q2, out_queue=q3, name="phase 2"
    )
    p3 = SynchronizedPhase(
        in_sem3, out_sem3, 10, context=ctx, in_queue=q3, out_queue=q4, name="phase 3"
    )

    # This should have already attached some ProgressInfo onto the context.
    assert [pi.name for pi in progress_infos] == ["phase 1", "phase 2", "phase 3"]

    # Now allow all the phases to start.
    with exitstack([p1, p2, p3]):
        # Put a few batches onto the first queue, 35 items in total.
        q1.put(list(range(0, 10)))
        q1.put(list(range(10, 20)))
        q1.put(list(range(20, 30)))
        q1.put(list(range(30, 35)))
        q1.put(constants.FINISHED)

        # Allow phases to make progress:
        # - p1 send 20 items
        # - p2 send 15 items
        # - p3 send 5 items
        for _ in range(0, 20):
            in_sem1.release()
            out_sem1.acquire()
        for _ in range(0, 15):
            in_sem2.release()
            out_sem2.acquire()
        for _ in range(0, 5):
            in_sem3.release()
            out_sem3.acquire()

        # We know exactly how much progress has been made, so check the queues now.
        try:
            # in, out should match exactly the progress that we've allowed.
            assert progress_infos[0].in_count == 30
            assert progress_infos[0].out_count == 20
            assert progress_infos[1].in_count == 20
            assert progress_infos[1].out_count == 15
            # in_count below is 10 because we still haven't finished processing
            # the first batch of 10.
            assert progress_infos[2].in_count == 10
            assert progress_infos[2].out_count == 5

            caplog.set_level(logging.INFO)
            progress_logger.dump_progress(width=70)

            # When visualized, this is what it should look like.
            # Note the ??? because the total number of items hasn't been set on
            # the context.
            assert (
                caplog.messages[-1].strip()
                # to allow u string literal... (FIXME: remove when py2 dropped)
                # fmt: off
                == textwrap.dedent(
                    u"""
                    Progress:
                      [ phase 1 | ███████████████████████████████████▒▒▒▒▒▒▒▒▒▒▒▒▒▒ ??? ]
                      [ phase 2 | ██████████████████████████▒▒▒▒▒▒▒▒                ??? ]
                      [ phase 3 | ████████▒▒▒▒▒▒▒▒                                  ??? ]
                    """
                ).strip()
                # fmt: on
            )

            # Similar info should be available in structured form (so it can go
            # into JSONL logs)
            rec = caplog.records[-1]
            assert rec.event == {
                "type": "progress-report",
                "phases": [
                    {"name": "phase 1", "done": 20, "in-progress": 10, "total": 30},
                    {"name": "phase 2", "done": 15, "in-progress": 5, "total": 30},
                    {"name": "phase 3", "done": 5, "in-progress": 5, "total": 30},
                ],
            }

            # Let's try logging again but this time after setting up
            # a known item count.
            ctx.item_info.items_count = 100
            ctx.item_info.items_known.set()
            progress_logger.dump_progress(width=70)

            # Now it should look like this. Compare to previously:
            # - bars have shrunk since actual item count (100) is larger than the
            #   previously estimated 35
            # - ??? is gone since the item count is no longer an estimate.
            assert (
                caplog.messages[-1].strip()
                # to allow u string literal... (FIXME: remove when py2 dropped)
                # fmt: off
                == textwrap.dedent(
                    u"""
                    Progress:
                      [ phase 1 | ██████████▒▒▒▒▒                                       ]
                      [ phase 2 | ███████▒▒                                             ]
                      [ phase 3 | ██▒▒                                                  ]
                    """
                ).strip()
                # fmt: on
            )

        finally:
            # Whether test passes or fails, release all the semaphores to avoid deadlock.
            for _ in range(0, 1000):
                in_sem1.release()
                in_sem2.release()
                in_sem3.release()


def test_context_progress_logger_disabled():
    """An interval of 0 disables the progress logger."""

    ctx = Context()

    # The progress logger is implemented by a thread, so we can verify that
    # nothing happens by checking that the thread count is stable.
    threadcount = len(threading.enumerate())

    with ProgressLogger.for_context(ctx, interval=0):
        # Should not have spawned a new thread.
        assert len(threading.enumerate()) == threadcount
