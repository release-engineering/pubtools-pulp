# -*- coding: utf-8 -*-

import logging
import textwrap
import threading

from threading import Semaphore

from pubtools._pulp.tasks.push.phase import Context, Phase
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

    def run(self):
        for items in self.iter_input_batched(
            batch_size=self.batch_size,
        ):
            for item in items:
                self.in_sem.acquire()
                self.put_output(item)
                self.out_sem.release()


def test_context_counts(caplog):
    """Context object counts events on queues, which can be logged
    by dump_progress."""

    ctx = Context()
    queues = ctx._queues

    in_sem1 = Semaphore(0)
    in_sem2 = Semaphore(0)
    in_sem3 = Semaphore(0)
    out_sem1 = Semaphore(0)
    out_sem2 = Semaphore(0)
    out_sem3 = Semaphore(0)

    q1 = ctx.new_queue()
    q2 = ctx.new_queue()
    q3 = ctx.new_queue()
    # last queue does not use counting since nothing consumes from it.
    q4 = ctx.new_queue(counting=False)

    p1 = SynchronizedPhase(
        in_sem1, out_sem1, 10, context=ctx, in_queue=q1, out_queue=q2, name="phase 1"
    )
    p2 = SynchronizedPhase(
        in_sem2, out_sem2, 10, context=ctx, in_queue=q2, out_queue=q3, name="phase 2"
    )
    p3 = SynchronizedPhase(
        in_sem3, out_sem3, 10, context=ctx, in_queue=q3, out_queue=q4, name="phase 3"
    )

    # Nothing has started yet, so all queues should have zero counts.
    # Note: there are 4 queues since the last phase also has an output queue, but
    # since counting is not enabled for that one, we ignore it when checking counts.
    assert len(queues) == 4
    for q in queues[:3]:
        assert q.counts == (0, 0, 0)

    # Now allow all the phases to start.
    with exitstack([p1, p2, p3]):
        # Fill up the first queue with 35 items (3.5 batches) and then an item
        # informing that we're done.
        for i in range(0, 35):
            q1.put(i)
        q1.put(Phase.FINISHED)

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
            assert queues[0].name == "phase 1"
            c1 = queues[0].counts

            assert queues[1].name == "phase 2"
            c2 = queues[1].counts

            assert queues[2].name == "phase 3"
            c3 = queues[2].counts

            # (put, get, done) counts should match exactly the progress
            # that we've allowed. Keep in mind that gets always happen
            # in multiples of 10 since that's our batch size.
            assert c1 == (35, 30, 20)
            assert c2 == (20, 20, 15)
            assert c3 == (15, 10, 5)

            caplog.set_level(logging.INFO)
            ctx.dump_progress(width=70)

            # When visualized, this is what it should look like.
            assert (
                caplog.messages[-1].strip()
                # to allow u string literal... (FIXME: remove when py2 dropped)
                # fmt: off
                == textwrap.dedent(
                    u"""
                    Progress:
                      [ phase 1 | ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▁▁▁▁▁▁▁  ]
                      [ phase 2 | ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▄▄▄▄▄▄▄                         ]
                      [ phase 3 | ▇▇▇▇▇▇▇▄▄▄▄▄▄▄▁▁▁▁▁▁▁                                 ]
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
                    {
                        "name": "phase 1",
                        "in-queue": 5,
                        "in-progress": 10,
                        "done": 20,
                        "total": 35,
                    },
                    {
                        "name": "phase 2",
                        "in-queue": 0,
                        "in-progress": 5,
                        "done": 15,
                        "total": 35,
                    },
                    {
                        "name": "phase 3",
                        "in-queue": 5,
                        "in-progress": 5,
                        "done": 5,
                        "total": 35,
                    },
                ],
            }

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

    with ctx.progress_logger(interval=0):
        # Should not have spawned a new thread.
        assert len(threading.enumerate()) == threadcount
