import time
from threading import Thread

import pytest

from pubtools._pulp.tasks.push.phase import Context, Phase, context, base, constants


def test_batch_timeout(monkeypatch):
    # Set all these to known values so we don't get affected by the relevant env vars.
    monkeypatch.setattr(constants, "QUEUE_SIZE", 100)
    monkeypatch.setattr(constants, "BATCH_TIMEOUT", 0.1)
    monkeypatch.setattr(constants, "BATCH_MAX_TIMEOUT", 60.0)

    ctx = Context()
    phase = Phase(ctx)

    # It should initially calculate a timeout equal to BATCH_TIMEOUT.
    assert phase._Phase__batch_timeout == 0.1

    # Let's say the output queue becomes about 2/3rds full.
    for _ in range(0, 66):
        phase.out_queue.put(object())

    # Now it should wait up until 2/3rds of the max timeout.
    assert 39.0 < phase._Phase__batch_timeout < 41.0

    # Fill the output entirely.
    for _ in range(66, 100):
        phase.out_queue.put(object())

    # That should make it use the max timeout.
    assert phase._Phase__batch_timeout == 60.0


def test_iter_respects_timeout(monkeypatch):
    """iter_input_batched respects the requested timeout."""

    monkeypatch.setattr(constants, "QUEUE_SIZE", 100)
    monkeypatch.setattr(constants, "BATCH_TIMEOUT", 0.4)
    monkeypatch.setattr(constants, "BATCH_MAX_TIMEOUT", 0.4)

    ctx = Context()

    # Make this smaller than usual for a more responsive test.
    ctx.interrupt_interval = 0.1

    queue = ctx.new_queue()
    phase = Phase(ctx, in_queue=queue)

    # Start a thread which will put a few items quickly, and then slow down.
    stop_thread = []

    def write_items():
        queue.put([0])
        queue.put([1])
        queue.put([2])
        for i in range(3, 1000):
            time.sleep(1.0)
            if stop_thread:
                queue.put(constants.FINISHED)
                return
            queue.put([i])

    thread = Thread(target=write_items)
    thread.start()

    def stop_write_items():
        stop_thread.append(True)
        thread.join()

    try:
        # Try iterating over the items now.
        got_batches = []
        for batch in phase.iter_input_batched():
            got_batches.append(batch)
            # We only expect one batch, so ask the thread to stop
            # once we've seen it.
            stop_write_items()

        # If our timeout was respected then we should have received a
        # batch with only a few items.
        # Since the test depends on timing we aren't precise about the number
        # of items, a few possibilities are acceptable.
        expected_batches = []
        for i in range(0, 5):
            expected_batches.append(list(range(0, i)))

        # We can't guarantee that the thread shut down before writing more
        # items, so more than one batch is also OK.
        assert len(got_batches) >= 1

        # But in any case, the first batch should have only had a handful of
        # items, thus indicating the timeout was hit.
        assert got_batches[0] in expected_batches

    finally:
        stop_write_items()
