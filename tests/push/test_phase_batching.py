from pubtools._pulp.tasks.push.phase import Context, Phase, context, base


def test_batch_timeout(monkeypatch):

    # Set all these to known values so we don't get affected by the relevant env vars.
    monkeypatch.setattr(base, "QUEUE_SIZE", 100)
    monkeypatch.setattr(context, "QUEUE_SIZE", base.QUEUE_SIZE)
    monkeypatch.setattr(base, "BATCH_TIMEOUT", 0.1)
    monkeypatch.setattr(base, "BATCH_MAX_TIMEOUT", 60.0)

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
