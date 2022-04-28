from pubtools._pulp.tasks.push.items import (
    PulpFilePushItem,
    PulpModuleMdPushItem,
    PulpRpmPushItem,
)
from pubtools._pulp.tasks.push.phase import Context, Associate, Phase


def test_associate_order():
    """Associate phase reorders items so that RPMs are processed last."""

    ctx = Context()
    queue = ctx.new_queue(maxsize=10000)
    phase = Associate(
        context=ctx,
        pulp_client=None,
        pre_push=None,
        allow_unsigned=True,
        in_queue=queue,
    )

    # Arrange for various items coming into the associate phase noting that
    # RPMs are not last.
    # Note: items don't have to actually be valid for this test, so we're just
    # creating a lot of empty objects.
    # Also, we use awkward numbers so it doesn't line up exactly with
    # BATCH_SIZE.
    rpms = [PulpRpmPushItem(pushsource_item=None) for _ in range(0, 1003)]
    modules = [PulpModuleMdPushItem(pushsource_item=None) for _ in range(0, 2040)]
    files = [PulpFilePushItem(pushsource_item=None) for _ in range(0, 300)]

    rpm_ids = [id(rpm) for rpm in rpms]
    module_ids = [id(module) for module in modules]
    file_ids = [id(file) for file in files]

    # Put everything on the queue...
    for item in rpms + modules + files:
        queue.put(item)

    # Put this so that iteration will end
    queue.put(Phase.FINISHED)

    # Now let's see how iteration over it will work out
    got_ids = []
    for batch in phase.iter_for_associate():
        got_ids.extend([id(item) for item in batch])

    # We should have got all the same items back out as we put in,
    # but the order is different: RPMs have been shifted to the
    # end (and the order is otherwise the same as the input)
    assert got_ids == module_ids + file_ids + rpm_ids
