from pushsource import FilePushItem
import attr

from more_executors.futures import f_return
from pubtools._pulp.tasks.push.items import (
    PulpFilePushItem,
    PulpModuleMdPushItem,
    PulpRpmPushItem,
)
from pubtools._pulp.tasks.push.phase import Context, Collect, Phase, constants


def test_collect_dupes():
    """Collect phase filters out duplicate items during iteration."""

    ctx = Context()
    phase = Collect(context=ctx, collector=None)

    # Set up some items to put onto the queue.
    files = [
        PulpFilePushItem(
            pushsource_item=FilePushItem(
                name="file%s" % i,
                dest=["some-repo"],
                src="/fake/file%s" % i,
                state="PENDING",
            )
        )
        for i in range(0, 10)
    ]

    # Let's add some duplicates of what's already there, just with an
    # updated state.
    files.append(
        attr.evolve(
            files[0],
            pushsource_item=attr.evolve(files[0].pushsource_item, state="EXISTS"),
        )
    )
    files.append(
        attr.evolve(
            files[0],
            pushsource_item=attr.evolve(files[0].pushsource_item, state="PUSHED"),
        )
    )
    files.append(
        attr.evolve(
            files[4],
            pushsource_item=attr.evolve(files[4].pushsource_item, state="WHATEVER"),
        )
    )

    # Sanity check: now we have this many files
    assert len(files) == 13

    # Put everything on the queue...
    phase.in_queue.put(files)

    # Put this so that iteration will end
    phase.in_queue.put(constants.FINISHED)

    # Now let's see how iteration over it will work out
    got_items = []
    for batch in phase.iter_for_collect():
        got_items.extend([i.pushsource_item for i in batch])

    # We got this many items - 3 dupes filtered, so only 10
    assert len(got_items) == 10

    # And let's check exactly what we got:
    # - order should be the same as the input, but...
    # - items at index 0 and 4 use their last submitted STATE rather
    #   than the original
    assert got_items == [
        FilePushItem(
            name="file0",
            dest=["some-repo"],
            src="/fake/file0",
            state="PUSHED",
        ),
        FilePushItem(
            name="file1",
            dest=["some-repo"],
            src="/fake/file1",
            state="PENDING",
        ),
        FilePushItem(
            name="file2",
            dest=["some-repo"],
            src="/fake/file2",
            state="PENDING",
        ),
        FilePushItem(
            name="file3",
            dest=["some-repo"],
            src="/fake/file3",
            state="PENDING",
        ),
        FilePushItem(
            name="file4",
            dest=["some-repo"],
            src="/fake/file4",
            state="WHATEVER",
        ),
        FilePushItem(
            name="file5",
            dest=["some-repo"],
            src="/fake/file5",
            state="PENDING",
        ),
        FilePushItem(
            name="file6",
            dest=["some-repo"],
            src="/fake/file6",
            state="PENDING",
        ),
        FilePushItem(
            name="file7",
            dest=["some-repo"],
            src="/fake/file7",
            state="PENDING",
        ),
        FilePushItem(
            name="file8",
            dest=["some-repo"],
            src="/fake/file8",
            state="PENDING",
        ),
        FilePushItem(
            name="file9",
            dest=["some-repo"],
            src="/fake/file9",
            state="PENDING",
        ),
    ]
