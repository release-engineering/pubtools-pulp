import logging

from pushsource import Source, FilePushItem, PushItem, RpmPushItem

from pubtools._pulp.tasks.push.phase import Context, Phase, LoadPushItems, constants


def test_load_filters():
    """Push items are filtered to supported Pulp destinations."""

    ctx = Context()
    phase = LoadPushItems(
        ctx,
        ["fake:"],
        allow_unsigned=True,
        pre_push=False,
    )

    # Set up these items to be generated by pushsource.
    # It simulates the ET case where some files are generated having
    # both pulp repo IDs and FTP paths.
    fake_items = [
        FilePushItem(name="file1", dest=["some-repo", "other-repo", "/some/path"]),
        FilePushItem(name="file2", dest=["/some/path", "/other/path"]),
        FilePushItem(name="file3", dest=["final-repo"]),
    ]
    Source.register_backend("fake", lambda: fake_items)

    # Let it run to completion...
    with phase:
        pass

    # It should have succeeded
    assert not ctx.has_error

    # Now let's get everything from the output queue.
    all_outputs = []
    while True:
        items = phase.out_queue.get()
        if items is constants.FINISHED:
            break
        all_outputs.extend([item.pushsource_item for item in items])

    # We should have got this:
    assert all_outputs == [
        # we get file1, but only repo IDs have been kept.
        FilePushItem(name="file1", dest=["some-repo", "other-repo"]),
        # we don't get file2 at all, since dest was filtered down to nothing.
        # we get file3 exactly as it was, since no changes were needed.
        FilePushItem(name="file3", dest=["final-repo"]),
    ]


def test_keep_prepush_no_dest_items():
    """Push item filtering keeps items with no dest if pre-pushable."""

    ctx = Context()
    phase = LoadPushItems(
        ctx,
        ["fake:"],
        allow_unsigned=True,
        pre_push=True,
    )

    fake_items = [
        FilePushItem(name="file", dest=["some-repo"]),
        RpmPushItem(name="rpm", dest=[]),
    ]
    Source.register_backend("fake", lambda: fake_items)

    # Let it run to completion...
    with phase:
        pass

    # It should have succeeded
    assert not ctx.has_error

    # Now let's get everything from the output queue.
    all_outputs = []
    while True:
        items = phase.out_queue.get()
        if items is constants.FINISHED:
            break
        all_outputs.extend([item.pushsource_item for item in items])

    # We should have got this:
    assert all_outputs == [
        # get file as usual
        FilePushItem(name="file", dest=["some-repo"]),
        # even though this item has no destination, we still get it since rpms
        # support pre-push and pre_push was enabled.
        RpmPushItem(name="rpm", dest=[]),
    ]
