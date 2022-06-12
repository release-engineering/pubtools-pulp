import time

import attr
import pytest
from pushsource import FilePushItem

from pubtools._pulp.tasks.push.phase import (
    LoadChecksums,
    Context,
    Phase,
    buffer,
    errors,
    constants,
)
from pubtools._pulp.tasks.push.items import PulpFilePushItem

# arbitrary fake checksum values
FAKE_MD5 = "d3b07a382ec010c01889250fce66fb13"
FAKE_SHA256 = "49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063"


class SlowFilePushItem(FilePushItem):
    # A FilePushItem which takes a bit of time to calculate checksums.
    def with_checksums(self):
        time.sleep(0.2)
        return super(SlowFilePushItem, self).with_checksums()


@attr.s
class SpyingFilePushItem(FilePushItem):
    # A FilePushItem which records with_checksum calls onto a list.
    spy = attr.ib(default=None)

    def with_checksums(self):
        self.spy.append(True)
        return super(SpyingFilePushItem, self).with_checksums()


def test_load_blocking_vs_nonblocking(tmpdir):
    """Verify that the phase efficiently handles both items where with_checksums
    will block, and items where with_checksums will immediately return.
    """
    ctx = Context()
    in_queue = ctx.new_queue()
    in_queue_writer = buffer.OutputBuffer(in_queue, ctx)

    # Add various push items onto the queue.
    all_filenames = []
    for i in range(0, 16):
        filename = "file%s" % i
        all_filenames.append(filename)
        filepath = tmpdir.join(filename)
        filepath.write(str(i))

        # Do a 50-50 mix between:
        # - even items: checksums are already known
        # - odd items: checksums are not known (and calculating them
        #   is not instantaneous)
        #
        if i % 2 == 0:
            item = FilePushItem(
                name=filename, src=str(filepath), md5sum=FAKE_MD5, sha256sum=FAKE_SHA256
            )
        else:
            item = SlowFilePushItem(name=filename, src=str(filepath))

        in_queue_writer.write(PulpFilePushItem(pushsource_item=item))

    in_queue_writer.flush()
    in_queue.put(constants.FINISHED)

    # Prepare the phase for loading checksums.
    phase = LoadChecksums(
        context=ctx,
        in_queue=in_queue,
        # Don't care about update_push_items for this test
        update_push_items=lambda *_: (),
    )

    # Tweak phase write behavior to ensure that all items handled synchronously
    # will be flushed before items handled asynchronously.
    phase.out_writer.flush_threshold = 1
    phase.out_writer.max_futures = 8

    # Let it run...
    with phase:
        pass

    # Should not have been any errors
    assert not ctx.has_error

    # Now let's get everything from the output queue.
    all_outputs = []
    while True:
        items = phase.out_queue.get()
        if items is constants.FINISHED:
            break
        all_outputs.extend(items)

    # Check the order of the files we've got:
    names = [i.pushsource_item.name for i in all_outputs]

    # Naturally we should have got all the same names back as we put in
    assert sorted(names) == sorted(all_filenames)

    # However, all the *even* names should have come first - and in the same
    # order as the input queue - because those had checksums available and
    # so could be yielded immediately.
    assert names[0:8] == [
        "file0",
        "file2",
        "file4",
        "file6",
        "file8",
        "file10",
        "file12",
        "file14",
    ]


def test_load_async_error(tmpdir, caplog):
    """Verify that the phase correctly handles errors which occur during
    async calls to with_checksums().
    """
    ctx = Context()
    in_queue = ctx.new_queue()
    in_queue_writer = buffer.OutputBuffer(in_queue, ctx)

    # Add various push items onto the queue:
    spied_calls = []

    # First some valid files which should be able to read OK (though
    # a little slow)
    for i in range(0, 8):
        filename = "file%s" % i
        filepath = tmpdir.join(filename)
        filepath.write(str(i))
        item = SlowFilePushItem(name=filename, src=str(filepath))
        in_queue_writer.write(PulpFilePushItem(pushsource_item=item))

    # Now let's throw in some files which don't exist and
    # therefore an error will occur when processing them
    for i in range(0, 8):
        filename = "notexist%s" % i
        filepath = tmpdir.join(filename)
        item = SlowFilePushItem(name=filename, src=str(filepath))
        in_queue_writer.write(PulpFilePushItem(pushsource_item=item))

    # And finally a few more files at the end just to spy on
    # whether processing reaches that point
    for i in range(0, 8):
        filename = "spy%s" % i
        filepath = tmpdir.join(filename)
        item = SpyingFilePushItem(spy=spied_calls, name=filename, src=str(filepath))
        in_queue_writer.write(PulpFilePushItem(pushsource_item=item))

    in_queue_writer.flush()
    in_queue.put(constants.FINISHED)

    # Prepare the phase for loading checksums.
    phase = LoadChecksums(
        context=ctx,
        in_queue=in_queue,
        # Don't care about update_push_items for this test
        update_push_items=lambda *_: (),
    )

    # Ensure max futures is small with respect to the number of files we've
    # set up in test data
    phase.out_writer.max_futures = 4

    # Let it run...
    with phase:
        pass

    # There should have been an error
    assert ctx.has_error

    # It should NOT have managed to get up to the last files.
    # This is important to verify that the phase didn't continue running
    # after a fatal error occurred.
    assert len(spied_calls) == 0

    # It should have logged the failure details.
    assert "Calculate checksums: fatal error occurred" in caplog.text
    assert "No such file or directory" in caplog.text
    assert "notexist" in caplog.text
