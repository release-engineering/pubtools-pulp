import os
from functools import partial

import attr
from more_executors.futures import f_map


from pubtools.pulplib import FakeController, YumRepository
from pushsource import RpmPushItem
from pubtools._pulp.tasks.push.items import (
    PulpRpmPushItem,
)
from pubtools._pulp.tasks.push.phase import Context, Upload, Phase, constants

# Wrap Pulp client/repo objects to spy on uploads.
class RepoWrapper(object):
    def __init__(self, delegate, uploads):
        self.delegate = delegate
        self.uploads = uploads

    def upload_rpm(self, path, *args, **kwargs):
        self.uploads.append(("rpm", path))
        return self.delegate.upload_rpm(path, *args, **kwargs)


class ClientWrapper(object):
    def __init__(self, delegate):
        self.delegate = delegate
        self.search_content = delegate.search_content
        self.uploads = []

    def get_repository(self, *args, **kwargs):
        wrapper = partial(RepoWrapper, uploads=self.uploads)
        return f_map(self.delegate.get_repository(*args, **kwargs), wrapper)


def test_uploads_shared(data_path):
    """Upload phase allows for uploads of identical content to be reused."""

    pulp_ctrl = FakeController()

    pulp_ctrl.insert_repository(YumRepository(id="all-rpm-content"))
    pulp_ctrl.insert_repository(YumRepository(id="repo1"))
    pulp_ctrl.insert_repository(YumRepository(id="repo2"))
    pulp_ctrl.insert_repository(YumRepository(id="repo3"))

    client_wrapper = ClientWrapper(pulp_ctrl.client)

    ctx = Context()
    queue = ctx.new_queue()
    phase = Upload(
        context=ctx,
        pulp_client=client_wrapper,
        pre_push=None,
        in_queue=queue,
        update_push_items=lambda _: None,
    )

    rpm1 = RpmPushItem(
        name="walrus-5.21-1.noarch.rpm",
        sha256sum="e837a635cc99f967a70f34b268baa52e0f412c1502e08e924ff5b09f1f9573f2",
        src=os.path.join(data_path, "staged-mixed/dest1/RPMS/walrus-5.21-1.noarch.rpm"),
    )
    rpm2 = RpmPushItem(
        name="test-srpm01-1.0-1.src.rpm",
        sha256sum="54cc4713fe704dfc7a4fd5b398f834ceb6a692f53b0c6aefaf89d88417b4c51d",
        src=os.path.join(
            data_path, "staged-mixed/dest1/SRPMS/test-srpm01-1.0-1.src.rpm"
        ),
    )

    inputs = [
        # Some copies of the same RPM to different repos
        PulpRpmPushItem(pushsource_item=attr.evolve(rpm1, dest=["repo1"])),
        PulpRpmPushItem(pushsource_item=attr.evolve(rpm1, dest=["repo2", "repo3"])),
        # A different RPM
        PulpRpmPushItem(pushsource_item=attr.evolve(rpm2, dest=["repo1"])),
    ]

    # Shove 'em into the queue
    queue.put(inputs)

    # Put this so that iteration will end
    queue.put(constants.FINISHED)

    # Let the phase run
    with phase:
        pass

    # It should not have failed
    assert not ctx.has_error

    # Should have called upload exactly once per file.
    assert sorted(client_wrapper.uploads) == [
        ("rpm", rpm1.src),
        ("rpm", rpm2.src),
    ]

    # Look at the pulp units created.
    outputs = {}
    while True:
        items = phase.out_queue.get()
        if items is constants.FINISHED:
            break
        for item in items:
            outputs.setdefault(item.pushsource_item.name, []).append(item.pulp_unit)

    # Although there were two items dealing with this RPM...
    assert len(outputs["walrus-5.21-1.noarch.rpm"]) == 2

    # If we de-duplicate, we'll find they're actually the same unit since
    # upload was shared.
    assert len(set(outputs["walrus-5.21-1.noarch.rpm"])) == 1

    # And the non-dupe should just work as normal.
    assert len(outputs["test-srpm01-1.0-1.src.rpm"]) == 1
