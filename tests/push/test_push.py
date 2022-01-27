import os
import datetime
import functools

import attr
import pytest

from pushsource import Source, PushItem

from pubtools.pulplib import (
    FileUnit,
    ErratumUnit,
    RpmUnit,
    RpmDependency,
    Criteria,
)
from pubtools.pluggy import pm

from pubtools._pulp.tasks.push import entry_point

from .util import hide_unit_ids


@pytest.fixture
def hookspy():
    hooks = []

    def record_hook(hook_name, _hook_impls, kwargs):
        hooks.append((hook_name, kwargs))

    def do_nothing(*args, **kwargs):
        pass

    undo = pm.add_hookcall_monitoring(before=record_hook, after=do_nothing)
    yield hooks
    undo()


def test_empty_push(fake_controller, fake_push, fake_state_path, command_tester):
    """Test a push with no content."""

    # Sanity check that the Pulp server is, initially, empty.
    client = fake_controller.client
    assert list(client.search_content()) == []

    # Set up a pushsource backend which returns no supported items
    Source.register_backend("null", lambda: [PushItem(name="quux")])

    compare_extra = {
        "pulp.yaml": {
            "filename": fake_state_path,
            "normalize": hide_unit_ids,
        }
    }
    args = [
        "",
        "--source",
        "null:",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        compare_extra=compare_extra,
    )


def test_typical_push(
    fake_controller, data_path, fake_push, fake_state_path, command_tester, hookspy
):
    """Test a typical case of push using all sorts of content where the content
    is initially not present in Pulp.
    """
    # Sanity check that the Pulp server is, initially, empty.
    client = fake_controller.client
    assert list(client.search_content()) == []

    # Set it up to find content from our staging dir, which contains a mixture
    # of just about every content type
    stagedir = os.path.join(data_path, "staged-mixed")

    compare_extra = {
        "pulp.yaml": {
            "filename": fake_state_path,
            "normalize": hide_unit_ids,
        }
    }
    args = [
        "",
        "--source",
        "staged:%s" % stagedir,
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        # TODO: find a way to normalize logs such that we can do plaintext compare?
        compare_plaintext=False,
        # This will ensure the Pulp state matches the baseline.
        compare_extra=compare_extra,
    )

    # It should have invoked hook(s).
    assert len(hookspy) == 9
    (hook_name, hook_kwargs) = hookspy[0]
    assert hook_name == "task_start"
    (hook_name, hook_kwargs) = hookspy[1]
    assert hook_name == "pulp_repository_pre_publish"
    (hook_name, hook_kwargs) = hookspy[2]
    assert hook_name == "pulp_repository_published"
    (hook_name, hook_kwargs) = hookspy[7]
    assert hook_name == "task_pulp_flush"
    (hook_name, hook_kwargs) = hookspy[8]
    assert hook_name == "task_stop"

    # Since push is supposed to be idempotent, we should be able to redo
    # the same command and the pulp state should be exactly the same after the
    # second push.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        compare_jsonl=False,
        compare_extra=compare_extra,
    )


def test_update_push(
    fake_controller, data_path, fake_push, fake_state_path, command_tester
):
    """Test a more complex push where items already exist in Pulp in a variety of
    different states.
    """

    # Sanity check that the Pulp server is, initially, empty.
    client = fake_controller.client
    assert list(client.search_content()) == []

    all_rpm_content = client.get_repository("all-rpm-content").result()
    iso_dest1 = client.get_repository("iso-dest1").result()
    dest1 = client.get_repository("dest1").result()

    # Make this RPM exist, but not in all the desired repos.
    existing_rpm = RpmUnit(
        cdn_published=datetime.datetime(2021, 12, 14, 9, 59),
        arch="src",
        filename="test-srpm01-1.0-1.src.rpm",
        md5sum="ba9257ced24f77f4d777e399e67924f5",
        name="test-srpm01",
        version="1.0",
        release="1",
        provides=[],
        requires=[
            RpmDependency(
                epoch="0",
                version="4.6.0",
                release="1",
                flags="LE",
                name="rpmlib(FileDigests)",
            ),
            RpmDependency(
                epoch="0",
                version="3.0.4",
                release="1",
                flags="LE",
                name="rpmlib(CompressedFileNames)",
            ),
        ],
        sha1sum="d9629c034fed3a2f47870fc6fdc78a30c5556e1d",
        sha256sum="54cc4713fe704dfc7a4fd5b398f834ceb6a692f53b0c6aefaf89d88417b4c51d",
        unit_id="existing-rpm-id1",
    )
    fake_controller.insert_units(all_rpm_content, [existing_rpm])

    # Make this file exist, but with an outdated description.
    existing_file = FileUnit(
        cdn_path="/content/origin/files/sha256/db/db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb/some-iso",
        cdn_published=datetime.datetime(2021, 12, 14, 9, 59),
        description="A wrong description",
        path="some-iso",
        sha256sum="db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb",
        size=46,
        unit_id="existing-file-id1",
    )
    fake_controller.insert_units(iso_dest1, [existing_file])

    # Make this file exist, but in no repos at all, making it an orphan
    orphan_file = FileUnit(
        cdn_path="/content/origin/files/sha256/d8/d8301c5f72f16455dbc300f3d1bef8972424255caad103cc6c7ba7dc92d90ca8/test.txt",
        cdn_published=datetime.datetime(2021, 12, 14, 9, 59),
        path="test.txt",
        sha256sum="d8301c5f72f16455dbc300f3d1bef8972424255caad103cc6c7ba7dc92d90ca8",
        size=33,
        unit_id="orphan-file-id1",
    )
    fake_controller.insert_units(None, [orphan_file])

    # Make this erratum exist, but with most fields missing
    existing_erratum = ErratumUnit(
        id="RHSA-2020:0509",
        unit_id="existing-erratum-id1",
        # make this have a non-integral version right now so usual bumping
        # does not work
        version="oops-not-integer",
    )
    fake_controller.insert_units(dest1, [existing_erratum])

    # Set it up to find content from our staging dir, which contains a mixture
    # of just about every content type
    stagedir = os.path.join(data_path, "staged-mixed")

    compare_extra = {
        "pulp.yaml": {
            "filename": fake_state_path,
            "normalize": hide_unit_ids,
        }
    }
    args = [
        "",
        "--source",
        "staged:%s" % stagedir,
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        # This will ensure the Pulp state matches the baseline.
        compare_extra=compare_extra,
    )

    # Pulp state is covered by compare_extra, but let's also explicitly compare
    # the changes we expect on those existing units...

    updated_rpm = list(
        client.search_content(Criteria.with_field("unit_id", existing_rpm.unit_id))
    )
    assert len(updated_rpm) == 1
    updated_rpm = updated_rpm[0]

    updated_file = list(
        client.search_content(Criteria.with_field("unit_id", existing_file.unit_id))
    )
    assert len(updated_file) == 1
    updated_file = updated_file[0]

    updated_orphan_file = list(
        client.search_content(Criteria.with_field("unit_id", orphan_file.unit_id))
    )
    assert len(updated_orphan_file) == 1
    updated_orphan_file = updated_orphan_file[0]

    updated_erratum = list(
        client.search_content(Criteria.with_field("unit_id", existing_erratum.unit_id))
    )
    assert len(updated_erratum) == 1
    updated_erratum = updated_erratum[0]

    # RPM after push should be as it was before except that dest1 was added into
    # repository_memberships.
    assert updated_rpm == attr.evolve(
        existing_rpm, repository_memberships=["all-rpm-content", "dest1"]
    )

    # File after push should be as it was before except that description was updated
    # to the desired value.
    assert updated_file == attr.evolve(updated_file, description="My wonderful ISO")

    # Orphaned file after push should be as it was before except no longer an orphan.
    assert updated_orphan_file == attr.evolve(
        orphan_file,
        repository_memberships=["iso-dest1"],
    )

    # Erratum after push should be updated. The full update will not be tested here
    # as it's extremely verbose, we'll just sample some fields. But, critically,
    # the 'version' field (which was not an integer in pulp) should have been
    # simply overwritten with the input rather than bumped.
    assert updated_erratum.title == "Important: sudo security update"
    assert updated_erratum.pkglist
    assert updated_erratum.version == "3"
