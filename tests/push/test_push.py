import os
import datetime
import functools
import mock

import attr
import pytest

from pushsource import Source, PushItem, RpmPushItem

from pubtools.pulplib import (
    FileUnit,
    ErratumUnit,
    RpmUnit,
    RpmDependency,
    Criteria,
)
from pubtools.pluggy import pm

from pubtools._pulp.tasks.push import entry_point
from pubtools._pulp.tasks.push.phase import context

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


def test_empty_push(
    fake_controller, fake_push, fake_state_path, command_tester, stub_collector
):
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
        compare_jsonl=False,
        compare_extra=compare_extra,
    )

    # It should not record any push items at all.
    assert not stub_collector


def test_typical_push(
    fake_controller,
    data_path,
    fake_push,
    fake_state_path,
    command_tester,
    hookspy,
    stub_collector,
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
        # This push needs to allow unsigned since some of the test RPMs
        # are not signed. There is a separate case covering the behavior
        # when --allow-unsigned is omitted.
        "--allow-unsigned",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        compare_jsonl=False,
        # This will ensure the Pulp state matches the baseline.
        compare_extra=compare_extra,
    )

    # It should have invoked hook(s).
    assert len(hookspy) == 24
    (hook_name, hook_kwargs) = hookspy[0]
    assert hook_name == "task_start"
    (hook_name, hook_kwargs) = hookspy[1]
    assert hook_name == "pulp_repository_pre_publish"
    (hook_name, hook_kwargs) = hookspy[2]
    assert hook_name == "pulp_repository_published"
    # after pulp_repository_published there's 13 calls of pulp_item_finished
    (hook_name, hook_kwargs) = hookspy[-15]
    assert hook_name == "task_pulp_flush"
    (hook_name, hook_kwargs) = hookspy[-2]
    assert set(["item_metadata","push_item"]) == set(hook_kwargs.keys())
    assert isinstance(hook_kwargs['item_metadata'], dict)
    assert 'cdn_path' in  hook_kwargs['item_metadata']
    assert isinstance(hook_kwargs["push_item"], PushItem)

    assert hook_name == "pulp_item_finished"
    (hook_name, hook_kwargs) = hookspy[-1]
    assert hook_name == "task_stop"

    # It should have recorded various push items.
    # We don't try to verify the entire sequence of items here, it's too
    # cumbersome. Instead we pick a single item and trace the expected
    # changes over time:

    # This item should be found in the staging dir, at which point it's PENDING.
    item = {
        "build": None,
        "checksums": {
            "md5": "6a3eec6d45e0ea80eab05870bf7a8d4b",
            "sha256": "e837a635cc99f967a70f34b268baa52e0f412c1502e08e924ff5b09f1f9573f2",
        },
        "dest": "dest1",
        "filename": "walrus-5.21-1.noarch.rpm",
        "origin": stagedir,
        "signing_key": "F78FB195",
        "src": "%s/dest1/RPMS/walrus-5.21-1.noarch.rpm" % stagedir,
        "state": "PENDING",
    }

    # For the first two item states, we can't guarantee that the item ever
    # makes it to the collector - it depends how fast we run. If we are
    # able to run to completion faster than the collect phase can grab items
    # from its queue, it will de-duplicate items and keep only later states.
    # All we can say is that the non-terminal states should appear 0 or 1
    # times.
    pending_count = stub_collector.count(item)
    assert pending_count in (0, 1)
    pending_idx = None if not pending_count else stub_collector.index(item)

    # Then it should become EXISTS once we've uploaded it to Pulp.
    item["state"] = "EXISTS"
    exists_count = stub_collector.count(item)
    assert exists_count in (0, 1)
    exists_idx = None if not exists_count else stub_collector.index(item)

    # And finally it should become PUSHED once publishing completes.
    # This is the only state we know *must* make it into the collector,
    # since it's the terminal state and no de-duplication can occur.
    item["state"] = "PUSHED"
    assert stub_collector.count(item) == 1
    pushed_idx = stub_collector.index(item)

    # If the item was indeed recorded at multiple states, those states
    # must have occurred in the correct order...
    if pending_idx is not None and exists_idx is not None:
        assert pending_idx < exists_idx
    if exists_idx is not None:
        assert exists_idx < pushed_idx

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


def test_nopublish_push(
    fake_controller,
    data_path,
    fake_push,
    fake_state_path,
    command_tester,
    stub_collector,
):
    """A push with `--skip publish' should complete successfully but not
    publish any Pulp repos.
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
        "--skip",
        "publish",
        "--source",
        "staged:%s" % stagedir,
        "--allow-unsigned",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        compare_jsonl=False,
        # This will ensure the Pulp state matches the baseline.
        compare_extra=compare_extra,
    )

    # We can determine that publish didn't occur by checking all
    # encountered states of push items.
    all_states = set([item["state"] for item in stub_collector])

    # Everything should be either PENDING (before upload to Pulp)
    # or EXISTS (after upload), but nothing should be PUSHED since
    # publish didn't happen.
    assert all_states == set(["PENDING", "EXISTS"])


def test_unsigned_failure(
    fake_push,
    command_tester,
    caplog,
):
    """Test that a failure occurs if an unsigned RPM is encountered without
    the --allow-unsigned option.
    """

    Source.register_backend(
        "unsigned",
        lambda: [RpmPushItem(name="quux", src="/some/unsigned.rpm", dest=["repo1"])],
    )

    args = [
        "",
        "--source",
        "unsigned:",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should exit...
    with pytest.raises(SystemExit) as excinfo:
        command_tester.test(
            run,
            args,
            compare_plaintext=False,
            compare_jsonl=False,
        )

    # ...unsuccessfully
    assert excinfo.value.code != 0

    # And it should tell us what went wrong
    assert "Unsigned content is not permitted: /some/unsigned.rpm" in caplog.text


def test_update_push(
    fake_controller, data_path, fake_push, fake_state_path, command_tester, monkeypatch
):
    """Test a more complex push where items already exist in Pulp in a variety of
    different states.
    """

    # For this test we'll force an abnormally small queue size.
    # This will verify that nothing breaks in edge cases such as the queue size
    # being smaller than the batch size.
    monkeypatch.setenv("PUBTOOLS_PULP_QUEUE_SIZE", "1")

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
        "--allow-unsigned",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

    # It should be able to run without crashing.
    command_tester.test(
        run,
        args,
        compare_plaintext=False,
        compare_jsonl=False,
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
        repository_memberships=["iso-dest1", "iso-dest2"],
    )

    # Erratum after push should be updated. The full update will not be tested here
    # as it's extremely verbose, we'll just sample some fields. But, critically,
    # the 'version' field (which was not an integer in pulp) should have been
    # simply overwritten with the input rather than bumped.
    assert updated_erratum.title == "Important: sudo security update"
    assert updated_erratum.pkglist
    assert updated_erratum.version == "3"
