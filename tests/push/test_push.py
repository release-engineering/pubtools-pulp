import os
import datetime
import functools
import re
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
    Unit,
)
from pubtools.pluggy import pm

from pubtools._pulp.tasks.push import entry_point, command
from pubtools._pulp.tasks.push.phase import context, constants

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
    monkeypatch,
):
    """Test a typical case of push using all sorts of content where the content
    is initially not present in Pulp.
    """
    # patch this constant to disallow duplicate units to be uploaded to Pulp.
    # should be a no-op for non-RPM units, should pass for RPM units.
    monkeypatch.setattr(constants, "ALLOW_DUPLICATE_UNITS", False)

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
    assert len(hookspy) == 25
    hook_name, hook_kwargs = hookspy[0]
    assert hook_name == "task_start"
    hook_name, hook_kwargs = hookspy[1]
    assert hook_name == "get_cert_key_paths"
    hook_name, hook_kwargs = hookspy[2]
    assert hook_name == "pulp_repository_pre_publish"
    hook_name, hook_kwargs = hookspy[3]
    assert hook_name == "pulp_repository_published"
    # after pulp_repository_published there's 13 calls of pulp_item_push_finished
    hook_name, hook_kwargs = hookspy[-15]
    assert hook_name == "task_pulp_flush"
    hook_name, hook_kwargs = hookspy[-4]
    for hook_called in hookspy[15:-4]:
        hook_name, hook_kwargs = hook_called
        if hook_kwargs["pulp_units"]:
            break

    assert set(["pulp_units", "push_item"]) == set(hook_kwargs.keys())
    assert isinstance(hook_kwargs["pulp_units"], list)
    assert isinstance(hook_kwargs["pulp_units"][0], Unit)
    assert isinstance(hook_kwargs["push_item"], PushItem)

    assert hook_name == "pulp_item_push_finished"
    hook_name, hook_kwargs = hookspy[-1]
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
    # repository_memberships and cdn_path was updated as well.
    assert updated_rpm == attr.evolve(
        existing_rpm,
        repository_memberships=["all-rpm-content", "dest1"],
        cdn_path="/content/origin/rpms/test-srpm01/1.0/1/none/test-srpm01-1.0-1.src.rpm",
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

@pytest.mark.parametrize("pre_push", [True, False])
@mock.patch("pubtools._pulp.tasks.push.phase.sync.uuid.uuid4", return_value="fake-uuid")
@mock.patch("pubtools.pulplib.YumRepository.sync")
def test_push_with_sync(
    mock_sync,
    mock_uuid,
    fake_controller,
    data_path,
    fake_push,
    fake_state_path,
    command_tester,
    monkeypatch,
    httpx_mock,
    pre_push,
):
    """Test a push with sync phase, having also existing units in Pulp. Tests also pre-push mode"""
    client = fake_controller.client
    assert list(client.search_content()) == []

    # patch this constant to disallow duplicate units to be uploaded to Pulp.
    # should be a no-op for non-RPM units, should pass for RPM units.
    monkeypatch.setattr(constants, "ALLOW_DUPLICATE_UNITS", False)
    monkeypatch.setattr(command, "KONFLUX_SOURCE_ENABLED", True)
    
    def import_unit(*_):
        repo = client.get_repository("tmp-konflux-fake-uuid").result()
        fake_controller.insert_units(repo, [rpm_to_sync])
        return mock.MagicMock()
        
    # this will make the RPM exist in repo that is synced from konflux source
    mock_sync.side_effect = import_unit
    
    konflux_dir = os.path.join(data_path, "konflux-src")

    _mock_pulp3_queries_sync_phase(httpx_mock)
    compare_extra = {
        "pulp.yaml": {
            "filename": fake_state_path,
            "normalize": hide_unit_ids,
        }
    }
    args = [
        "",
        "--source",
        "konflux:%s?%s&%s&%s&%s&%s"
        % (
            konflux_dir,
            "advisories=RHSA-2020:0509",
            "pulp_user=test",
            "pulp_password=test",
            "pulp_url=https://pulp3.example.com",
            "pulp_domain=test",
        ),
        "--allow-unsigned",
        "--pulp-url",
        "https://pulp.example.com/",
        "--pulp3-url",
        "https://pulp3.example.com/",
        "--domain",
        "test",
    ]
    if pre_push:
        args.append("--pre-push")

    rpm_to_sync = RpmUnit(
        arch="src",
        filename="zebra-0.1-2.noarch.rpm",
        md5sum="0d56f302617696d3511e71e1669e62c0",
        name="zebra",
        version="0.1",
        release="2",
        sha256sum="7aa66335d8ebc295d626abc0639135ff6dec6333d4e94e0da69ed720c5fdd5f0",
        unit_id="to-sync-rpm-id1",
    )
    existing_rpm = RpmUnit(
        arch="src",
        filename="fake-0.1-2.noarch.rpm",
        md5sum="e7cb3d31a7a16c19e9f2ff3a0f183689",
        name="fake",
        version="0.1",
        release="2",
        sha256sum="cc1cd9f37d87e49e2b7d5e33b19df8a93a557b275c338c69ef8b03809c5b3314",
        unit_id="existing-rpm-id1",
        cdn_path="/content/origin/rpms/fake/0.1/2/fd431d51/fake-0.1-2.noarch.rpm",
        cdn_published=datetime.datetime(2021, 12, 10, 9, 59),
        repository_memberships=["dest1"],
    )

    repo = client.get_repository("dest1").result()
    fake_controller.insert_units(repo, [existing_rpm])

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

    # Do extra check on synced RPM - repository_memberships and cdn_path should be updated.
    synced_rpm = list(
        client.search_content(Criteria.with_field("unit_id", rpm_to_sync.unit_id))
    )
    assert len(synced_rpm) == 1
    synced_rpm = synced_rpm[0]
    if pre_push:
        # if doing pre-push, rpms are synced into tmp repo only
        assert synced_rpm == attr.evolve(
            rpm_to_sync,
            repository_memberships=["tmp-konflux-fake-uuid"],)
    else:
         assert synced_rpm == attr.evolve(
            rpm_to_sync,
            repository_memberships=["tmp-konflux-fake-uuid", "dest1"],
            cdn_path="/content/origin/rpms/zebra/0.1/2/fd431d51/zebra-0.1-2.noarch.rpm",
            cdn_published=datetime.datetime(2021, 12, 10, 9, 59),
        )


    # Do extra check on existing RPM - no change.
    present_rpm = list(
        client.search_content(Criteria.with_field("unit_id", existing_rpm.unit_id))
    )
    assert len(present_rpm) == 1
    present_rpm = present_rpm[0]

    assert present_rpm == existing_rpm

    # Do extra check on Erratum - only in dest and all-erratum-content-2020 repo.
    uploaded_erratum = list(client.search_content(Criteria.with_unit_type(ErratumUnit)))
    if pre_push:
        # erratum is not uploaded to pulp in pre-push mode
        assert len(uploaded_erratum) == 0
    else:
        assert len(uploaded_erratum) == 1
        uploaded_erratum = uploaded_erratum[0]

        assert uploaded_erratum.id == "RHSA-2020:0509"
        assert uploaded_erratum.repository_memberships == [
            "all-erratum-content-2020",
            "dest1",
        ]
        assert uploaded_erratum.title == "Important: sudo security update"


def _mock_pulp3_queries_sync_phase(httpx_mock):
    # mock pulp3 queries, we don't have fake pulp3 client as of Sept 2026,
    # but we should rather mock real queries to pulp3 as much as possible.

    # pushsource query to pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/content/rpm/packages/"
        ),
        method="GET",
        json={
            "results": [
                {
                    "pulp_href": "fake/href",
                    "sha256": "7aa66335d8ebc295d626abc0639135ff6dec6333d4e94e0da69ed720c5fdd5f0",
                    "name": "zebra-1.0-1.noarch.rpm",
                },
                {
                    "pulp_href": "fake/href",
                    "sha256": "cc1cd9f37d87e49e2b7d5e33b19df8a93a557b275c338c69ef8b03809c5b3314",
                    "name": "fake-1.0-1.noarch.rpm",
                }
            ]
        },
        status_code=200,
    )
    # mock create repository in pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/repositories/rpm/rpm/"
        ),
        method="POST",
        json={"pulp_href": "fake/href"},
        status_code=201,
    )
    # mock create distribution in pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/distributions/rpm/rpm/"
        ),
        method="POST",
        json={"task": "fake-task"},
        status_code=201,
    )
    # mock get distribution in pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/distributions/rpm/rpm/"
        ),
        method="GET",
        json={"count": 1, "results": [{"base_url": "fake/url"}]},
        status_code=200,
    )
    # mock modify content in pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/repositories/rpm/rpm/fake/href/modify/"
        ),
        method="POST",
        json={"task": "fake-task"},
        status_code=201,
    )
    # mock create publication in pulp3
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/publications/rpm/rpm/"
        ),
        method="POST",
        json={"task": "fake-task"},
        status_code=201,
    )
    # mock get task in pulp3 (reusable)
    httpx_mock.add_response(
        url=re.compile(
            "https://pulp3.example.com/api/pulp/test/api/v3/tasks/fake-task/"
        ),
        method="GET",
        json={"state": "completed"},
        status_code=200,
        is_reusable=True,
    )
