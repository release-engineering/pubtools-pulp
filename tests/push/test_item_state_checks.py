import re
import attr

from pubtools.pulplib import FakeController, FileRepository, FileUnit
from pushsource import FilePushItem

from pubtools._pulp.tasks.push.items import PulpFilePushItem, State


class NeverInReposItem(PulpFilePushItem):
    """A push item which somehow never arrives in any repos."""

    @property
    def in_pulp_repos(self):
        return []


class NeverUpToDateItem(PulpFilePushItem):
    """A push item which somehow always needs an update."""

    @property
    def unit_for_update(self):
        # By always flipping the current unit's description, we ensure
        # that the item can never be considered up-to-date.
        return attr.evolve(
            self.pulp_unit, description="".join(reversed(self.pulp_unit.description))
        )


def test_update_checks_state():
    """Update fails if update apparently succeeded in pulp client, yet the item
    doesn't match the desired state."""

    pulp_unit = FileUnit(
        unit_id="some-file-unit",
        path="some/file.txt",
        size=5,
        sha256sum="49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063",
        description="a test file",
        repository_memberships=["some-repo"],
    )

    pulp_ctrl = FakeController()
    repo = FileRepository(id="some-repo")
    pulp_ctrl.insert_repository(repo)
    pulp_ctrl.insert_units(repo, [pulp_unit])

    item = NeverUpToDateItem(
        pushsource_item=FilePushItem(
            name="some/file.txt",
            sha256sum="49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063",
            dest=["some-repo"],
        ),
        pulp_unit=pulp_unit,
        pulp_state=State.NEEDS_UPDATE,
    )

    # Try updating it.
    update_f = item.ensure_uptodate(pulp_ctrl.client)

    # The update attempt should fail.
    exc = update_f.exception()

    # It should tell us why.
    assert (
        "item supposedly updated successfully, but actual and desired state still differ:"
        in str(exc)
    )

    # It should tell us the item we failed to process.
    assert "item:         FilePushItem(name='some/file.txt'" in str(exc)

    # It should show the current and desired field values:

    # The 'current unit', i.e. the state after we updated, reversed the original
    # description.
    assert re.search(r"current unit: FileUnit.*elif tset a", str(exc))

    # The 'desired unit', i.e. the reason we still don't consider the unit up-to-date,
    # wants to reverse the description back again...
    assert re.search(r"desired unit: FileUnit.*a test file", str(exc))


def test_upload_checks_repos(tmpdir):
    """Upload fails if upload apparently succeeded in pulp client, yet the item
    still is missing from all Pulp repos."""

    testfile = tmpdir.join("myfile")
    testfile.write("hello")

    pulp_ctrl = FakeController()
    repo = FileRepository(id="some-repo")
    pulp_ctrl.insert_repository(repo)

    item = NeverInReposItem(
        pushsource_item=FilePushItem(name="test", src=str(testfile), dest=["some-repo"])
    )
    item = item.with_checksums()

    ctx = item.upload_context(pulp_ctrl.client)
    upload_f = item.ensure_uploaded(ctx)

    # The upload attempt should fail.
    exc = upload_f.exception()

    # It should tell us why & which item.
    assert (
        "item supposedly uploaded successfully, but remains missing from Pulp:"
        in str(exc)
    )
    assert "FilePushItem(name='test'" in str(exc)
