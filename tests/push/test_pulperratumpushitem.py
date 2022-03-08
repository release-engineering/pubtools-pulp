from pubtools.pulplib import ErratumUnit
from pushsource import ErratumPushItem

from pubtools._pulp.tasks.push.items import PulpErratumPushItem, State


def test_erratum_publishes_all_repos():
    item = PulpErratumPushItem(
        # We're being asked to push an advisory to a few repos...
        pushsource_item=ErratumPushItem(
            name="RHSA-1234:56", dest=["new1", "new2", "existing1"]
        ),
        pulp_state=State.PARTIAL,
        pulp_unit=ErratumUnit(
            id="abc123",
            # ...and the advisory already exists in some Pulp, repos, maybe with
            # some overlap
            repository_memberships=[
                "all-rpm-content",
                "all-rpm-content-ff",
                "existing1",
                "existing2",
            ],
        ),
    )

    # Then when we calculate the repos which should be published for this item,
    # it should always include both the new repo(s) we're pushing to and also the
    # existing repos, as any mutation of the erratum requires metadata to be
    # republished for all of them.
    # all-rpm-content is an exception given that those repos don't get published.
    assert item.publish_pulp_repos == ["existing1", "existing2", "new1", "new2"]
