from datetime import datetime
from pushsource import RpmPushItem
from pubtools.pulplib import RpmUnit, RpmDependency

from pubtools._pulp.tasks.push.items import PulpRpmPushItem


def test_rpm_thin_unit():
    """RPM push items discard fields unnecessary for push."""

    # Make an item
    item = PulpRpmPushItem(
        pushsource_item=RpmPushItem(name="bash-5.0.7-1.fc30.x86_64.rpm")
    )

    # Start with a 'full' unit as returned from pulp, with various
    # unused fields available
    unit_in = RpmUnit(
        name="bash",
        version="5.0.7",
        release="1.fc30",
        arch="x86_64",
        epoch="9001",
        signing_key="a1b2c3d4",
        filename="bash-5.0.7-1.fc30.x86_64.rpm",
        sourcerpm="bash-5.0.7-1.fc30.src.rpm",
        md5sum="d3b07a382ec010c01889250fce66fb13",
        sha1sum="f3d9ae4aeea6946a8668445395ba10b7399523a0",
        sha256sum="49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063",
        cdn_path="/origin/some/path.rpm",
        cdn_published=datetime(2022, 4, 29, 14, 48),
        repository_memberships=["a", "b", "c"],
        unit_id="best-unit",
        requires=[RpmDependency(name="foo", flags="EQ", version="1.0.0")],
    )

    # Get the item with unit attached
    item = item.with_unit(unit_in)

    # Now have a look at what the unit has become:
    # It's a cut down version of the above with many fields thrown
    # away if they are not required for push.
    assert item.pulp_unit == RpmUnit(
        name="bash",
        version="5.0.7",
        release="1.fc30",
        arch="x86_64",
        sha256sum="49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063",
        cdn_path="/origin/some/path.rpm",
        cdn_published=datetime(2022, 4, 29, 14, 48),
        repository_memberships=["a", "b", "c"],
        unit_id="best-unit",
    )
