import pytest

from pushsource import RpmPushItem

from pubtools._pulp.tasks.push.items import PulpRpmPushItem


def test_rpm_bad_filename():
    """rpm_nvr raises a meaningful error when NVR can't be parsed."""

    # This RPM's filename doesn't match the NVR.A.rpm convention
    # per http://ftp.rpm.org/max-rpm/ch-rpm-file-format.html
    item = PulpRpmPushItem(pushsource_item=RpmPushItem(name="my-badlynamed.rpm"))

    # We should not be able to retrieve the NVR.
    with pytest.raises(ValueError) as excinfo:
        item.rpm_nvr

    # The exception should tell us why.
    assert (
        "Invalid RPM filename my-badlynamed.rpm "
        "(expected: [name]-[version]-[release].[arch].rpm" in str(excinfo.value)
    )
