import pytest

from pushsource import RpmPushItem

from pubtools._pulp.tasks.push.items import (
    PulpPushItem,
    PulpRpmPushItem,
    PulpFilePushItem,
    PulpProductIdPushItem,
)


def test_needs_sync_without_opener():
    """An item whose content has no local opener needs to be synced.

    This models a Konflux push item, whose 'src' is a Pulp href rather than a
    local file. It's the item (i.e. the source of its content) which decides
    this, not any external configuration.
    """
    item = PulpRpmPushItem(
        pushsource_item=RpmPushItem(
            name="konflux.rpm", src="some/pulp/href", dest=["dest1"], opener=None
        )
    )
    assert item.needs_sync is True


def test_needs_sync_with_opener():
    """An item with local content (an opener) is uploaded, not synced."""
    item = PulpRpmPushItem(
        pushsource_item=RpmPushItem(
            name="local.rpm",
            src="/some/local.rpm",
            dest=["dest1"],
            opener=lambda _: None,
        )
    )
    assert item.needs_sync is False


def test_match_items_units_empty():
    """match_items_units returns empty list if input is empty."""
    result = PulpPushItem.match_items_units(
        items=[],
        # We don't expect it to even look at units if items is empty.
        # Throw some garbage in there to demonstrate this.
        units=[object(), object()],
    )

    assert result == []


def test_match_items_units_mismatch():
    """match_items_units raises if mixing multiple item types."""

    with pytest.raises(TypeError):
        PulpPushItem.match_items_units(
            items=[
                PulpFilePushItem(pushsource_item=None),
                PulpProductIdPushItem(pushsource_item=None),
            ],
            units=[object(), object()],
        )


def test_match_items_units_abstract():
    """match_items_units raises if invoked on base class."""

    with pytest.raises(NotImplementedError):
        PulpPushItem.match_items_units(
            items=[
                PulpPushItem(pushsource_item=None),
            ],
            units=[object(), object()],
        )


def test_empty_pulp_repos():
    """in_pulp_repos on an item with no pulp_unit is an empty list"""

    item = PulpPushItem(pushsource_item=None)
    assert item.in_pulp_repos == []


def test_abstract_methods():
    """Various properties/methods on base class are not implemented or have trivial defaults."""
    item = PulpPushItem(pushsource_item=None)

    with pytest.raises(NotImplementedError):
        item.unit_type

    with pytest.raises(NotImplementedError):
        item.upload_to_repo(object())

    assert item.criteria() is None
    assert not item.supports_signing
    assert not item.is_signed
