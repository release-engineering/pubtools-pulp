import pytest

from pushsource import RpmPushItem

from pubtools._pulp.tasks.push.items import (
    PulpPushItem,
    PulpRpmPushItem,
    PulpFilePushItem,
    PulpProductIdPushItem,
)


def test_needs_sync_true_when_to_sync_set():
    """An item which the source marked with to_sync needs to be synced.

    This models a Konflux push item, whose content resides in a remote Pulp
    (referenced by 'src') and must be synced in. It's the source of the item
    which decides this via the explicit to_sync flag, not any external
    configuration.
    """
    item = PulpRpmPushItem(
        pushsource_item=RpmPushItem(
            name="konflux.rpm",
            src="/api/pulp/test/api/v3/content/rpm/packages/abc-123/",
            dest=["dest1"],
            opener=None,
            to_sync=True,
        )
    )
    assert item.needs_sync is True


def test_needs_sync_false_when_to_sync_not_set():
    """An item not marked with to_sync is uploaded, not synced.

    The decision is driven solely by the to_sync flag: an item is *not* routed
    to Sync merely because it happens to lack an opener or carries a particular
    kind of 'src'. Only the source's explicit to_sync marking matters.
    """
    # A regular local item (has an opener).
    local = PulpRpmPushItem(
        pushsource_item=RpmPushItem(
            name="local.rpm",
            src="/some/local.rpm",
            dest=["dest1"],
            opener=lambda _: None,
        )
    )
    assert local.needs_sync is False

    # An item with no opener but not marked to_sync must still NOT be synced.
    no_opener = PulpRpmPushItem(
        pushsource_item=RpmPushItem(
            name="thing.rpm",
            src="/some/local.rpm",
            dest=["dest1"],
            opener=None,
        )
    )
    assert no_opener.needs_sync is False


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
