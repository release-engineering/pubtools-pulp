import pytest

from pubtools._pulp.tasks.push.items import (
    PulpPushItem,
    PulpFilePushItem,
    PulpProductIdPushItem,
)


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
