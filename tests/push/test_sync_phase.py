from pushsource import FilePushItem, RpmPushItem
from mock import MagicMock

from pubtools._pulp.tasks.push.phase import Context, Sync, constants
from pubtools._pulp.tasks.push.contextlib_compat import exitstack
from pubtools._pulp.tasks.push.items import PulpFilePushItem, PulpRpmPushItem, State


def _run_sync_over(items, pre_push):
    """Drive the Sync phase over the given items and return the items it output.

    The pulp3 client factory raises if invoked, asserting that nothing is synced
    (and no pulp3 connection is opened) when no item needs a sync.
    """
    ctx = Context()
    in_queue = ctx.new_queue()
    out_queue = ctx.new_queue()

    def no_client():
        raise AssertionError("pulp3 client must not be created when nothing is synced")

    sync_phase = Sync(
        context=ctx,
        in_queue=in_queue,
        out_queue=out_queue,
        pulp_client=None,
        pulp3_client_factory=no_client,
        pulp3_credentials=("basic", ("user", "password")),
        pre_push=pre_push,
        allow_unsigned=True,
    )

    in_queue.put(list(items))
    in_queue.put(constants.FINISHED)

    with exitstack([sync_phase]):
        # Exiting the context waits for the phase thread to complete.
        pass

    assert not ctx.has_error

    out = []
    while True:
        got = out_queue.get(block=True, timeout=constants.PHASE_TIMEOUT)
        if got is constants.FINISHED:
            break
        out.extend(got)
    return out


def test_sync_phase_passes_through_item_not_marked_for_sync():
    """An item not marked to_sync passes straight through to Upload, with no sync
    and no pulp3 client creation - the common case for a regular push."""
    item = PulpRpmPushItem(
        pushsource_item=RpmPushItem(name="walrus-5.21-1.noarch.rpm", dest=["dest1"])
    )
    # Not marked for sync, and in its default (unknown) Pulp state.
    assert item.needs_sync is False
    assert item.pulp_state is State.UNKNOWN

    assert _run_sync_over([item], pre_push=False) == [item]


def test_sync_phase_skips_prepush_unsupported_to_sync_item():
    """During pre-push, a to_sync item which can't pre-push is passed through,
    not synced."""
    item = PulpFilePushItem(
        pushsource_item=FilePushItem(name="thing.txt", dest=["dest1"], to_sync=True)
    )
    assert item.needs_sync is True
    assert item.can_pre_push is False

    assert _run_sync_over([item], pre_push=True) == [item]


def test_sync_phase_get_creds_basic():
    """
    Checks that the default sync arguments are returned correctly for basic authentication.
    """
    sync_phase = Sync(
        context=MagicMock(),
        pulp_client=None,
        pulp3_client_factory=lambda: None,
        pulp3_credentials=("basic", ("user", "password")),
        pre_push=None,
        allow_unsigned=True,
        in_queue=None,
        out_queue=None,
    )

    assert sync_phase._default_sync_args == {
        "require_signature": False,
        "skip": ["erratum", "distribution"],
        "basic_auth_username": "user",
        "basic_auth_password": "password",
    }


def test_sync_phase_get_creds_pki():
    """
    Checks that the default sync arguments are returned correctly for PKI authentication.
    """
    sync_phase = Sync(
        context=MagicMock(),
        pulp_client=None,
        pulp3_client_factory=lambda: None,
        pulp3_credentials=("pki", ("cert", "key")),
        pre_push=None,
        allow_unsigned=True,
        in_queue=None,
        out_queue=None,
    )

    assert sync_phase._default_sync_args == {
        "require_signature": False,
        "skip": ["erratum", "distribution"],
        "ssl_client_cert": "cert",
        "ssl_client_key": "key",
    }
