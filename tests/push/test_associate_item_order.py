from pushsource import RpmPushItem, ModuleMdPushItem
from pubtools._pulp.tasks.push.items import (
    PulpFilePushItem,
    PulpModuleMdPushItem,
    PulpRpmPushItem,
)
from pubtools._pulp.tasks.push.phase import Context, Associate, Phase, constants


def test_associate_order():
    """Associate phase reorders items so that RPMs are processed
    after modulemds per repo.
    """

    ctx = Context()
    queue = ctx.new_queue(maxsize=10000)
    phase = Associate(
        context=ctx,
        pulp_client=None,
        pre_push=None,
        allow_unsigned=True,
        in_queue=queue,
    )

    # Arrange for various items coming into the associate phase noting that
    # RPMs are not last.
    # Note: items don't have to actually be valid for this test, so we're just
    # creating a lot of empty objects.
    # Also, we use awkward numbers so it doesn't line up exactly with
    # BATCH_SIZE.
    rpms_nomod = [
        PulpRpmPushItem(
            pushsource_item=RpmPushItem(name="rpm", dest=["nomod-dest1", "nomod-dest2"])
        )
        for _ in range(0, 1003)
    ]
    rpms_mod = [
        PulpRpmPushItem(
            pushsource_item=RpmPushItem(
                name="rpm", dest=["nomod-dest1", "mod-dest1", "mod-dest2"]
            )
        )
        for _ in range(0, 237)
    ]
    modules = [
        PulpModuleMdPushItem(
            pushsource_item=ModuleMdPushItem(
                name="rpm", dest=["mod-dest1", "mod-dest2", "mod-dest3"]
            )
        )
        for _ in range(0, 2040)
    ]
    files = [PulpFilePushItem(pushsource_item=None) for _ in range(0, 300)]

    rpm_nomod_ids = [id(rpm) for rpm in rpms_nomod]
    rpm_mod_ids = [id(rpm) for rpm in rpms_mod]
    module_ids = [id(module) for module in modules]
    file_ids = [id(file) for file in files]

    # Put everything on the queue...
    all_items = rpms_nomod + rpms_mod + modules + files
    queue.put(all_items)

    # Put this so that iteration will end
    queue.put(constants.FINISHED)

    # Record accurate context info on all the items
    for item in all_items:
        ctx.item_info.add_item(item)
    ctx.item_info.items_known.set()

    # Now let's see how iteration over it will work out
    got_ids = []
    for batch in phase.iter_for_associate():
        got_ids.extend([id(item) for item in batch])

    # We should have got all the same items back out as we put in,
    # but the order is different: RPMs going to any repo which will also
    # receive a module have been shifted to the end
    # (and the order is otherwise the same as the input)
    assert got_ids == rpm_nomod_ids + module_ids + file_ids + rpm_mod_ids


def test_no_delay_if_all_modules_yielded():
    """Associate should not delay processing an RPM if it is known that
    all modules have already been yielded for the same repos.
    """
    rpm = PulpRpmPushItem(
        pushsource_item=RpmPushItem(name="rpm", dest=["dest1", "dest2"])
    )
    module = PulpModuleMdPushItem(
        pushsource_item=ModuleMdPushItem(name="module", dest=["dest1", "dest2"])
    )
    ctx = Context()
    phase = Associate(
        context=ctx,
        pulp_client=None,
        pre_push=None,
        allow_unsigned=True,
        in_queue=None,
    )

    item_info = ctx.item_info
    item_info.add_item(rpm)
    item_info.add_item(module)
    item_info.add_item(module)
    item_info.items_known.set()

    # If all items are known, and all modules have been yielded...
    phase.record_yielded([module, module])

    # Then there is no reason to delay processing the RPM
    assert not phase.delay_item(rpm)


def test_delay_if_items_unknown():
    """Associate should delay processing an RPM if there are still some items
    not yet discovered.
    """
    rpm = PulpRpmPushItem(
        pushsource_item=RpmPushItem(name="rpm", dest=["dest1", "dest2"])
    )
    module = PulpModuleMdPushItem(
        pushsource_item=ModuleMdPushItem(name="module", dest=["dest1", "dest2"])
    )
    ctx = Context()
    phase = Associate(
        context=ctx,
        pulp_client=None,
        pre_push=None,
        allow_unsigned=True,
        in_queue=None,
    )

    item_info = ctx.item_info
    item_info.add_item(rpm)
    item_info.add_item(module)
    item_info.add_item(module)

    # Not setting items_known here.

    # Even if all modules have been yielded...
    phase.record_yielded([module, module])

    # We still need to delay the RPM since we don't know all the items in the push.
    assert phase.delay_item(rpm)


def test_delay_if_pending_modules():
    """Associate should delay processing an RPM if there are still some modules
    to be yielded for target repos.
    """
    rpm1 = PulpRpmPushItem(pushsource_item=RpmPushItem(name="rpm", dest=["dest1"]))
    rpm2 = PulpRpmPushItem(pushsource_item=RpmPushItem(name="rpm", dest=["dest2"]))
    module1 = PulpModuleMdPushItem(
        pushsource_item=ModuleMdPushItem(name="module", dest=["dest1"])
    )
    module2 = PulpModuleMdPushItem(
        pushsource_item=ModuleMdPushItem(name="module", dest=["dest2"])
    )
    module3 = PulpModuleMdPushItem(
        pushsource_item=ModuleMdPushItem(name="module", dest=["dest1", "dest2"])
    )
    ctx = Context()
    phase = Associate(
        context=ctx,
        pulp_client=None,
        pre_push=None,
        allow_unsigned=True,
        in_queue=None,
    )

    item_info = ctx.item_info
    item_info.add_item(rpm1)
    item_info.add_item(rpm2)
    item_info.add_item(module1)
    item_info.add_item(module2)
    item_info.add_item(module3)
    item_info.items_known.set()

    # Simulate that all modules have been yielded for repo dest1,
    # but there's still one pending for dest2
    phase.record_yielded([module1, module3])

    # Then we should have to delay the rpm for dest2
    assert phase.delay_item(rpm2)

    # By contrast, the rpm for dest1 would be OK to handle immediately
    assert not phase.delay_item(rpm1)
