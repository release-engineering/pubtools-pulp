import sys

from pubtools.pluggy import hookspec, pm


@hookspec
def task_pulp_flush():
    """Invoked during task execution after successful completion of all Pulp
    publishes.

    This hook is invoked a maximum of once per task, to indicate that all Pulp
    content associated with the task is considered fully up-to-date. The
    intended usage is to flush Pulp-derived caches or to notify systems that
    Pulp content may have recently changed."""


@hookspec
def pulp_item_push_finished(pulp_units, push_item):  # pylint: disable=unused-argument

    """Invoked during push tasks after each item has been processed fully.

    By the time this hook is invoked, the referenced item and unit is expected
    to be fully uploaded into Pulp and published onto the CDN.

    Args:
        pulp_units (list[:class:`~pubtools.pulplib.Unit`])
            A list of zero or more Pulp unit(s) created/updated for this item.
            Note that this information may not be available for every content type,
            and may only contain a subset of the Pulp fields.
        push_item (:class:`~pushsource.PushItem`)
            The item which has been pushed.
    """


pm.add_hookspecs(sys.modules[__name__])
