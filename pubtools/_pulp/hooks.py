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
def pulp_item_finished(item_metadata=None):  # pylint: disable=unused-argument

    """Invoked when PulpPushItem is pushed to a pulp target

    This hook should be invoked per every pub push item after end of push
    process."""


pm.add_hookspecs(sys.modules[__name__])
