import logging

from .base import Phase
from ..items import State


LOG = logging.getLogger("pubtools.pulp")


class EndPush(Phase):
    """Explicit push termination phase.

    This is not required in the typical scenario where 'publish' phase
    awaits end of a push. In cases where the push ends early, such as
    pre-push or `--skip publish', this phase is used to await the
    output of all prior phases.

    Input queue:
    - items in any state.

    Output queue:
    - none.

    Side-effects:
    - ensures the input queue is fully drained, thus ensuring all prior
      phases have completed fully.
    - sends all items to and then shuts down the Collect phase.
    """

    def __init__(self, context, in_queue, update_push_items, **_):
        super(EndPush, self).__init__(
            context, in_queue=in_queue, out_queue=False, name="End push"
        )
        self.update_push_items = update_push_items

    def run(self):
        count_present = 0
        count_pending = 0

        for item in self.iter_input():
            # Count the items as either in pulp or not, to give a simple report.
            # The idea is that if we are ending early due to pre-push or skipping publish,
            # the count here will give some idea of how much work still needs to be done
            # for a later complete push.
            if item.pulp_state in (State.NEEDS_UPDATE, State.PARTIAL, State.IN_REPOS):
                # These are the states which mean that the item's content is in Pulp.
                count_present += 1
            else:
                count_pending += 1

            # Notify of final push item state.
            self.update_push_items([item])
            self.in_queue.task_done()

        # Notify that there are no more push item updates coming.
        self.update_push_items([self.FINISHED])

        LOG.info(
            "Ending push. Items in pulp: %s, pending: %s",
            count_present,
            count_pending,
        )
