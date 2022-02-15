import logging

from .base import Phase
from ..items import State


LOG = logging.getLogger("pubtools.pulp")


class EndPrePush(Phase):
    """Pre-push termination phase.

    This should be invoked only during pre-push tasks, as the very
    last phase.

    Input queue:
    - items which have been fully pre-pushed.

    Output queue:
    - none.

    Side-effects:
    - ensures the input queue is fully drained, thus ensuring push has
      completed.
    - sends all items to and then shuts down the Collect phase.
    """

    def __init__(self, context, in_queue, update_push_items, **_):
        super(EndPrePush, self).__init__(
            context, in_queue=in_queue, out_queue=False, name="End pre-push"
        )
        self.update_push_items = update_push_items

    def run(self):
        count_prepush = 0
        count_other = 0

        for item in self.iter_input():
            if item.pulp_state in (State.NEEDS_UPDATE, State.PARTIAL, State.IN_REPOS):
                # These are the states which mean that the item's content is in Pulp,
                # which is the extent of what prepush can do.
                count_prepush += 1
            else:
                count_other += 1
            # Notify of final push item state.
            self.update_push_items([item])

        # Notify that there are no more push item updates coming.
        self.update_push_items([self.FINISHED])

        LOG.info(
            "Ending pre-push. Items in pulp: %s, pending: %s",
            count_prepush,
            count_other,
        )
