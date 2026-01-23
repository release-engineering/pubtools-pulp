import logging

from .base import Phase
from ..items import State
from . import constants

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
    - sends all items to the Collect phase.
    """

    PROGRESS_TYPE = constants.PROGRESS_TYPE_NONE

    def __init__(self, context, in_queue, **kwargs):
        super(EndPush, self).__init__(
            context, in_queue=in_queue, out_queue=False, name="End push", **kwargs
        )

    def run(self):
        count_present = 0
        count_pending = 0

        for item_batch in self.iter_input_batched():
            # Notify of final push item state.
            self.update_push_items(item_batch)

            for item in item_batch:
                # Count the items as either in pulp or not, to give a simple report.
                # The idea is that if we are ending early due to pre-push or skipping publish,
                # the count here will give some idea of how much work still needs to be done
                # for a later complete push.
                if item.pulp_state in (
                    State.NEEDS_UPDATE,
                    State.PARTIAL,
                    State.IN_REPOS,
                ):
                    # These are the states which mean that the item's content is in Pulp.
                    count_present += 1
                else:
                    count_pending += 1

        LOG.info(
            "Ending push. Items in pulp: %s, pending: %s",
            count_present,
            count_pending,
        )
