import logging

from .base import Phase
from ..items import State


LOG = logging.getLogger("pubtools.pulp")


class Update(Phase):
    """Update phase.

    This phase ensures any per-item mutable fields in Pulp are set to their
    desired values.

    Input queue:
    - items which exist in Pulp but might have some mutable fields which do
      not match desired values.

    Output queue:
    - items which exist in Pulp and have all mutable fields matching their
      desired values.

    Side-effects:
    - mutates unit fields (under pulp_user_metadata) in Pulp.
    """

    def __init__(self, context, pulp_client, in_queue, **_):
        super(Update, self).__init__(
            context, in_queue=in_queue, name="Update items in Pulp"
        )
        self.pulp_client = pulp_client

    def run(self):
        no_update_needed = 0
        update_needed = 0

        for item in self.iter_input():
            if item.pulp_state not in State.NEEDS_UPDATE:
                # This item is already up-to-date in Pulp (or just doesn't support
                # being updated)
                no_update_needed += 1
                self.put_output(item)
            else:
                # This item needs an update.
                update_needed += 1
                self.put_future_output(item.ensure_uptodate(self.pulp_client))

        LOG.info(
            "Update: %s item(s) already up-to-date, %s updating",
            no_update_needed,
            update_needed,
        )
