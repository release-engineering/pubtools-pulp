from .base import Phase
from ..items import PulpPushItem


class QueryPulp(Phase):
    """Phase for querying Pulp.

    This phase takes items which have an unknown Pulp state and performs
    Pulp queries to enrich them.

    Input queue:
    - items with an unknown Pulp state.

    Output queue:
    - items with a known Pulp state.

    Side-effects:
    - none.
    """

    def __init__(self, context, pulp_client, in_queue, **_):
        super(QueryPulp, self).__init__(
            context, in_queue=in_queue, name="Query items in Pulp"
        )
        self.pulp_client = pulp_client

    def run(self):
        for batch in self.iter_input_batched():
            for items in PulpPushItem.items_by_type(batch):
                updated_items_f = PulpPushItem.items_with_pulp_state_single_batch(
                    self.pulp_client, items
                )
                self.put_future_outputs(updated_items_f)
