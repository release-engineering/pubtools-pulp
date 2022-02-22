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
        # TODO: this could be parallelized further. There's no reason we need to
        # wait for one batch to complete before starting the search for the next
        # one.
        for batch in self.iter_input_batched():
            for items in PulpPushItem.items_by_type(batch):
                for item in PulpPushItem.items_with_pulp_state_single_batch(
                    self.pulp_client, items
                ):
                    assert item
                    self.put_output(item)
