from .base import Phase, BATCH_SIZE
from ..items import PulpPushItem


class Associate(Phase):
    """Association phase.

    Input queue:
    - items which exist in Pulp, but not necessarily in all the desired target repos.

    Output queue:
    - items which exist in Pulp in all the desired target repos.

    Side-effects:
    - executes Pulp association (copy) tasks.
    """

    def __init__(self, context, pulp_client, pre_push, in_queue, **_):
        super(Associate, self).__init__(
            context, in_queue=in_queue, name="Associate items in Pulp"
        )
        self.pulp_client = pulp_client
        self.pre_push = pre_push

    def run(self):
        # Synchronization point prior to association.
        #
        # Why: it is strongly encouraged to ensure all modulemds are put into repos
        # before we start putting RPMs into them, to reduce the risk that we could
        # accidentally expose an RPM without the corresponding modulemd. Therefore we
        # need to ensure any module items are processed by uploaded_items above,
        # before we can proceed to associate any RPM items.
        #
        # TODO: try to make this smarter so it handles only that modulemd/rpm case
        # described above without slowing other stuff down?
        remaining = list(self.iter_input())

        while remaining:
            batch = remaining[:BATCH_SIZE]
            remaining = remaining[BATCH_SIZE:]

            for items in PulpPushItem.items_by_type(batch):
                for associated_f in PulpPushItem.associated_items_single_batch(
                    self.pulp_client, items
                ):
                    self.put_future_outputs(associated_f)
