from pubtools.pulplib import CopyOptions

from .base import Phase, BATCH_SIZE
from ..items import PulpPushItem, PulpRpmPushItem


class Associate(Phase):
    """Association phase.

    Input queue:
    - items which exist in Pulp, but not necessarily in all the desired target repos.

    Output queue:
    - items which exist in Pulp in all the desired target repos.

    Side-effects:
    - executes Pulp association (copy) tasks.
    """

    def __init__(self, context, pulp_client, pre_push, allow_unsigned, in_queue, **_):
        super(Associate, self).__init__(
            context, in_queue=in_queue, name="Associate items in Pulp"
        )
        self.pulp_client = pulp_client
        self.pre_push = pre_push
        self.copy_options = CopyOptions(require_signed_rpms=not allow_unsigned)

    def iter_for_associate(self):
        """A special batched iterator for this phase which reorders all RPMs
        to the end of the queue.

        Why: it is strongly encouraged to ensure all modulemds are put into repos
        before we start putting RPMs into them, to reduce the risk that we could
        accidentally expose an RPM without the corresponding modulemd as that
        can break systems consuming the repos. Therefore we need to ensure any
        module items are processed before we can proceed to associate any RPM
        items.

        Note this potentially could be made smarter by, for example, tracking
        the state of modulemds for each repo, or even fully tracking the
        dependencies between each modulemd and the RPMs referenced by it, but
        it's unclear whether the additional complexity would be worth it.
        """

        yield_later = []

        for batch in self.iter_input_batched():
            yield_now = []

            for item in batch:
                if isinstance(item, PulpRpmPushItem):
                    # RPMs are held until later
                    yield_later.append(item)
                else:
                    yield_now.append(item)

            if yield_now:
                yield yield_now

        # OK, everything other than RPMs have been seen already.
        # By this point we know that modulemds are all in the right repos (noting
        # that modulemds are fully handled during upload phase, so by the time
        # we see a modulemd item in this phase, it's all done).
        #
        # That means it's safe to go ahead and yield RPMs, since any corresponding
        # modulemds must be in place.
        while yield_later:
            batch = yield_later[:BATCH_SIZE]
            yield_later = yield_later[BATCH_SIZE:]
            yield batch

    def run(self):
        for batch in self.iter_for_associate():
            for items in PulpPushItem.items_by_type(batch):
                for associated_f in PulpPushItem.associated_items_single_batch(
                    self.pulp_client, items, self.copy_options
                ):
                    self.put_future_outputs(associated_f)
