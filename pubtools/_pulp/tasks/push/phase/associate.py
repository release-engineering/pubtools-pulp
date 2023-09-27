import logging
from collections import defaultdict
from pubtools.pulplib import CopyOptions

from .base import Phase
from ..items import PulpPushItem, PulpRpmPushItem, PulpModuleMdPushItem
from . import constants


LOG = logging.getLogger("pubtools.pulp")


class Associate(Phase):
    """Association phase.

    Input queue:
    - items which exist in Pulp, but not necessarily in all the desired target repos.

    Output queue:
    - items which exist in Pulp in all the desired target repos.

    Side-effects:
    - executes Pulp association (copy) tasks.
    """

    # This phase needs to buffer up items for a long while before the first associate,
    # so we'll avoid marking it as started until then
    STARTUP_TYPE = constants.STARTUP_TYPE_NOTIFY

    def __init__(self, context, pulp_client, pre_push, allow_unsigned, in_queue, **_):
        super(Associate, self).__init__(
            context, in_queue=in_queue, name="Associate items in Pulp"
        )
        self.pulp_client = pulp_client
        self.pre_push = pre_push
        self.copy_options = CopyOptions(require_signed_rpms=not allow_unsigned)

        # Used later for scheduling of rpm vs modulemd items.
        self.modulemd_yielded_per_dest = defaultdict(int)

    def delay_item(self, item):
        """Returns True if handling of the given item should be delayed until later.

        Used to ensure RPMs are associated into repos later than modulemds.
        """
        if not isinstance(item, PulpRpmPushItem):
            # no reason to delay anything else
            return False

        # If it's an RPM, it might still be possible to handle it immediately.
        # It depends whether there are any modulemds going to the same target
        # repos.

        item_info = self.context.item_info
        if not item_info.items_known.is_set():
            # We don't know all the items yet => we have no choice but to do
            # the safe thing and assume we might see more modulemds, so delay
            # until later.
            return True

        # We know how many modulemds are going to each repo, so we can check
        # to see if there is an intersection.
        for dest in item.pushsource_item.dest:
            if (
                self.modulemd_yielded_per_dest[dest]
                < item_info.modulemd_count_per_dest[dest]
            ):
                # There's still some modules to be processed for this repo,
                # so we can't safely handle the RPM yet.
                return True

        # No modulemds remaining for any repo, so we're good to go.
        return False

    def record_yielded(self, items):
        for item in items:
            if isinstance(item, PulpModuleMdPushItem):
                for dest in item.pushsource_item.dest:
                    self.modulemd_yielded_per_dest[dest] += 1

    def iter_for_associate(self):
        """A special batched iterator for this phase which ensures that RPMs
        cannot be processed until after all modulemds in the same repo.

        Why: it is strongly encouraged to ensure all modulemds are put into repos
        before we start putting RPMs into them, to reduce the risk that we could
        accidentally expose an RPM without the corresponding modulemd as that
        can break systems consuming the repos. Therefore we need to ensure any
        module items are processed before we can proceed to associate any RPM
        items.
        """

        yield_later = []

        for batch in self.iter_input_batched():
            yield_now = []

            for item in batch:
                if self.delay_item(item):
                    yield_later.append(item)
                else:
                    yield_now.append(item)

            LOG.debug("associate: %s now, %s later", len(yield_now), len(yield_later))

            if yield_now:
                self.notify_started()
                yield yield_now
                self.record_yielded(yield_now)

        # OK, everything other than RPMs have been seen already.
        # By this point we know that modulemds are all in the right repos (noting
        # that modulemds are fully handled during upload phase, so by the time
        # we see a modulemd item in this phase, it's all done).
        #
        # That means it's safe to go ahead and yield RPMs, since any corresponding
        # modulemds must be in place.
        while yield_later:
            batch = yield_later[: self.default_batch_size]
            yield_later = yield_later[self.default_batch_size :]
            self.notify_started()
            yield batch

        # If there's no items at all we should still notify
        self.notify_started()

    def run(self):
        for batch in self.iter_for_associate():
            for items in PulpPushItem.items_by_type(batch):
                for associated_f in PulpPushItem.associated_items_single_batch(
                    self.pulp_client, items, self.copy_options
                ):
                    self.put_future_outputs(associated_f)
