from .base import Phase
from .errors import PhaseInterrupted
from . import constants


class Collect(Phase):
    """Collection phase.

    Input queue:
    - items in any state.

    Output queue:
    - none.

    Side-effects:
    - uses pushcollector library to record the current state of a push item.
    """

    PROGRESS_TYPE = constants.PROGRESS_TYPE_NONE

    def __init__(self, context, collector, **_):
        super(Collect, self).__init__(
            context,
            in_queue=context.new_queue(),
            out_queue=False,
            name="Collect push item metadata",
        )
        self.collector = collector

    def item_key(self, item):
        # Given an item, return a key which should be comparable between items
        # in order to decide if a pair of items are considered duplicates.
        pushsource_item = item.pushsource_item
        return (pushsource_item.name, pushsource_item.dest, pushsource_item.src)

    def iter_for_collect(self):
        """A special batched iterator for this phase which filters out
        duplicate items."""
        for item_batch in self.iter_input_batched():
            # Depending how fast we're running, it's possible for the input
            # batch to have the "same" item more than once with different
            # states; for example, an RPM which was initially PENDING, then
            # updated to EXISTS after it was found to be present in Pulp.
            #
            # In that case we only want the latter item to be included here,
            # because:
            # - it's pointless to include both
            # - some backends (Pub's at least) do not expect to receive such
            #   duplicates and may crash when they do.
            #
            out_batch = []
            out_keys = set()
            for item in item_batch:
                key = self.item_key(item)
                if key not in out_keys:
                    out_keys.add(key)
                    out_batch.append(item)
                else:
                    idx = None
                    for idx, out_item in enumerate(out_batch):
                        if self.item_key(out_item) == key:
                            break

                    # Current item replaces the last one we saw with the same key.
                    assert idx is not None
                    out_batch[idx] = item

            yield out_batch

    def run(self):
        for item_batch in self.iter_for_collect():
            pushsource_items = [item.pushsource_item for item in item_batch]
            self.collector.update_push_items(pushsource_items).result()

    def __exit__(self, *args):
        # This phase is unusual in that it shuts down its own input queue during __exit__,
        # rather than expecting someone else to shut it down.
        # NOTE: in order for this to work properly and not shut down too early, it's critical
        # that __exit__ on this phase is called after all other phases.
        try:
            self.in_queue.put(constants.FINISHED)
        except PhaseInterrupted:
            # this is fine since it means the phase is already exiting,
            # which is what we want.
            pass

        return super(Collect, self).__exit__(*args)
