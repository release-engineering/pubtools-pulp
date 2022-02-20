from .base import Phase


class Collect(Phase):
    """Collection phase.

    Input queue:
    - items in any state.

    Output queue:
    - none.

    Side-effects:
    - uses pushcollector library to record the current state of a push item.
    """

    def __init__(self, context, collector, **_):
        super(Collect, self).__init__(
            context,
            in_queue=context.new_queue(counting=False),
            out_queue=False,
            name="Collect push item metadata",
        )
        self.collector = collector

    def update_push_items(self, items):
        for item in items:
            self.in_queue.put(item)

    def run(self):
        for item_batch in self.iter_input_batched():
            pushsource_items = [item.pushsource_item for item in item_batch]
            self.collector.update_push_items(pushsource_items).result()
