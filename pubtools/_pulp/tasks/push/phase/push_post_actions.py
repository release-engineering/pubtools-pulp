import logging
import attrs

from .base import Phase
from . import constants

from ....hooks import pm


LOG = logging.getLogger("pubtools.pulp")


class PostPushActions(Phase):
    """Phase to do any neccesary PulpPushItem related operations

    Currently it only triggers pulp_item_finished hook

    Input queue:
    - PulpPushItem items.

    Output queue:
    - none.
    """

    PROGRESS_TYPE = constants.PROGRESS_TYPE_NONE

    def __init__(self, context, in_queue, **kwargs):
        super(PostPushActions, self).__init__(
            context, in_queue=in_queue, name="Post Push Actions", **kwargs
        )

    def run(self):
        for item_batch in self.iter_input_batched():
            for item in item_batch:
                pm.hook.pulp_item_finished(item_metadata=attrs.asdict(item.pulp_unit) if item.pulp_unit else None, push_item=item.pushsource_item) # pylint: disable=no-member
                self.put_output(item)
