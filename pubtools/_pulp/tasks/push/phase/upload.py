import logging

from .base import Phase
from ..items import State


LOG = logging.getLogger("pubtools.pulp")


class Upload(Phase):
    """Upload phase.

    This phase ensures files corresponding to each item are uploaded into Pulp,
    when applicable.

    Input queue:
    - items which may or may not exist in Pulp.

    Output queue:
    - (regular push): items which definitely exist in Pulp but not necessarily in
      all desired target repos; or...
    - (pre-push): items which may or may not exist in Pulp depending on whether the
      content type supports pre-push.

    Side-effects:
    - uploads content to Pulp, creating various units in repos.
    """

    # Outputs of this phase should update push items since an upload is
    # a significant event.
    UPDATES_PUSH_ITEMS = True

    def __init__(self, context, pulp_client, pre_push, in_queue, **kwargs):
        super(Upload, self).__init__(
            context, in_queue=in_queue, name="Upload items to Pulp", **kwargs
        )
        self.pulp_client = pulp_client
        self.pre_push = pre_push

    def run(self):
        """Yields push items with item uploaded if needed, such that the item will
        be present in at least one Pulp repo.
        """

        uploaded = 0
        uploading = 0
        prepush_skipped = 0

        upload_context = {}

        for item in self.iter_input():
            if item.pulp_state in [State.IN_REPOS, State.PARTIAL, State.NEEDS_UPDATE]:
                # This item is already in Pulp.
                uploaded += 1
                self.put_output(item)
            elif self.pre_push and not item.can_pre_push:
                # We're doing a pre-push, but this item doesn't support that.
                prepush_skipped += 1
                self.put_output(item)
            else:
                # This item is not in Pulp, or otherwise needs a reupload.
                item_type = type(item)
                if item_type not in upload_context:
                    upload_context[item_type] = item_type.upload_context(
                        self.pulp_client
                    )

                ctx = upload_context[item_type]

                uploading += 1
                self.put_future_output(item.ensure_uploaded(ctx))

        event = {
            "type": "uploading-pulp",
            "items-present": uploaded,
            "items-uploading": uploading,
        }
        messages = [
            "%s already present" % uploaded,
            "%s uploading" % uploading,
        ]

        if self.pre_push:
            messages.append("%s skipped during pre-push" % prepush_skipped)
            event["items-prepush-skipped"] = prepush_skipped

        LOG.info("Upload items: %s", ", ".join(messages), extra={"event": event})
