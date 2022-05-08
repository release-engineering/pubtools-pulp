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

    def __init__(
        self, context, update_push_items, pulp_client_factory, pre_push, in_queue, **_
    ):
        super(Upload, self).__init__(
            context, in_queue=in_queue, name="Upload items to Pulp"
        )
        self.update_push_items = update_push_items
        self.pulp_client_factory = pulp_client_factory
        self.pre_push = pre_push

    def _update_after_uploaded(self, item_f):
        # Callback invoked after an item has been uploaded, to ensure
        # update_push_items is called (if upload succeeded).
        if not item_f.exception():
            self.update_push_items([item_f.result()])

    def run(self):
        """Yields push items with item uploaded if needed, such that the item will
        be present in at least one Pulp repo.
        """

        with self.pulp_client_factory() as client:
            return self.run_with_client(client)

    def run_with_client(self, client):
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
                    upload_context[item_type] = item_type.upload_context(client)

                ctx = upload_context[item_type]

                uploading += 1
                uploaded_f = item.ensure_uploaded(ctx)
                uploaded_f.add_done_callback(self._update_after_uploaded)
                self.put_future_output(uploaded_f)

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
