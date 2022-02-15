import logging
import concurrent

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
        self, context, update_push_items, pulp_client, pre_push, in_queue, **_
    ):
        super(Upload, self).__init__(
            context, in_queue=in_queue, name="Upload items to Pulp"
        )
        self.update_push_items = update_push_items
        self.pulp_client = pulp_client
        self.pre_push = pre_push

    def run(self):
        """Yields push items with item uploaded if needed, such that the item will
        be present in at least one Pulp repo.
        """

        uploaded = []
        needs_upload = []
        prepush_skipped = []

        upload_context = {}

        for item in self.iter_input():
            if item.pulp_state in [State.IN_REPOS, State.PARTIAL, State.NEEDS_UPDATE]:
                # This item is already in Pulp.
                uploaded.append(item)
            elif self.pre_push and not item.can_pre_push:
                # We're doing a pre-push, but this item doesn't support that.
                prepush_skipped.append(item)
            else:
                # This item is not in Pulp, or otherwise needs a reupload.
                item_type = type(item)
                if item_type not in upload_context:
                    upload_context[item_type] = item_type.upload_context(
                        self.pulp_client
                    )

                ctx = upload_context[item_type]

                needs_upload.append(item.ensure_uploaded(ctx))

        event = {
            "type": "uploading-pulp",
            "items-present": len(uploaded),
            "items-uploading": len(needs_upload),
        }
        messages = [
            "%s already present" % len(uploaded),
            "%s uploading" % len(needs_upload),
        ]

        if self.pre_push:
            messages.append("%s skipped during pre-push" % len(prepush_skipped))
            event["items-prepush-skipped"] = len(prepush_skipped)

        LOG.info("Upload items: %s", ", ".join(messages), extra={"event": event})

        # Anything already in the system or being skipped can be immediately yielded.
        for item in uploaded + prepush_skipped:
            assert item
            self.put_output(item)

        # Then wait for the completion of anything we're uploading.
        # TODO: apply a configurable timeout
        for item in concurrent.futures.as_completed(needs_upload):
            out = item.result()
            assert out
            self.update_push_items([out])
            self.put_output(out)
