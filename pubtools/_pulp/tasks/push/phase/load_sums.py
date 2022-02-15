import logging

from .base import Phase


LOG = logging.getLogger("pubtools.pulp")


class LoadChecksums(Phase):
    """Phase for loading/calculating checksums of push items.

    Push items may be initially discovered either with checksum information
    (e.g. from Errata Tool) or without (e.g. files in a staging directory).

    In cases where a push item checksum is currently unknown, this phase
    will read the push item's file in order to calculate it. This is handled
    by a dedicated phase since it can be very slow for large content accessed
    via NFS.

    Input queue:
    - items which may or may not have checksum info.

    Output queue:
    - items which have checksums (where the push item type supports that).

    Side-effects:
    - sends updated item states to the Collect phase as soon as checksums
      are known.
    """

    def __init__(self, context, update_push_items, in_queue, **_):
        super(LoadChecksums, self).__init__(
            context, in_queue=in_queue, name="Calculate checksums"
        )
        self.update_push_items = update_push_items

    def run(self):
        for item in self.iter_input():
            # TODO: parallelize this
            with_sums = item.with_checksums()

            # As we figure out checksums for each item we'll record that item,
            # generally in PENDING state.
            self.update_push_items([with_sums])

            self.put_output(with_sums)
