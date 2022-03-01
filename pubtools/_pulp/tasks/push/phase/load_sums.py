import os
import logging

from more_executors import Executors

from .base import Phase


LOG = logging.getLogger("pubtools.pulp")

CHECKSUM_THREADS = int(os.getenv("PUBTOOLS_PULP_CHECKSUM_THREADS") or "4")


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

    def _get_sums(self, item):
        with_sums = item.with_checksums()

        # As we figure out checksums for each item we'll record that item,
        # generally in PENDING state.
        self.update_push_items([with_sums])

        return with_sums

    def run(self):
        with Executors.thread_pool(
            max_workers=CHECKSUM_THREADS, name="checksummer"
        ) as exc:

            for item in self.iter_input():
                # Use a heuristic to try to hand off the item onto the next
                # phase as quickly as possible.
                #
                # - in general we use a thread pool to do calculations in parallel.
                #
                # - but if we probably already have sums, we don't want to put
                #   that item onto the back of a potentially long queue where it
                #   may have to wait a long time, when it could be passsed on
                #   immediately.
                #
                # Hence we handle some items synchronously and others not.

                if not item.blocking_checksums:
                    # with_checksums (probably) won't block so just do
                    # it immediately, thus letting the next phase get hold
                    # of the item more quickly.
                    self.put_output(self._get_sums(item))

                else:
                    # with_checksums (probably) will block so put it onto
                    # the thread pool's queue.
                    f = exc.submit(self._get_sums, item)
                    self.put_future_output(f)
