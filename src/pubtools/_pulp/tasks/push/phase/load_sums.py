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

    # Outputs of this phase should update push items since this is the first
    # point during a push where we know what the items actually are (in terms
    # of the bytes being pushed...)
    UPDATES_PUSH_ITEMS = True

    def __init__(self, context, in_queue, **kwargs):
        super(LoadChecksums, self).__init__(
            context, in_queue=in_queue, name="Calculate checksums", **kwargs
        )

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

                LOG.debug(
                    "Calculating checksums (blocking: %s): %s",
                    item.blocking_checksums,
                    item.pushsource_item.name,
                )

                if not item.blocking_checksums:
                    # with_checksums (probably) won't block so just do
                    # it immediately, thus letting the next phase get hold
                    # of the item more quickly.
                    self.put_output(item.with_checksums())

                else:
                    # with_checksums (probably) will block so put it onto
                    # the thread pool's queue.
                    f = exc.submit(item.with_checksums)
                    self.put_future_output(f)
