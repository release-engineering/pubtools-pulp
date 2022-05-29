import logging
import sys
import os


from .contextlib_compat import exitstack
from .phase import (
    LoadPushItems,
    LoadChecksums,
    QueryPulp,
    Upload,
    EndPush,
    Update,
    Collect,
    Associate,
    Publish,
    Context,
    ProgressLogger,
)
from ..common import Publisher, PulpTask
from ...services import (
    CollectorService,
    CachingPulpClientService,
)
from ...arguments import SplitAndExtend

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


# Because pylint misunderstands the type of e.g. pulp_client:
# E1101: Instance of 'CollectorProxy' has no 'search_repository' member (no-member)
#
# pylint: disable=no-member


class Push(
    CollectorService,
    CachingPulpClientService,
    Publisher,
    PulpTask,
):
    """Push and publish content via Pulp."""

    def add_args(self):
        super(Push, self).add_args()

        self.add_publisher_args(self.parser)

        self.parser.add_argument(
            "--skip",
            help="skip given comma-separated sub-steps",
            type=str,
            action=SplitAndExtend,
            split_on=",",
            default=[],
        )

        self.parser.add_argument(
            "--pre-push",
            action="store_true",
            help=(
                "Pre-push mode: do as much as possible without making content "
                "available to end-users, then stop. May be used to improve the "
                "performance of a subsequent full push."
            ),
        )

        self.parser.add_argument(
            "--allow-unsigned",
            action="store_true",
            help="Allow pushing unsigned RPMs (forbidden by default)",
        )

        self.parser.add_argument(
            "--source", action="append", help="Source(s) of content to be pushed"
        )

    def pulp_client_for_phase(self):
        """Returns a Pulp client for use within a single phase."""

        if self.args.pulp_fake:
            return self.pulp_fake_controller.new_client()

        # When using a real server we want to use a separate client per phase.
        # The reason is that clients under the hood have some fixed size thread pools
        # for certain things (e.g. number of requests in flight at once). If the same
        # client is used by multiple phases, this means one phase can block another
        # since both compete for the same resources.
        #
        # The worst-case scenario can trigger a deadlock: all of the client's request
        # threads can be blocked trying to put an item onto a phase's output queue,
        # while the next phase can't consume from its input queue because it's blocked
        # on a Pulp request using the same client.
        #
        # We will tune thread counts a bit because if we are creating more clients,
        # we should probably use fewer threads per client to avoid overwhelming the
        # system... there is not really a perfect way to do this, let's just cut
        # the count in half somewhat arbitrarily.
        threads = int(os.environ.get("PUBTOOLS_PULPLIB_REQUEST_THREADS") or "4")
        threads = max(1, int(threads / 2.0))
        return self.new_caching_pulp_client(pulp_client=None, threads=threads)

    def run(self):
        # Push workflow.
        #
        # Push is separated into various phases. Each phase has one thread
        # associated with it, and generally is connected to the next phase by
        # a queue.
        phases = []

        # This is a context object shared by all phases.
        ctx = Context()

        # Prepare pushcollector 'phase'. This phase is a bit special in that
        # it runs in parallel to all other phases, and its input queue is written
        # to by all other phases.
        collect_phase = Collect(context=ctx, collector=self.collector)
        phases.append(collect_phase)

        # A helper to add a phase with consistent initialization.
        def add_phase(klass, **kwargs):
            if "in_queue" not in kwargs and phases:
                # For all phases except the first, the input queue defaults
                # to the previous phase's output queue, i.e. each phase passes
                # some items onto the next via the queue.
                kwargs["in_queue"] = phases[-1].out_queue

            # Provide many default arguments to each phase.
            # Phases accept any number of keyword arguments, so these aren't
            # all used by every phase.
            kwargs.update(
                context=ctx,
                pulp_client_factory=self.pulp_client_for_phase,
                pre_push=self.args.pre_push,
                allow_unsigned=self.args.allow_unsigned,
                update_push_items=collect_phase.update_push_items,
                publish_with_cache_flush=self.publish_with_cache_flush,
            )
            phases.append(klass(**kwargs))

        # Now proceed with adding the phases which make up a push...

        # Load push items from pushsource library.
        # As the first phase, this does not have an input queue as it obtains
        # its inputs from pushsource library.
        add_phase(LoadPushItems, in_queue=None, source_urls=self.args.source)

        # Ensure we have checksums for each push item. Potentially involves
        # reading content for push over NFS.
        add_phase(LoadChecksums)

        # Figure out the current state of each item in Pulp.
        add_phase(QueryPulp)

        # Ensure all items are uploaded to Pulp. This uploads bytes into Pulp
        # but does not guarantee the items are present in each of the desired
        # destination repos.
        add_phase(Upload)

        if self.args.pre_push:
            # If we are in pre-push mode then we do not go any further, we just wait
            # for all previous steps, then log a message and exit.
            add_phase(EndPush)

        else:
            # Ensure all items are up-to-date in Pulp. This adjusts any mutable fields
            # whose current value doesn't match the desired value.
            add_phase(Update)

            # Ensure all items are associated into the desired target repos.
            add_phase(Associate)

            if "publish" in self.args.skip:
                # Caller doesn't want to publish, then we just wait for prior phases
                # to complete.
                add_phase(EndPush)

            else:
                # Ensure all repos are published once the desired content is present.
                add_phase(Publish)

        # We've connected up all phases of the push, now we just need to
        # start them all.
        #
        # This will start all the phases...
        with exitstack([ProgressLogger.for_context(ctx)] + phases):
            LOG.debug("All push phases are now running.")
            # ...and exiting the 'with' block here will wait for them to
            # complete.

        # If a phase failed, it's communicated back to us through the
        # context object here. Exit unsuccessfully if so.
        if ctx.has_error:
            LOG.error("Push failed with fatal error, see previous logs.")
            sys.exit(59)
