import logging

import attr
from pubtools.pulplib import Criteria

from .base import Phase
from . import constants


LOG = logging.getLogger("pubtools.pulp")


class Publish(Phase):
    """Publish phase.

    The publish phase should be the last phase of a (non-pre-push) push task.
    It takes care of all the final steps to make bits of content available to
    end-users from the CDN.

    Input queue:
    - items which exist in Pulp in all the desired target repos.

    Output queue:
    - Updated pulp push items

    Side-effects:
    - sets cdn_published field on all relevant Pulp units.
    - publishes all relevant Pulp repos.
    - purges CDN cache.
    - purges UD cache.
    - sends all items to Collect phase, with state="PUSHED".
    """

    # publish phase waits a long time before doing anything, so delay
    # startup notification until we're ready
    STARTUP_TYPE = constants.STARTUP_TYPE_NOTIFY

    def __init__(
        self, context, pulp_client, publish_with_cache_flush, in_queue, **kwargs
    ):
        super(Publish, self).__init__(
            context, in_queue=in_queue, name="Publish and cache flush", **kwargs
        )
        self.pulp_client = pulp_client
        self.publish_with_cache_flush = publish_with_cache_flush

    def run(self):
        # At the time we run, it is the case that all items exist with the desired
        # state, in the desired repos. Now we need to publish affected repos.
        #
        # This is also a synchronization point. The idea is that publishing repo
        # makes content available, and there may be dependencies between the bits
        # of content we've handled, so we should ensure *all* of them are in correct
        # repos before we start publish of *any* repos to increase the chance that
        # all of them land at once.
        #
        # TODO: once exodus is live, consider refactoring this to not be a
        # synchronization point (or make it optional?) as the above motivation goes
        # away - the CDN origin supports near-atomic update.
        all_repo_ids = set()
        set_cdn_published = set()
        all_items = []

        for item in self.iter_input():
            all_repo_ids.update(item.publish_pulp_repos)

            # any unit which supports cdn_published but hasn't had it set yet should
            # have it set once the publish completes.
            unit = item.pulp_unit
            if hasattr(unit, "cdn_published") and unit.cdn_published is None:
                set_cdn_published.add(unit)

            all_items.append(item)

        # From a user's point of view, this is the point at which we are
        # starting publishes.
        self.notify_started()

        # Locate all the repos for publish.
        repo_fs = self.pulp_client.search_repository(
            Criteria.with_id(sorted(all_repo_ids))
        )

        # Start publishing them, including cache flushes.
        publish_fs = self.publish_with_cache_flush(repo_fs, set_cdn_published)

        # Then wait for publishes to finish.
        for f in publish_fs:
            f.result()

        # At this stage we consider all items to be fully "pushed".
        pushed_items = [
            attr.evolve(
                item, pushsource_item=attr.evolve(item.pushsource_item, state="PUSHED")
            )
            for item in all_items
        ]
        self.update_push_items(pushed_items)

        # Mark as done for accurate progress logs.
        # Note we don't keep track of exactly which items got published through each
        # repo, so this will simply show that everything moved from in progress to done
        # at once.
        for item in pushed_items:
            self.progress_info.incr_out()
            self.put_output(item)
