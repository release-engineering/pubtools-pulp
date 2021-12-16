import logging
import concurrent.futures
import random

from pushsource import Source
from pubtools.pulplib import Criteria

from .items import PulpPushItem, State
from .copy import CopyOperation
from ..common import Publisher, PulpTask
from ...services import (
    CollectorService,
    PulpClientService,
)

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


# Because pylint misunderstands the type of e.g. pulp_client:
# E1101: Instance of 'CollectorProxy' has no 'search_repository' member (no-member)
#
# pylint: disable=no-member


def batched_items(items, batchsize=100):
    # A simplistic batcher for an item generator.
    #
    # TODO: this batching should be smarter so that it does not block
    # the 'items' generator while processing a batch.
    #
    # Note: this could have used 'grouper' recipe at
    # https://docs.python.org/3/library/itertools.html ,
    # however not all of the dependencies are available in py2.

    it = iter(items)
    batch = []

    while True:
        try:
            item = next(it)
        except StopIteration:
            break

        batch.append(item)
        if len(batch) >= batchsize:
            yield batch
            batch = []

    if batch:
        yield batch


def items_by_type(items):
    # Given an iterable of items, returns an iterable-of-iterable
    # grouping items by their unit_type.
    items_by_unit_type = {}
    for item in items:
        unit_type = item.unit_type
        items_by_unit_type.setdefault(unit_type, []).append(item)

    return items_by_unit_type.values()


class Push(
    CollectorService,
    PulpClientService,
    Publisher,
    PulpTask,
):
    """Push and publish content via Pulp."""

    def add_args(self):
        super(Push, self).add_args()

        self.add_publisher_args(self.parser)

        # TODO: --skip

        self.parser.add_argument(
            "--source", action="append", help="Source(s) of content to be pushed"
        )

    @step("Load push items")
    def all_pushitems(self):
        """Yields all push items found in the requested `--source', wrapped into
        PulpPushItem instances.
        """
        for source_url in self.args.source:
            with Source.get(source_url) as source:
                LOG.info("Loading items from %s", source_url)
                for item in source:
                    pulp_item = PulpPushItem.for_item(item)
                    if pulp_item:
                        yield pulp_item
                    else:
                        LOG.info("Skipping unsupported type: %s", item)

    @step("Calculate checksums")
    def pushitems_with_sums(self, items):
        """Yields push items with checksums filled in (if they were not already present)."""
        # TODO: improve performance by parallelizing
        for item in items:
            yield item.with_checksums()

    @step("Query items in Pulp")
    def pushitems_with_pulp_state(self, items):
        """Yields push items with Pulp state queried/calculated."""

        # We process items in batches so that we can find multiple items per
        # Pulp search rather than one at a time.
        for batch in batched_items(items):
            for items in items_by_type(batch):
                for item in self.pushitems_with_pulp_state_single_batch(items):
                    assert item
                    yield item

    def pushitems_with_pulp_state_single_batch(self, items):
        # Find Pulp state for a batch of items using a single Pulp query.
        #
        # It is mandatory that all provided items are of the same unit_type.
        if not items:
            return

        unit_type = items[0].unit_type

        if unit_type is None:
            # This means that the item doesn't map to a specific single unit type
            # (e.g. modulemd stream, comps.xml) and we don't support querying the
            # state at all; such items are simply returned as-is.
            for item in items:
                assert item
                yield item
            return

        crit = Criteria.and_(
            Criteria.with_unit_type(unit_type),
            Criteria.or_(*[item.criteria() for item in items]),
        )
        LOG.info("Doing Pulp search: %s", crit)

        units = self.pulp_client.search_content(crit).result()
        new_items = PulpPushItem.match_items_units(items, units)

        for item in new_items:
            assert item
            yield item

    @step("Upload items to Pulp")
    def uploaded_items(self, items):
        """Yields push items with item uploaded if needed, such that the item will
        be present in at least one Pulp repo.
        """

        uploaded = []
        needs_upload = []

        upload_context = {}

        for item in items:
            if item.pulp_state in [State.IN_REPOS, State.PARTIAL, State.NEEDS_UPDATE]:
                # This item is already in Pulp.
                uploaded.append(item)
            else:
                # This item is not in Pulp, or otherwise needs a reupload.
                item_type = type(item)
                if item_type not in upload_context:
                    upload_context[item_type] = item_type.upload_context(
                        self.pulp_client
                    )

                ctx = upload_context[item_type]

                needs_upload.append(item.ensure_uploaded(ctx))

        LOG.info(
            "Upload: %s item(s) already present, %s uploading",
            len(uploaded),
            len(needs_upload),
        )

        # Anything already in the system can be immediately yielded.
        for item in uploaded:
            assert item
            yield item

        # Then wait for the completion of anything we're uploading.
        # TODO: apply a configurable timeout
        for item in concurrent.futures.as_completed(needs_upload):
            out = item.result()
            assert out
            yield out

    @step("Update items in Pulp")
    def uptodate_items(self, items):
        """Yields push items with item updated if needed, i.e. with any mutable fields
        set to their desired values.
        """

        no_update_needed = []
        update_needed = []

        for item in items:
            if item.pulp_state not in State.NEEDS_UPDATE:
                # This item is already up-to-date in Pulp (or just doesn't support
                # being updated)
                no_update_needed.append(item)
            else:
                # This item needs an update.
                update_needed.append(item.ensure_uptodate(self.pulp_client))

        LOG.info(
            "Update: %s item(s) already up-to-date, %s updating",
            len(no_update_needed),
            len(update_needed),
        )

        # Anything already in the system can be immediately yielded.
        for item in no_update_needed:
            yield item

        # Then wait for the completion of anything we're uploading.
        # TODO: apply a configurable timeout
        for item in concurrent.futures.as_completed(update_needed):
            out = item.result()
            assert out
            yield out

    @step("Associate items in Pulp")
    def associated_items(self, items):
        """Yields push items with item associated into target Pulp repos.

        Each yielded item has been placed into all of the desired Pulp repos according
        to the push item 'dest'.
        """

        for batch in batched_items(items):
            for items in items_by_type(batch):
                for item in self.associated_items_single_batch(items):
                    assert item
                    yield item

    def associated_items_single_batch(self, items):
        # Associate a single batch of items into destination repos.
        #
        # All provided items must be of the same unit_type.
        #
        # It is guaranteed that every yielded item exists in the desired
        # target repos in Pulp. A fatal error occurs if this can't be done
        # for any item in the batch.
        copy_crit = {}
        copy_opers = {}
        copy_results = []

        copy_items = []
        nocopy_items = []

        unit_type = items[0].unit_type

        base_crit = Criteria.with_unit_type(unit_type) if unit_type else None

        for item in items:
            if not item.missing_pulp_repos:
                # Don't need to do anything with this item.
                nocopy_items.append(item)
            else:
                copy_items.append(item)
                crit = item.criteria()
                # This item needs to be copied into each of the missing repos.
                for dest_repo_id in item.missing_pulp_repos:
                    # The source repo for copy can be anything. However, as copying
                    # locks both src and dest repo, it's better to select the src
                    # randomly so the locks tend to be uniformly distributed.
                    #
                    # TODO: could be sped up by looking for the repo with the smallest
                    # available queue.
                    #
                    src_repo_id = random.sample(item.in_pulp_repos, 1)[0]
                    key = (src_repo_id, dest_repo_id)
                    copy_crit.setdefault(key, []).append(crit)

        for key in copy_crit.keys():
            (src_repo_id, dest_repo_id) = key

            # TODO: cache repo lookups?
            src_repo = self.pulp_client.get_repository(src_repo_id)
            dest_repo = self.pulp_client.get_repository(dest_repo_id)

            crit = Criteria.and_(base_crit, Criteria.or_(*copy_crit[key]))

            oper = CopyOperation(src_repo_id, dest_repo_id, crit)
            oper.log_copy_start()

            copy_f = self.pulp_client.copy_content(
                src_repo.result(), dest_repo.result(), crit
            )

            # Stash the oper for logging later.
            copy_opers[copy_f] = oper

            copy_results.append(copy_f)

        # Copies have been started.
        # Any items which didn't need a copy can be immediately yielded now.
        for item in nocopy_items:
            assert item
            yield item

        # Then wait for copies to complete.
        # TODO: apply a configurable timeout
        for copy in concurrent.futures.as_completed(copy_results):
            oper = copy_opers[copy]
            tasks = copy.result()
            for t in tasks:
                oper.log_copy_done(t)

        # All copies succeeded.
        # Now re-query the same items from Pulp, but this time expecting that
        # they are in all the repos.
        for item in self.pushitems_with_pulp_state_single_batch(copy_items):
            missing_repos = item.missing_pulp_repos
            if missing_repos:
                # TODO: consider improving this to add a bit more detail,
                # e.g. mention which copy operation was expected to cover this item.
                msg = (
                    "Fatal error: Pulp unit not present in repo(s) %s after copy: %s"
                    % (", ".join(missing_repos), item.pulp_unit)
                )
                raise RuntimeError(msg)

            assert item
            yield item

    def run(self):
        # Push workflow.
        #
        # Note most of these calls below are generators, so we're not
        # loading all items at once - most steps are pipelined together.
        #
        # TODO: insert calls to pushcollector throughout the below to ensure
        # push item state is updated as the push runs.

        # Locate all items for push.
        items = self.all_pushitems()

        # Ensure we have checksums for all of them (needed for Pulp search);
        # this potentially involves slow reading of content, e.g. over NFS.
        items = self.pushitems_with_sums(items)

        # Get Pulp state for each of these items (e.g. is it already in Pulp; is
        # it in all the correct repos). This will do batched queries to Pulp.
        items = self.pushitems_with_pulp_state(items)

        # Ensure all items are uploaded to Pulp. This uploads bytes into Pulp
        # but does not guarantee the items are present in each of the desired
        # destination repos.
        # TODO: pre-push or 'nochannel' support.
        items = self.uploaded_items(items)

        # Ensure all items are up-to-date in Pulp. This adjusts any mutable fields
        # whose current value doesn't match the desired value.
        items = self.uptodate_items(items)

        # Synchronization point prior to association.
        #
        # Why: it is strongly encouraged to ensure all modulemds are put into repos
        # before we start putting RPMs into them, to reduce the risk that we could
        # accidentally expose an RPM without the corresponding modulemd. Therefore we
        # need to ensure any module items are processed by uploaded_items above,
        # before we can proceed to associate any RPM items.
        #
        # TODO: try to make this smarter so it handles only that modulemd/rpm case
        # described above without slowing other stuff down?
        items = list(items)

        # Ensure all the uploaded items are present in all the target repos.
        # TODO: pre-push or 'nochannel' support should avoid doing this.
        items = self.associated_items(items)

        # It is now the case that all items exist with the desired state, in the
        # desired repos. Now we need to publish affected repos.
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
        for item in items:
            all_repo_ids.update(item.publish_pulp_repos)

            # any unit which supports cdn_published but hasn't had it set yet should
            # have it set once the publish completes.
            unit = item.pulp_unit
            if hasattr(unit, "cdn_published") and unit.cdn_published is None:
                set_cdn_published.add(unit)

        # Locate all the repos for publish.
        repo_fs = self.pulp_client.search_repository(
            Criteria.with_id(sorted(all_repo_ids))
        )

        # Start publishing them, including cache flushes.
        publish_fs = self.publish_with_cache_flush(repo_fs, set_cdn_published)

        # Wait for everything to finish.
        for f in publish_fs:
            f.result()
