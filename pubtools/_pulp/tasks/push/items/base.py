import logging
import random
import os
from functools import partial

from pushsource import PushItem
from pubtools.pulplib import Unit, Criteria


from more_executors.futures import f_map, f_flat_map, f_return, f_sequence
import attr

from ..copy import CopyOperation, asserting_all_copied_ok


# A mapping between PushItem classes and the PulpPushItem wrappers
# we'll use to handle them. Starts empty and is built up as each
# class is registered.
SUPPORTED_TYPES = []

LOG = logging.getLogger("pubtools.pulp")


def supports_type(pushitem_type):
    """Decorator used to define which PulpPushItem subclass implements support
    for each PushItem subclass.
    """

    def fn(klass):
        SUPPORTED_TYPES.append((pushitem_type, klass))
        return klass

    return fn


class State(object):
    """Possible states of a push item in Pulp with respect to our workflow.

    In typical cases, a push item is expected to transition linearly through
    the non-error states listed below. Some content types might be able to
    skip some states.

    TODO: please make this a real enum once py2 support is dropped.
    """

    # State with respect to Pulp is unknown.
    UNKNOWN = "UNKNOWN"

    # Not present in Pulp at all
    MISSING = "MISSING"

    # Present, but is an orphan (not available in any repo and therefore
    # not available for association tasks)
    ORPHAN = "ORPHAN"

    # Present, but the unit has to be re-uploaded to perform some mutations
    # (example: erratum unit with changes to some fields).
    NEEDS_REUPLOAD = "NEEDS_REUPLOAD"

    # Present, but the unit needs some mutable fields to be updated without
    # requiring a full reupload (example: 'description' on an installer ISO).
    NEEDS_UPDATE = "NEEDS_UPDATE"

    # Present and in some repos, but not in all the desired repos.
    PARTIAL = "PARTIAL"

    # Present and in all the desired repos.
    IN_REPOS = "IN_REPOS"


@attr.s(frozen=True)
class UploadContext(object):
    client = attr.ib(default=None)
    random = attr.ib(default=None)


@attr.s(frozen=True)
class PulpPushItem(object):
    """Wraps a pushitem with additional info for Pulp push.

    This class must be subclassed for each specific content type supporting push.
    """

    pushsource_item = attr.ib(type=PushItem)
    """Underlying pushsource.PushItem without any Pulp-specific details."""

    pulp_state = attr.ib(type=int, default=State.UNKNOWN)
    """Current state of item in Pulp."""

    pulp_unit = attr.ib(type=Unit, default=None)
    """Current Pulp unit for this item, if known.

    This value is always ``None`` for the push items which don't directly map
    to a single unit (e.g. modulemd YAML files; comps.xml files).
    """

    @classmethod
    def for_item(cls, pushsource_item, **kwargs):
        """Given a pushsource.PushItem, returns an instance of a PulpPushItem wrapper
        of a concrete subtype, or None if the push item is unsupported."""

        for (pushitem_type, wrapper_type) in SUPPORTED_TYPES:
            if isinstance(pushsource_item, pushitem_type):
                return wrapper_type(pushsource_item=pushsource_item, **kwargs)

    @classmethod
    def match_items_units(cls, items, units):
        """Given an iterable of items and an iterable of units, returns items
        with state evolved according to the given units (e.g. state updated to missing,
        orphan, present etc...)

        Every provided item will be returned, whether or not it matches with a unit.
        For example, if called with some items with a state of MISSING, and no units
        are found matching those items, then the same items will be returned with the
        state still MISSING.

        It is mandatory that every object in 'items' shares the same concrete type.

        Subclasses MUST override this to match between items and units in the most
        efficient way for that type (e.g. by checksum, by name, or other...)
        """
        if not items:
            return []

        klasses = list(set([type(item) for item in items]))
        if len(klasses) != 1:
            raise TypeError(
                "BUG: mixing item types: %s" % ", ".join([str(k) for k in klasses])
            )
        klass = klasses[0]

        if klass == PulpPushItem:
            # This is a bug - PulpPushItem should have been subclassed.
            raise NotImplementedError()

        return klass.match_items_units(items, units)

    @classmethod
    def upload_context(cls, pulp_client):
        """Return a context object used during uploads.

        The context object will be shared across all uploads for a specific content
        type.

        Subclasses MAY override this to provide their own context
        (e.g. to cache a value rather than recalculating per upload).
        """
        return UploadContext(
            client=pulp_client,
            random=random.Random(float(os.getenv("PUBTOOLS_SEED") or random.random())),
        )

    @classmethod
    def items_by_type(cls, items):
        """Given an iterable of items, returns an iterable-of-iterable
        grouping items by their unit_type."""
        items_by_unit_type = {}
        for item in items:
            unit_type = item.unit_type
            items_by_unit_type.setdefault(unit_type, []).append(item)

        return items_by_unit_type.values()

    @classmethod
    def items_with_pulp_state_single_batch(cls, pulp_client, items):
        """Find Pulp state for a batch of items using a single Pulp query.
        Returns a Future[list] of updated items.

        It is mandatory that all provided items are of the same unit_type.
        The caller is responsible for ensuring this.
        """
        if not items:
            return f_return([])

        unit_type = items[0].unit_type

        if unit_type is None:
            # This means that the item doesn't map to a specific single unit type
            # (e.g. modulemd stream, comps.xml) and we don't support querying the
            # state at all; such items are simply returned as-is.
            return f_return(items)

        crit = Criteria.and_(
            Criteria.with_unit_type(unit_type),
            Criteria.or_(*[item.criteria() for item in items]),
        )
        LOG.info("Doing Pulp search: %s", crit)

        units_f = pulp_client.search_content(crit)
        matcher = partial(cls.match_items_units, items)
        return f_map(units_f, matcher)

    @classmethod
    def associated_items_single_batch(cls, pulp_client, items):
        """Associate a single batch of items into destination repos.

        This generator yields instances of Future[list[<associated-items>]].

        All provided items must be of the same unit_type.

        It is guaranteed that every yielded item exists in the desired
        target repos in Pulp. A fatal error occurs if this can't be done
        for any item in the batch.
        """

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
            src_repo = pulp_client.get_repository(src_repo_id)
            dest_repo = pulp_client.get_repository(dest_repo_id)

            crit = Criteria.and_(base_crit, Criteria.or_(*copy_crit[key]))

            oper = CopyOperation(src_repo_id, dest_repo_id, crit)
            oper.log_copy_start()

            copy_f = pulp_client.copy_content(
                src_repo.result(), dest_repo.result(), crit
            )

            # Stash the oper for logging later.
            copy_opers[copy_f] = oper

            copy_results.append(copy_f)

        # Copies have been started.
        # Any items which didn't need a copy can be immediately yielded now.
        if nocopy_items:
            yield f_return(nocopy_items)

        # Add some reasonable logging onto the copies...
        def log_copy_done(f):
            if not f.exception():
                tasks = f.result()
                oper = copy_opers[f]
                for t in tasks:
                    oper.log_copy_done(t)

        for f in copy_results:
            f.add_done_callback(log_copy_done)

        # A helper to refresh the state of each item in Pulp and make sure they
        # were copied OK.
        def refresh_after_copy(_):
            # Get an up-to-date version of all the copy items.
            f = cls.items_with_pulp_state_single_batch(pulp_client, copy_items)

            # Raise if any still have missing repos.
            f = f_map(f, asserting_all_copied_ok)

            return f

        # This future completes once *all* copies are done successfully.
        # TODO: this still could be improved, as not every item needs every copy
        # before the state could be refreshed.
        all_copies = f_sequence(copy_results)

        # To finish up: wait for all copies to complete, then refresh item states
        # and ensure they're no longer missing any repos.
        yield f_flat_map(all_copies, refresh_after_copy)

    @property
    def in_pulp_repos(self):
        """The repo IDs in which this item currently exists."""
        if self.pulp_unit and self.pulp_unit.repository_memberships:
            return self.pulp_unit.repository_memberships

        return []

    @property
    def missing_pulp_repos(self):
        """The repo IDs in which this item should exist, but currently does not."""
        desired_repos = self.pushsource_item.dest or []
        return sorted(set(desired_repos) - set(self.in_pulp_repos))

    @property
    def publish_pulp_repos(self):
        """The repo IDs which should be published in order to push this item.

        This is normally the exact set of repos to which we've been requested to
        push this item.

        Subclasses MAY override this to add additional repos onto the publish, for
        example if it is known that updates to this unit may have affected other
        repos as well.
        """
        return sorted(self.pushsource_item.dest)

    def with_checksums(self):
        """Return a copy of this item with checksums guaranteed to be present."""
        return attr.evolve(self, pushsource_item=self.pushsource_item.with_checksums())

    @property
    def blocking_checksums(self):
        """True if it is likely that with_checksums() on this item will block.

        This is a hint, to be used for scheduling purposes only.
        """
        return not (self.pushsource_item.md5sum and self.pushsource_item.sha256sum)

    def with_unit(self, unit):
        """Returns a copy of this item with state evolved according to the metadata in
        'unit'.

        Subclasses MAY override this to add different logic per content type.
        """

        # The new state of the item for pushsource/pushcollector.
        # Valid values are documented at:
        # https://release-engineering.github.io/pushcollector/schema.html
        pushsource_state = self.pushsource_item.state

        if not unit:
            # It's not in Pulp
            state = State.MISSING
        elif not unit.repository_memberships:
            # Not in any repo at all, or membership info unavailable
            state = State.ORPHAN
        elif set(self.pushsource_item.dest) - set(unit.repository_memberships):
            # In some repos, but not all desired repos
            state = State.PARTIAL

            # This counts as "existing" in Pulp since upload is not required.
            pushsource_state = "EXISTS"
        else:
            # It's already present in all the desired repos.
            state = State.IN_REPOS

            # Also counts as "existing" in Pulp.
            # Not quite PUSHED yet as we still want to ensure repos are published.
            pushsource_state = "EXISTS"

        out = attr.evolve(
            self,
            pulp_unit=unit,
            pulp_state=state,
            pushsource_item=attr.evolve(self.pushsource_item, state=pushsource_state),
        )

        # If the unit is present, but the state doesn't match what we want, mark it
        # as needing an update.
        if (
            state in [State.PARTIAL, State.IN_REPOS]
            and out.unit_for_update
            and out.unit_for_update != unit
        ):
            out = attr.evolve(out, pulp_state=State.NEEDS_UPDATE)

        return out

    def with_pulp_refreshed(self, pulp_client):
        """Returns a Future with a copy of this item, with the item's state refreshed
        from latest Pulp data."""
        crit = Criteria.and_(Criteria.with_unit_type(self.unit_type), self.criteria())

        def handle_result(page):
            items = [self]
            matched = self.match_items_units(items, page.data)
            return next(matched)

        return f_map(pulp_client.search_content(crit), handle_result)

    def ensure_uploaded(self, ctx, repo_f=None):
        """Ensure that this item is uploaded into at least one Pulp repo.

        Returns a Future with an updated copy of this item, resolved after
        upload succeeds.

        Should be called only if the caller has determined that the item needs
        an upload (e.g. because the item is missing).

        Subclasses MAY override this to customize upload behavior, however in
        most cases it makes more sense to override only `upload_to_repo`.
        """
        # In order to get the unit into Pulp, we must first upload it into some
        # (any) repo from dest.
        if repo_f is None:
            # Because uploading to a repo will lock the repo (during import task),
            # for the most possible concurrency it's best to try to uniformly
            # distribute the repos used for upload. For example if we receive
            # 100 items all for repos [a, b, c, d], we will get the best
            # performance if each repo is used for roughly 25 uploads.
            #
            # Hence the random choice of a target repo.
            repo_id = ctx.random.choice(self.pushsource_item.dest)
            repo_f = ctx.client.get_repository(repo_id)

        upload_tasks = f_flat_map(repo_f, self.upload_to_repo)

        # Helper to verify that we've really got into at least one repo as a result
        # of the upload.
        def asserting_uploaded_ok(item):
            if not item.in_pulp_repos:
                msg = (
                    "Fatal error: item supposedly uploaded successfully, "
                    "but remains missing from Pulp: %s"
                ) % item.pushsource_item
                raise RuntimeError(msg)
            return item

        updated_f = f_flat_map(
            upload_tasks, lambda _: self.with_pulp_refreshed(ctx.client)
        )
        return f_map(updated_f, asserting_uploaded_ok)

    def ensure_uptodate(self, client):
        """Ensure that this item is up-to-date in Pulp.

        In this context, up-to-date means that any mutable fields on the associated
        Pulp unit hold the desired values (e.g. "description" field on FileUnits).

        Returns a Future with an updated copy of this item, resolved after
        the item has been updated.
        """

        LOG.info("Updating fields on %s", self.pushsource_item.name)
        update = client.update_content(self.unit_for_update)

        # Helper to verify that our update really did make the item up-to-date.
        def asserting_updated_ok(item):
            if item.pulp_state not in (State.PARTIAL, State.IN_REPOS):
                msg = (
                    "Fatal error: item supposedly updated successfully, "
                    "but actual and desired state still differ:\n"
                    "  item:         %s\n"
                    "  current unit: %s\n"
                    "  desired unit: %s"
                ) % (item.pushsource_item, item.pulp_unit, item.unit_for_update)
                raise RuntimeError(msg)
            return item

        updated_f = f_flat_map(update, lambda _: self.with_pulp_refreshed(client))
        return f_map(updated_f, asserting_updated_ok)

    @property
    def can_pre_push(self):
        """Returns True if items of this type support 'pre-push' functionality,
        which means uploading content into Pulp in a manner which makes content
        available on Pulp internal storage but not available to end-users.

        Subclasses MAY override this method to return True. If so, ensure_uploaded
        MUST be implemented in a manner which does not make content available to
        end-users.
        """
        return False

    @property
    def unit_for_update(self):
        """Desired state of the Pulp unit associated with this item.

        Subclasses MAY override this method:

        - if this item doesn't map to a single Pulp unit, it should return None
        - if the Pulp unit is not considered mutable (i.e. uploading is the only
          way to replace/change a unit), it should return None
        - if the Pulp unit is considered mutable, it should return a copy of
          `pulp_unit` with all fields evolved to the desired state.
        """
        return None

    @property
    def unit_type(self):
        """The pulplib.Unit subclass which corresponds to this item type.

        Subclasses MUST override this method:

        - if this item doesn't map to a single Pulp unit, it should return None
        - otherwise, it should return the specific Unit subclass associated with
          the item
        """
        raise NotImplementedError()

    def criteria(self):
        """Returns a Criteria object capable of finding this item in Pulp.

        Subclasses SHOULD override this method if the item maps to a single Pulp
        unit. In that case, it should return a Criteria which will identify that
        unit.

        Otherwise, subclasses should not override the method.
        """
        return None

    def upload_to_repo(self, repo):
        """Upload this item to a specific repo.

        Subclasses MUST override this method to invoke whichever upload method is
        appropriate for the handled content type.
        """
        raise NotImplementedError()
