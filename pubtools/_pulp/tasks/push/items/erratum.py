import logging

from pubtools.pulplib import (
    ErratumUnit,
    Criteria,
)
from pushsource import ErratumPushItem
import attr

from .base import supports_type, PulpPushItem, State
from . import erratum_conv


LOG = logging.getLogger("pubtools.pulp")


@supports_type(ErratumPushItem)
@attr.s(frozen=True, slots=True)
class PulpErratumPushItem(PulpPushItem):
    """Handler for errata."""

    @property
    def unit_type(self):
        return ErratumUnit

    def criteria(self):
        return Criteria.with_field("id", self.pushsource_item.name)

    @property
    def publish_pulp_repos(self):
        # Due to the mutable nature of errata, plus the fact that client tools
        # complain if the same errata are found in multiple repos with not-identical
        # fields, the repos to publish for an erratum are all the usual *plus* every
        # repo containing that erratum - whether or not we're trying to push there
        # right now.
        #
        out = set(super(PulpErratumPushItem, self).publish_pulp_repos)

        for repo_id in self.in_pulp_repos:
            # Though the existing code doesn't push errata to all-rpm-content,
            # historically advisories have been pushed there. We shouldn't return
            # this from publish_pulp_repos as there's no point in trying to publish
            # it, and worse, UD cache flush on this repo gives a fatal error.
            #
            # Also note that there's an entire family of these repos, hence the
            # startswith rather than plain equality check.
            if not repo_id.startswith("all-rpm-content"):
                out.add(repo_id)

        return sorted(out)

    def with_unit(self, unit):
        # with_unit is overridden to add handling for the mutable fields on
        # erratum units.
        out = super(PulpErratumPushItem, self).with_unit(unit)

        if unit and out.pulp_state in (State.PARTIAL, State.IN_REPOS):
            # If it's present in Pulp, we'll have to check if it's equal
            # to the desired value and mark it as needing an update if not.
            new_unit = erratum_conv.unit_for_item(self.pushsource_item, unit)

            # Compare present/desired fields with the exception of certain fields...
            unit_cmp = attr.evolve(unit, version="", repository_memberships=None)
            new_unit_cmp = attr.evolve(
                new_unit, version="", repository_memberships=None
            )

            if unit_cmp != new_unit_cmp:
                # It's not what we want it to be; it'll need to be uploaded again.
                out = attr.evolve(out, pulp_state=State.NEEDS_REUPLOAD)

        return out

    @classmethod
    def match_items_units(cls, items, units):
        units_by_id = {}

        for unit in units:
            assert isinstance(unit, ErratumUnit)
            units_by_id[unit.id] = unit

        for item in items:
            yield item.with_unit(units_by_id.get(item.pushsource_item.name))

    def upload_to_repo(self, repo):
        # Convert the push item into a Pulp unit - possibly includes a version bump
        # of the old unit, if there was one.
        new_unit = erratum_conv.unit_for_item(
            self.pushsource_item, old_unit=self.pulp_unit
        )
        return repo.upload_erratum(new_unit)
