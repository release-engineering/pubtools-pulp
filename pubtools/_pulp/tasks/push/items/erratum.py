import logging
import re
import attr
from pushsource import ErratumPushItem
from pubtools.pulplib import (
    ErratumUnit,
    Criteria,
)

from .base import supports_type, PulpPushItem, State, UploadContext
from . import erratum_conv


LOG = logging.getLogger("pubtools.pulp")


@attr.s(frozen=True, slots=True)
class ErratumUploadContext(UploadContext):
    """A custom context for Erratum uploads.

    This context object avoids having to query the PushItem's repo repeatedly.
    """

    upload_repo = attr.ib(default=None)


class ErratumPushItemException(BaseException):
    """
    Custom exception for PulpErratumPushItem specific issues.

    Mainly used for when a year value can't be parsed from the advisory name.
    """


@supports_type(ErratumPushItem)
@attr.s(frozen=True, slots=True)
class PulpErratumPushItem(PulpPushItem):
    """Handler for errata."""

    MULTI_UPLOAD_CONTEXT = True
    ADVISORY_PATTERN = re.compile(r"RH.A-(\d{4})", re.IGNORECASE)
    CONTENT_SPLIT_RANGES = [(2014, 2040), (8014, 8040)]

    @property
    def upload_repo(self):
        # Split errata into different repos by year, assuming the advisory name
        # is formatted like RHXA-YYYY
        name_match = self.ADVISORY_PATTERN.match(self.pushsource_item.name)
        if not name_match:
            LOG.error(
                "Bad Advisory name: '%s' does not contain a reasonable year value.",
                self.pushsource_item.name,
            )
            raise ErratumPushItemException
        year = int(name_match.group(1))
        if not any([r[0] <= year <= r[1] for r in self.CONTENT_SPLIT_RANGES]):
            LOG.warning(
                "%s was not in a valid date range for repo content splitting, using the default.",
                self.pushsource_item.name,
            )
            year = "0000"
        return "all-erratum-content-%s" % year

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
            if not (
                repo_id.startswith("all-rpm-content")
                or repo_id.startswith("all-erratum-content")
            ):
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

    def upload_context(self, pulp_client):
        return ErratumUploadContext(
            client=pulp_client,
            upload_repo=pulp_client.get_repository(self.upload_repo),
        )

    @classmethod
    def match_items_units(cls, items, units):
        units_by_id = {}

        for unit in units:
            assert isinstance(unit, ErratumUnit)
            units_by_id[unit.id] = unit

        for item in items:
            yield item.with_unit(units_by_id.get(item.pushsource_item.name))

    def ensure_uploaded(self, ctx, repo_f=None):
        # Overridden to force our desired upload repo.
        return super(PulpErratumPushItem, self).ensure_uploaded(ctx, ctx.upload_repo)

    def upload_to_repo(self, repo):
        # Convert the push item into a Pulp unit - possibly includes a version bump
        # of the old unit, if there was one.
        new_unit = erratum_conv.unit_for_item(
            self.pushsource_item, old_unit=self.pulp_unit
        )
        return repo.upload_erratum(new_unit)
