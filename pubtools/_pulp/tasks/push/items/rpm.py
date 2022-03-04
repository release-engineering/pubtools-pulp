import os

from pubtools.pulplib import RpmUnit, Criteria
from pushsource import RpmPushItem
import attr

from .base import supports_type, PulpPushItem


@attr.s(frozen=True, slots=True)
class RpmUploader(object):
    """A custom context for RPM uploads.

    This context object avoids having to query the all-rpm-content repo repeatedly.
    """

    upload_repo = attr.ib(default=None)
    client = attr.ib(default=None)


@supports_type(RpmPushItem)
@attr.s(frozen=True, slots=True)
class PulpRpmPushItem(PulpPushItem):
    """Handler for RPMs."""

    # RPMs are always uploaded to this repo first.
    UPLOAD_REPO = "all-rpm-content"

    @property
    def unit_type(self):
        return RpmUnit

    @property
    def rpm_nvr(self):
        # (n, v, r) tuple derived from filename, used in cdn_path calculation

        # TODO: handle the case where below code crashes because the RPM filename
        # is invalid. Just make it raise a more useful error message in that case
        # so it's clear what the problem is.

        # ipa-admintools-4.4.0-14.el7_3.1.1.noarch.rpm
        filename = self.pushsource_item.name

        # mpr.hcraon.1.1.3_7le.41-0.4.4-slootnimda-api
        filename_rev = "".join(reversed(filename))

        # 1.1.3_7le.41-0.4.4-slootnimda-api
        nvr_rev = filename_rev.split(".", 2)[2]

        # ('1.1.3_7le.41', '0.4.4', 'slootnimda-api')
        components_revrev = nvr_rev.split("-", 2)

        # ['14.el7_3.1.1', '4.4.0', 'ipa-admintools']
        components_rev = ["".join(reversed(c)) for c in components_revrev]

        # ('ipa-admintools', '4.4.0', '14.el7_3.1.1')
        return tuple(reversed(components_rev))

    @property
    def cdn_path(self):
        """Desired value of RpmUnit.cdn_path field."""
        (n, v, r) = self.rpm_nvr
        return os.path.join(
            "/content/origin/rpms",
            n,
            v,
            r,
            (self.pushsource_item.signing_key or "none").lower(),
            self.pushsource_item.name,
        )

    def criteria(self):
        return Criteria.with_field("sha256sum", self.pushsource_item.sha256sum)

    @classmethod
    def match_items_units(cls, items, units):
        units_by_sum = {}

        for unit in units:
            assert isinstance(unit, RpmUnit)
            units_by_sum[unit.sha256sum] = unit

        for item in items:
            yield item.with_unit(units_by_sum.get(item.pushsource_item.sha256sum))

    @classmethod
    def upload_context(cls, pulp_client):
        return RpmUploader(
            client=pulp_client, upload_repo=pulp_client.get_repository(cls.UPLOAD_REPO)
        )

    @property
    def can_pre_push(self):
        # We support pre-push by uploading to all-rpm-content first.
        return True

    def ensure_uploaded(self, ctx, repo_f=None):
        # Overridden to force our desired upload repo.
        return super(PulpRpmPushItem, self).ensure_uploaded(ctx, ctx.upload_repo)

    def upload_to_repo(self, repo):
        return repo.upload_rpm(self.pushsource_item.src, cdn_path=self.cdn_path)
