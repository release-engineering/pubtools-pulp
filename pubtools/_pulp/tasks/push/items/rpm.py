import os

from pushsource import RpmPushItem
import attr
import six
from pubtools.pulplib import RpmUnit, Criteria

from .base import supports_type, PulpPushItem, UploadContext


@attr.s(frozen=True, slots=True)
class RpmUploadContext(UploadContext):
    """A custom context for RPM uploads.

    This context object avoids having to query the all-rpm-content repo repeatedly.
    """

    upload_repo = attr.ib(default=None)


@supports_type(RpmPushItem)
@attr.s(frozen=True, slots=True)
class PulpRpmPushItem(PulpPushItem):
    """Handler for RPMs."""
    extra_export_fields = ["cdn_path", "upload_key", "rpm_nvr"]

    # RPMs are always uploaded to this repo first.
    UPLOAD_REPO = "all-rpm-content"

    @property
    def unit_type(self):
        return RpmUnit

    @property
    def rpm_nvr(self):
        # (n, v, r) tuple derived from filename, used in cdn_path calculation

        # Filename convention can be found at:
        # http://ftp.rpm.org/max-rpm/ch-rpm-file-format.html

        try:
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
        except Exception as exc:  # pylint: disable=broad-except
            # Crashes above may be a bit hard to understand, so we raise with
            # a more self-explanatory message.
            six.raise_from(
                ValueError(
                    "Invalid RPM filename %s (expected: "
                    "[name]-[version]-[release].[arch].rpm)" % self.pushsource_item.name
                ),
                exc,
            )

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
        return RpmUploadContext(
            client=pulp_client,
            upload_repo=pulp_client.get_repository(cls.UPLOAD_REPO),
        )

    @property
    def can_pre_push(self):
        # We support pre-push by uploading to all-rpm-content first.
        return True

    @property
    def supports_signing(self):
        # It is possible for RPMs to be signed.
        return True

    @property
    def is_signed(self):
        # The RPM is signed if signing_key is non-empty.
        return self.pushsource_item and bool(self.pushsource_item.signing_key)

    @property
    def upload_key(self):
        # Any prior upload of identical content can be reused.
        return self.pushsource_item.sha256sum

    @property
    def unit_fields(self):
        # RpmUnits contain some complex fields but only a minority
        # are relevant to us. Here we request only those fields we
        # need to operate successfully.
        return [
            "name",
            "version",
            "release",
            "arch",
            "sha256sum",
            "repository_memberships",
            "cdn_path",
            "cdn_published",
            "unit_id",
        ]

    def ensure_uploaded(self, ctx, repo_f=None):
        # Overridden to force our desired upload repo.
        return super(PulpRpmPushItem, self).ensure_uploaded(ctx, ctx.upload_repo)

    def upload_to_repo(self, repo):
        return repo.upload_rpm(self.pushsource_item.src, cdn_path=self.cdn_path)
