import os

from pubtools.pulplib import FileUnit, Criteria
from pushsource import FilePushItem
import attr

from .base import supports_type, PulpPushItem


@supports_type(FilePushItem)
@attr.s(frozen=True)
class PulpFilePushItem(PulpPushItem):
    """Handler for generic files (in Pulp2 terms, "iso" units)."""

    @property
    def file_key(self):
        """A key which (should) uniquely identify this item in Pulp."""
        return (self.pushsource_item.name, self.pushsource_item.sha256sum)

    @property
    def cdn_path(self):
        """Desired value of FileUnit.cdn_path field."""
        checksum = self.pushsource_item.sha256sum
        return os.path.join(
            "/content/origin/files/sha256",
            checksum[:2],
            checksum,
            os.path.basename(self.pushsource_item.name),
        )

    @property
    def unit_type(self):
        return FileUnit

    @property
    def unit_for_update(self):
        return attr.evolve(
            self.pulp_unit,
            description=self.pushsource_item.description,
            version=self.pushsource_item.version,
            display_order=self.pushsource_item.display_order,
            # Note: cdn_path is intentionally omitted here as it would not be safe
            # to change that value if the unit was already published, so we only
            # support setting it on upload.
        )

    def criteria(self):
        return Criteria.and_(
            Criteria.with_field("sha256sum", self.pushsource_item.sha256sum),
            Criteria.with_field("path", self.pushsource_item.name),
        )

    @classmethod
    def match_items_units(cls, items, units):
        units_by_key = {}

        for unit in units:
            assert isinstance(unit, FileUnit)
            key = (unit.path, unit.sha256sum)
            units_by_key[key] = unit

        for item in items:
            yield item.with_unit(units_by_key.get(item.file_key))

    def upload_to_repo(self, repo):
        return repo.upload_file(
            self.pushsource_item.src,
            relative_url=self.pushsource_item.name,
            description=self.pushsource_item.description,
            version=self.pushsource_item.version,
            display_order=self.pushsource_item.display_order,
            cdn_path=self.cdn_path,
            # If there's an existing pulp unit which has already been published to
            # CDN, then cdn_publish is copied across.
            # The only scenario where we expect this to happen is if the unit is
            # an orphan, as otherwise we wouldn't be uploading at all.
            cdn_published=self.pulp_unit.cdn_published if self.pulp_unit else None,
        )
