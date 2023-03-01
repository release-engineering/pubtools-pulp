"""Converters between erratum push items and pulp units."""

import logging
from pushsource import ErratumPushItem

from pubtools.pulplib import (
    ErratumUnit,
    ErratumReference,
    ErratumModule,
    ErratumPackage,
    ErratumPackageCollection,
)


LOG = logging.getLogger("pubtools.pulp")


def unit_for_item(item, old_unit):
    assert isinstance(item, ErratumPushItem)

    return ErratumUnit(
        # Fields which are plainly copied
        id=item.name,
        status=item.status,
        updated=item.updated,
        issued=item.issued,
        description=item.description,
        pushcount=item.pushcount,
        reboot_suggested=item.reboot_suggested,
        from_=item.from_,
        rights=item.rights,
        title=item.title,
        severity=item.severity,
        release=item.release,
        type=item.type,
        solution=item.solution,
        summary=item.summary,
        content_types=item.content_types,
        container_list=item.container_list,
        # This field needs to be bumped if there is an existing unit,
        # else Pulp will discard changes.
        version=unit_erratum_version(item, old_unit),
        # These types need some conversions applied.
        references=unit_erratum_references(item),
        pkglist=unit_erratum_pkglist(item),
    )


def unit_erratum_version(item, old_unit):
    # The version from the push item is only used if there was no erratum in pulp,
    # or the pulp version couldn't be bumped.
    if old_unit:
        return bump_erratum_version(old_unit, item.version)

    return item.version


def bump_erratum_version(unit, default_version="1"):
    # if erratum already exists in Pulp, then the new version must be higher than
    # the old version. If the version can't be bumped, it defaults to the provied
    # default_version.
    assert isinstance(unit, ErratumUnit)

    try:
        version_int = int(unit.version) + 1
        return str(version_int)
    except ValueError:
        LOG.warning(
            "Erratum %s in Pulp has non-integer version '%s', forcing value to '%s'",
            unit.id,
            unit.version,
            default_version,
        )

    return str(default_version)


def unit_erratum_references(item):
    return [
        ErratumReference(
            href=ref.href,
            id=ref.id,
            title=ref.title,
            type=ref.type,
        )
        for ref in item.references
    ]


def unit_erratum_packages(item_packages):
    return [
        ErratumPackage(
            arch=pkg.arch,
            filename=pkg.filename,
            epoch=pkg.epoch,
            name=pkg.name,
            version=pkg.version,
            release=pkg.release,
            src=pkg.src,
            reboot_suggested=pkg.reboot_suggested,
            md5sum=pkg.md5sum,
            sha1sum=pkg.sha1sum,
            sha256sum=pkg.sha256sum,
        )
        for pkg in (item_packages or [])
    ]


def unit_erratum_module(m):
    if m is None:
        return None

    return ErratumModule(
        name=m.name,
        stream=m.stream,
        version=m.version,
        context=m.context,
        arch=m.arch,
    )


def unit_erratum_pkglist(item):
    assert isinstance(item, ErratumPushItem)

    return [
        ErratumPackageCollection(
            name=c.name,
            short=c.short,
            packages=unit_erratum_packages(c.packages),
            module=unit_erratum_module(c.module),
        )
        for c in (item.pkglist or [])
    ]
