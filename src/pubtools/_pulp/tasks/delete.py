import logging
import re
import sys
from functools import partial

import attr
from more_executors.futures import f_flat_map, f_map, f_return, f_sequence, f_zip
from pubtools.pulplib import (
    Criteria,
    ErratumUnit,
    FileUnit,
    Matcher,
    ModulemdUnit,
    RpmUnit,
)
from pushsource import FilePushItem, ModuleMdPushItem, RpmPushItem

from pubtools._pulp.arguments import SplitAndExtend
from pubtools._pulp.services.collector import CollectorService
from pubtools._pulp.services.pulp import PulpClientService
from pubtools._pulp.task import PulpTask
from pubtools._pulp.tasks.common import Publisher

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step

MODULEMD_REGEX = re.compile(r"^[-.+\w]+:[-.+\w]+:\d+(:[-.+\w]+){0,2}$")

ALL_REPOS_INDICATOR = "*"

@attr.s
class ClearedRepo(object):
    """Represents a single repo where contents were removed."""

    tasks = attr.ib()
    """The completed Pulp tasks for removing content from this repo."""

    repo = attr.ib()
    """The repo which where content was cleared."""

    content = attr.ib()
    """The content that was cleared from the repo"""


@attr.s
class RemoveUnitItem(object):
    """Represents a unit with destination repos"""

    unit = attr.ib()
    """A Pulp unit"""

    repos = attr.ib()
    """The list of repos from where the unit to be removed"""


@attr.s
class RpmInfoItem(object):
    """Represents an RPM info item with filename and sha256sum"""

    filename = attr.ib()
    """RPM filename"""

    sha256sum = attr.ib()
    """sha256 checksum of the RPM"""


class Delete(PulpClientService, CollectorService, Publisher, PulpTask):
    """Remove specific content from one or more Pulp repository"""

    def __init__(self, *args, **kwargs):
        super(Delete, self).__init__(*args, **kwargs)
        self.to_await = []

    def add_args(self):
        super(Delete, self).add_args()

        self.add_publisher_args(self.parser)

        self.parser.add_argument(
            "--repo",
            help="remove content from these comma-seperated repositories. If "
                 "'%s' is used, the package will be removed from all repos, "
                 "excluding all-rpm-content-* repos. These may be added "
                 "separately." % ALL_REPOS_INDICATOR,
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--advisory",
            help="remove packages and modules in these comma-separated advisories",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--file",
            help="remove these comma-separated rpm, srpm, modulemd or iso file(s)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--signing-key",
            help="remove the content with these signing-keys(s)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--allow-unsigned", help="remove unsigned content", action="store_true"
        )

        self.parser.add_argument(
            "--skip", help="skip given comma-separated sub-steps", type=str
        )

    def fail(self, *args, **kwargs):
        LOG.error(*args, **kwargs)
        sys.exit(30)

    def run(self):
        repos = {}
        file_fs = []
        signing_keys = self.args.signing_key
        repo_names = self.args.repo
        advisories = self.args.advisory

        if not (self.args.file or self.args.advisory):
            self.fail("One of --file or --advisory is required")

        if self.args.file and not self.args.repo:
            self.fail("Repository names in --repo is required")

        if not signing_keys and self.args.allow_unsigned:
            signing_keys = [None]

        # separates files into rpms, isos and modules
        rpms, files, modules = self.separate_files(self.args.file or [])

        if rpms and not signing_keys:
            self.fail(
                "One of --signing-key or --allow-unsigned is required to delete RPMs"
            )

        rpms_info = [RpmInfoItem(filename=rpm, sha256sum=None) for rpm in rpms]

        # delete files
        file_f = self._delete_standalone_files(
            repo_names, signing_keys, rpms_info, files, modules
        )

        # delete packages from advisory
        if advisories:
            file_fs = self.delete_from_advisories(advisories, repo_names)

        # wait for the futures to resolve
        # file_f returns a list of futures for content removal on each repo
        # file_fs is a list of file_f from deletion of set of rpms and modules in
        # each advisory
        file_fs.append(file_f)
        fs = f_sequence(file_fs)
        cleared_repos = [cr for sublist in fs.result() for cr in sublist]
        for cr in cleared_repos:
            repo = cr.repo
            repos.setdefault(repo.id, repo)

        # publish affected repos
        if repos:
            publish_fs = self.publish_with_cache_flush(sorted(repos.values()))

            f_sequence(publish_fs).result()

        # wait for any futures that were not resolved during the process
        if self.to_await:
            for f in self.to_await:
                f.result()

    def _delete_standalone_files(
        self, repo_names, signing_keys=None, rpms_info=None, files=None, modules=None
    ):
        rpm_f = f_return([])
        file_f = f_return([])
        module_f = f_return([])

        # delete files
        if files:
            file_f = self.delete_files(repo_names, files)

        # delete rpms
        if rpms_info:
            rpm_f = self.delete_rpms(repo_names, rpms_info, signing_keys)

        # delete modules
        if modules:
            module_f = self.delete_modules(repo_names, modules, signing_keys)

        fs = rpm_f.result() + file_f.result() + module_f.result()

        return f_sequence(fs)

    @step("Delete advisories")
    def delete_from_advisories(self, advisory_ids, repos):
        out = []

        # get advisory from pulp
        advisories = self.get_advisories(advisory_ids)

        # get verified_repos, packages and modules from advisory
        # verified_repos are the requested repos in advisory's
        # repository_memberships
        advisory_map = self.process_advisories(advisories, repos)

        # delete packages and modules
        for _, advisory_info in advisory_map.items():
            verified_repos, rpms_info, modules = advisory_info

            out.append(
                self._delete_standalone_files(
                    verified_repos, rpms_info=rpms_info, modules=modules
                )
            )

        # return fs
        return out

    @step("Delete modules")
    def delete_modules(self, repos, module_names, signing_keys=None):
        # get modules from Pulp
        mods_f = self.get_modules(module_names)

        # map modules to repos
        f = f_map(mods_f, partial(self.map_to_repo, repos=repos, unit_attr="nsvca"))
        repo_map_f = f_map(f, self.log_units)

        # remove packages in the modules from all the provided repos,
        # after establishing that the modules are present in one of the
        # provided repos in the previous step
        unit_map_f = f_map(f, lambda map: map[1])
        artifact_f = f_map(
            unit_map_f,
            lambda unit_map: self.remove_mod_artifacts(
                unit_map.values(), repos=repos, signing_keys=signing_keys
            ),
        )

        # hold for rpms in the artifacts to be removed before removing modules
        # wait for artifact_f to resolve for f_zip to resolve and return repo_map_f
        # to remove_modules in next step
        repo_map_f = f_map(
            f_zip(repo_map_f, f_sequence(artifact_f.result())), lambda t: t[0]
        )

        # remove modules
        cleared_repos_f = f_map(repo_map_f, self.remove_modules)

        # collect items
        f = f_map(cleared_repos_f, self.record_clears)
        self.to_await.extend(f.result())

        # return affected repos
        return cleared_repos_f

    @step("Delete files")
    def delete_files(self, repos, file_names):
        # get files from Pulp
        files_f = self.get_files(file_names)

        # map files to repos
        f = f_map(files_f, partial(self.map_to_repo, repos=repos, unit_attr="path"))
        repo_map_f = f_map(f, self.log_units)

        # remove from repos
        cleared_repos_f = f_map(repo_map_f, self.remove_files)

        # collect items
        f = f_map(cleared_repos_f, self.record_clears)
        self.to_await.extend(f.result())

        # return affected repos
        return cleared_repos_f

    @step("Delete RPMs")
    def delete_rpms(self, repos, rpms_info, signing_keys=None):
        # get rpms from Pulp
        rpms_f = self.get_rpms(rpms_info, signing_keys)

        # map rpms to repos
        f = f_map(rpms_f, partial(self.map_to_repo, repos=repos, unit_attr="filename"))
        repo_map_f = f_map(f, self.log_units)

        # remove rpms from repos
        cleared_repos_f = f_map(repo_map_f, self.remove_rpms)

        # collect items
        f = f_map(cleared_repos_f, self.record_clears)
        self.to_await.extend(f.result())

        # return affected repos
        return cleared_repos_f

    def log_units(self, maps):
        missing = []
        repo_map, unit_map = maps

        for unit, runit_item in sorted(unit_map.items()):
            repos = runit_item.repos
            if not repos:
                missing.append(unit)
            else:
                LOG.info("Deleting %s from %s", unit, ", ".join(sorted(repos)))

        if missing:
            LOG.warning(
                "Unit(s) don't belong to any requested repos %s: %s",
                ", ".join(sorted(repos)),
                ", ".join(sorted(missing)),
            )

        return repo_map

    def process_advisories(self, advisories, repos):
        advisory_map = {}

        for advisory in sorted(advisories):
            verified_repos = self.verify_repos_from_advisory(advisory, repos)
            if not verified_repos:
                LOG.warning(
                    "Advisory %s doesn't belong to any of the repos %s",
                    advisory.id,
                    ", ".join(sorted(repos)),
                )
                continue

            advisory_map.setdefault(advisory.id, []).append(verified_repos)
            rpms = []
            modules = []
            for pkg_coll in advisory.pkglist:
                for rpm in pkg_coll.packages or []:
                    rpms.append(RpmInfoItem(rpm.filename, rpm.sha256sum))

                if pkg_coll.module:
                    modules.append(self._get_nsvca(pkg_coll.module))

            advisory_map.get(advisory.id).extend([rpms, modules])

        self._log_from_advisory(advisory_map)

        return advisory_map

    def _log_from_advisory(self, advisory_map):
        for advisory_id, advisory_info in sorted(advisory_map.items()):
            repos, rpms, modules = advisory_info
            LOG.info(
                "%s packages and %s modules found from the advisory %s in repos %s",
                len(rpms),
                len(modules),
                advisory_id,
                ", ".join(sorted(repos)),
            )
            if rpms:
                LOG.info("RPMs:")
                for rpm in sorted(rpms, key=lambda v: v.filename):
                    LOG.info("- %s", rpm.filename)
            if modules:
                LOG.info("Modules:")
                for module in sorted(modules):
                    LOG.info("- %s", module)

    def _get_nsvca(self, module):
        nsvca = []
        for p in ["name", "stream", "version", "context", "arch"]:
            part = getattr(module, p)
            if part:
                nsvca.append(part)
        return ":".join(nsvca)

    def verify_repos_from_advisory(self, advisory, repos):
        verified_repos = []
        repo_membership = advisory.repository_memberships

        # return all the repos the advisory is part of if
        # repos are not specified in the request
        if not repos:
            return repo_membership

        for repo_name in repos:
            if repo_name not in repo_membership:
                LOG.warning("Advisory %s is not in repo %s", advisory.id, repo_name)
            else:
                verified_repos.append(repo_name)

        return verified_repos

    @step("Remove artifacts from modules")
    def remove_mod_artifacts(self, modules, repos, signing_keys):
        out = []
        for item in modules:
            rpms = item.unit.artifacts_filenames
            if rpms:
                rpms_info = [RpmInfoItem(filename=rpm, sha256sum=None) for rpm in rpms]
                out.append(self.delete_rpms(repos, rpms_info, signing_keys))

        return out

    @step("Get advisories")
    def get_advisories(self, advisory_ids):
        criteria = criteria = Criteria.and_(
            Criteria.with_unit_type(ErratumUnit), Criteria.with_id(advisory_ids)
        )
        adv_f = self.search_content(criteria)
        advisories = [a for a in adv_f.result()]
        if not advisories:
            self.fail("Advisory(ies) not found: %s", ", ".join(sorted(advisory_ids)))

        return advisories

    @step("Get RPMs")
    def get_rpms(self, rpms_info, signing_keys=None):
        rpm_names = [rpm_info.filename for rpm_info in rpms_info]
        criteria = self.unit_criteria(
            RpmUnit, self._rpm_search_criteria(rpms_info, signing_keys)
        )
        rpms_f = self.search_content(criteria)
        rpms_f = f_map(
            rpms_f,
            partial(
                self.log_missing_units,
                unit_names=rpm_names,
                unit_type="RPM",
                unit_attr="filename",
            ),
        )
        return rpms_f

    @step("Get files")
    def get_files(self, file_names):
        criteria = self.unit_criteria(FileUnit, self._file_search_criteria(file_names))
        files_f = self.search_content(criteria)
        files_f = f_map(
            files_f,
            partial(
                self.log_missing_units,
                unit_names=file_names,
                unit_type="file",
                unit_attr="path",
            ),
        )
        return files_f

    @step("Get modules")
    def get_modules(self, module_names):
        criteria = self.unit_criteria(
            ModulemdUnit, self._module_search_criteria(module_names)
        )
        mods_f = self.search_content(criteria)
        mods_f = f_map(
            mods_f,
            partial(
                self.log_missing_units,
                unit_names=module_names,
                unit_type="module",
                unit_attr="nsvca",
            ),
        )
        return mods_f

    def unit_criteria(self, unit_type, partial_crit):
        criteria = Criteria.and_(
            Criteria.with_unit_type(unit_type), Criteria.or_(*partial_crit)
        )
        return criteria

    def _module_search_criteria(self, module_names):
        part_crit = []

        for mod_name in module_names:
            part_crit.append(self._module_criteria(mod_name))

        return part_crit

    def _module_criteria(self, module_name):
        crit = []
        mod_nsvca_dict = self._get_nsvca_dict(module_name)
        for m_part, value in mod_nsvca_dict.items():
            crit.append(Criteria.with_field(m_part, value))
        return Criteria.and_(*crit)

    def _get_nsvca_dict(self, module_name):
        mod_parts = ["name", "stream", "version", "context", "arch"]
        nsvca = module_name.split(":", 4)
        mod_dict = {}
        for i, p in enumerate(nsvca):
            if mod_parts[i] == "version":
                mod_dict[mod_parts[i]] = int(p)
            else:
                mod_dict[mod_parts[i]] = p

        return mod_dict

    def _file_search_criteria(self, file_names):
        part_crit = []

        for file_name in file_names:
            part_crit.append(self._file_criteria(file_name))

        return part_crit

    def _file_criteria(self, file_name):
        return Criteria.with_field("path", file_name)

    def _rpm_search_criteria(self, rpms_info, signing_keys=None):
        part_crit = []
        signing_keys = (
            [s.lower() if s else None for s in signing_keys] if signing_keys else None
        )

        for rpm_info in rpms_info:
            part_crit.append(
                self._rpm_criteria(rpm_info.filename, signing_keys, rpm_info.sha256sum)
            )

        return part_crit

    def _rpm_criteria(self, filename, signing_keys=None, sha256sum=None):
        if signing_keys:
            return Criteria.and_(
                Criteria.with_field("filename", filename),
                Criteria.with_field("signing_key", Matcher.in_(signing_keys)),
            )

        if sha256sum:
            return Criteria.and_(
                Criteria.with_field("filename", filename),
                Criteria.with_field("sha256sum", sha256sum),
            )

        return Criteria.with_field("filename", filename)

    def search_content(self, criteria):
        return self.pulp_client.search_content(criteria=criteria)

    def log_missing_units(self, searched_units, unit_attr, unit_type, unit_names):
        found = []

        for unit in searched_units:
            found.append(getattr(unit, unit_attr))

        missing = set(unit_names) - set(found)
        if missing:
            missing = sorted(list(missing))
            LOG.warning(
                "Requested unit(s) don't exist as %s: %s",
                unit_type,
                ", ".join(sorted(missing)),
            )

        LOG.info("%s unit(s) found for deletion", len(found))

        return searched_units

    def map_to_repo(self, units, repos, unit_attr):
        repo_map = {}
        unit_map = {}
        repos = sorted(repos)
        for unit in sorted(units):
            unit_name = getattr(unit, unit_attr)
            unit_map.setdefault(unit_name, RemoveUnitItem(unit=unit, repos=[]))
            if ALL_REPOS_INDICATOR in repos:
                for repo in unit.repository_memberships:
                    if re.match("all-rpm-content-.*", repo) and repo not in repos:
                        continue
                    repo_map.setdefault(repo, []).append(unit)
                    unit_map.get(unit_name).repos.append(repo)
            else:
                for repo in repos:
                    if repo not in unit.repository_memberships:
                        LOG.warning(
                            "%s is not present in %s",
                            unit_name,
                            repo,
                        )
                    else:
                        repo_map.setdefault(repo, []).append(unit)
                        unit_map.get(unit_name).repos.append(repo)

        missing = set(repos) - set(repo_map.keys())
        if ALL_REPOS_INDICATOR in missing:
            missing.remove(ALL_REPOS_INDICATOR)
        if missing:
            missing = ", ".join(sorted(list(missing)))
            LOG.warning("No units to remove from %s", missing)

        return repo_map, unit_map

    @step("Unassociate RPMs")
    def remove_rpms(self, repo_map):
        return self.delete_content(RpmUnit, repo_map, self._rpm_remove_crit)

    @step("Unassociate files")
    def remove_files(self, repo_map):
        return self.delete_content(FileUnit, repo_map, self._file_remove_crit)

    @step("Unassociate modules")
    def remove_modules(self, repo_map):
        return self.delete_content(ModulemdUnit, repo_map, self._module_remove_crit)

    def delete_content(self, unit_type, repo_map, criteria_fn):
        if not repo_map:
            LOG.warning("Nothing mapped for removal")
            return []
        out = []
        # get repos
        repos = self.search_repo(repo_map.keys())

        # request removal
        for repo in sorted(repos):
            units = repo_map.get(repo.id)
            criteria = self.unit_criteria(unit_type, criteria_fn(units))
            f = repo.remove_content(criteria=criteria)
            f = f_map(f, partial(ClearedRepo, repo=repo, content=units))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    def _rpm_remove_crit(self, units):
        part_crit = []
        for unit in units:
            part_crit.append(self._rpm_criteria(unit.filename, [unit.signing_key]))

        return part_crit

    def _file_remove_crit(self, units):
        part_crit = []
        for unit in units:
            part_crit.append(self._file_criteria(unit.path))

        return part_crit

    def _module_remove_crit(self, units):
        part_crit = []
        for unit in units:
            part_crit.append(self._module_criteria(unit.nsvca))

        return part_crit

    def search_repo(self, repo_ids):
        return self.pulp_client.search_repository(Criteria.with_id(repo_ids)).result()

    def separate_files(self, filenames):
        rpms = []
        modules = []
        files = []

        for fname in filenames:
            # try to delete all the input files as file
            files.append(fname)
            # delete files as rpm only if it ends with .rpm
            if fname.endswith(".rpm"):
                rpms.append(fname)
            # delete files as module if they have valid NSVCA
            if self.is_valid_modulemd(fname):
                modules.append(fname)

        return rpms, files, modules

    def is_valid_modulemd(self, file):
        if MODULEMD_REGEX.match(file):
            return True
        return False

    def log_remove(self, removed_repo):
        # Given a repo which has been cleared, log some messages
        # summarizing the removed unit(s)
        content_types = {}

        for task in removed_repo.tasks:
            for unit in task.units:
                type_id = unit.content_type_id
                content_types[type_id] = content_types.get(type_id, 0) + 1

        task_ids = ", ".join(sorted([t.id for t in removed_repo.tasks]))
        repo_id = removed_repo.repo.id
        if content_types:
            removed_types = []
            for key in sorted(content_types.keys()):
                removed_types.append("%s %s(s)" % (content_types[key], key))
            removed_types = ", ".join(removed_types)

            LOG.info("%s: removed %s, tasks: %s", repo_id, removed_types, task_ids)

        return removed_repo

    @step("Record push items")
    def record_clears(self, cleared_repo_fs):
        return [f_flat_map(f, self.record_cleared_repo) for f in cleared_repo_fs]

    def record_cleared_repo(self, cleared_repo):
        push_items = []
        for task in cleared_repo.tasks:
            push_items.extend(self.push_items_for_task(task, cleared_repo.repo.id))
        return self.collector.update_push_items(push_items)

    def push_items_for_task(self, task, repo):
        out = []
        for unit in task.units:
            push_item = self.push_item_for_unit(unit, repo, "DELETED")
            if push_item:
                out.append(push_item)
        return out

    def push_item_for_unit(self, unit, dest, state):
        for unit_type, fn in [
            (ModulemdUnit, self.push_item_for_modulemd),
            (RpmUnit, self.push_item_for_rpm),
            (FileUnit, self.push_item_for_file),
        ]:
            if isinstance(unit, unit_type):
                return fn(unit, dest, state)

    def push_item_for_modulemd(self, unit, dest, state):
        out = {}
        out["state"] = state
        out["origin"] = "pulp"

        # Note: N:S:V:C:A format here is kept even if some part
        # of the data is missing (never expected to happen).
        # For example, if C was missing, you'll get N:S:V::A
        # so the arch part can't be misinterpreted as context.
        nsvca = ":".join(
            [unit.name, unit.stream, str(unit.version), unit.context, unit.arch]
        )

        out["name"] = nsvca
        out["dest"] = [dest]

        return ModuleMdPushItem(**out)

    def push_item_for_rpm(self, unit, dest, state):
        out = {}

        out["state"] = state
        out["origin"] = "pulp"

        filename_parts = [
            unit.name,
            "-",
            unit.version,
            "-",
            unit.release,
            ".",
            unit.arch,
            ".rpm",
        ]
        out["name"] = "".join(filename_parts)
        out["dest"] = [dest]

        # Note: in practice we don't necessarily expect to get all of these
        # attributes, as after a delete the server will only provide those
        # which make up the unit key. We still copy them anyway (even if
        # values are None) in case this is improved some day.
        out["sha256sum"] = unit.sha256sum
        out["md5sum"] = unit.md5sum
        out["signing_key"] = unit.signing_key

        return RpmPushItem(**out)

    def push_item_for_file(self, unit, dest, state):
        out = {}

        out["state"] = state
        out["origin"] = "pulp"
        out["name"] = unit.path
        out["sha256sum"] = unit.sha256sum
        out["dest"] = [dest]

        return FilePushItem(**out)


def entry_point(cls=Delete):
    with cls() as instance:
        instance.main()


def doc_parser():
    return Delete().parser
