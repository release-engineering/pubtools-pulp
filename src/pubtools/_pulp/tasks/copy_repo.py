import logging
from collections import namedtuple
from functools import partial
from itertools import chain

import attr
from more_executors.futures import f_map, f_sequence, f_proxy, f_flat_map, f_return
from pubtools.pulplib import (
    ContainerImageRepository,
    Criteria,
    Matcher,
    FileUnit,
    RpmUnit,
    ErratumUnit,
    ModulemdUnit,
    ModulemdDefaultsUnit,
    YumRepoMetadataFileUnit,
)


from pubtools._pulp.arguments import SplitAndExtend
from pubtools._pulp.services import CollectorService, PulpClientService
from pubtools._pulp.task import PulpTask
from pubtools._pulp.tasks.common import PulpRepositoryOperation

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")

ContentType = namedtuple(
    "ContentType", ["content_type_ids", "klass", "fields"], defaults=[None, None]
)

_RPM_FIELDS = (
    "name",
    "version",
    "release",
    "arch",
    "sha256sum",
    "md5sum",
    "signing_key",
)
_MODULEMD_FIELDS = (
    "name",
    "stream",
    "version",
    "context",
    "arch",
)
_FILE_FIELDS = (
    "path",
    "sha256sum",
)
_MINIMAL_FIELDS = ("unit_id",)

CONTENT_TYPES = (
    ContentType(("iso",), FileUnit, _FILE_FIELDS),
    ContentType(
        (
            "rpm",
            "srpm",
        ),
        RpmUnit,
        _RPM_FIELDS,
    ),
    ContentType(("erratum",), ErratumUnit, _MINIMAL_FIELDS),
    ContentType(("modulemd",), ModulemdUnit, _MODULEMD_FIELDS),
    ContentType(("modulemd_defaults",), ModulemdDefaultsUnit, _MINIMAL_FIELDS),
    ContentType(("yum_repo_metadata_file",), YumRepoMetadataFileUnit, _MINIMAL_FIELDS),
    ContentType(("package_group",)),
    ContentType(("package_category",)),
    ContentType(("package_environment",)),
    ContentType(("package_langpacks",)),
)

RPM_CONTENT_TYPES = (
    "rpm",
    "srpm",
)

NON_RPM_CONTENT_TYPES = (
    "iso",
    "erratum",
    "modulemd",
    "modulemd_defaults",
    "yum_repo_metadata_file",
    "package_group",
    "package_category",
    "package_environment",
    "package_langpacks",
)


@attr.s(slots=True)
class RepoCopy(object):
    """Represents a copy of a single repo."""

    tasks = attr.ib()
    """The completed Pulp tasks for copying."""

    repo = attr.ib()
    """The repo to which content was copied."""


class freeze_arguments(partial):
    # execute the function only with the args and kwargs that were
    # provided while creating the object. args and kwargs provided
    # during the call are ignored.
    def __call__(self, /, *args, **keywords):
        return self.func(*self.args, **self.keywords)


class CopyRepo(CollectorService, PulpClientService, PulpRepositoryOperation):
    """
    Copy content from one repository to another.

    This command copies content from one repository to another, for multiple provided `source,destination` pairs.
    Copied content may optionally be filtered on content types.
    If the user provides a non existing repo, the command fails.
    """

    @property
    def content_type_criteria(self):
        # Only return non-None if there were really any types given.
        # Otherwise, return None to let library defaults apply
        out = None

        def str_to_content_type(content_type_id):
            for item in CONTENT_TYPES:
                if content_type_id in item.content_type_ids:
                    return item

        if self.args.content_type:
            # replace srpm with rpm - we don't need to specify it separately and remove duplicated entries
            content_types = set(
                map(
                    lambda x: x.lower().strip().replace("srpm", "rpm"),
                    self.args.content_type,
                )
            )

            # check for unsupported content types
            unsupported = content_types.difference(
                RPM_CONTENT_TYPES + NON_RPM_CONTENT_TYPES
            )
            if unsupported:
                self.fail("Unsupported content type(s): %s", ",".join(unsupported))

            rpm_content_types = [
                str_to_content_type(t) for t in content_types if t in RPM_CONTENT_TYPES
            ]
            non_rpm_content_types = [
                t for t in content_types if t in NON_RPM_CONTENT_TYPES
            ]

            # NOTE: Order of appending the criteria is critical here.
            # Non-rpm content types should be copied first as it may contain modulemd
            # content type. Modulemd units should be copied before the modular rpms,
            # so as in case of a failure or partial copy, the modular rpms aren't
            # available to the users. Hence, non-rpm content type criteria is appended
            # first in the list of criteria
            criteria = []

            # criteria for all non-rpm content types
            # unit_fields are ignored as they are small in size and the repos have
            # small unit counts for non-rpm content types
            if non_rpm_content_types:
                # type_id filter with empty list includes all the content types.
                # hence, check for the presence of non-rpm content types.
                criteria.append(
                    Criteria.with_field(
                        "content_type_id", Matcher.in_(sorted(non_rpm_content_types))
                    )
                )

            # criteria for rpm content types
            # unit_fields to keep a check on memory consumption with large rpm unit
            # counts in the repo
            for item in sorted(rpm_content_types):
                criteria.append(
                    Criteria.with_unit_type(item.klass, unit_fields=item.fields)
                )

            out = criteria
        return out

    @property
    def repo_pairs(self):
        out = set()
        for pair in self.args.repopairs:
            parsed = tuple(pair.split(","))
            out.add(parsed)

            if not len(parsed) == 2 or any([not r_id.strip() for r_id in pair]):
                self.fail(
                    "Pair(s) must contain two repository IDs, source and destination. Got: '%s'",
                    pair,
                )

        return out

    def add_args(self):
        super(CopyRepo, self).add_args()

        self.parser.add_argument(
            "--content-type",
            help="copy only content of these comma-separated type(s). e.g. --content-type=(rpm, srpm, modulemd, iso, erratum)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )
        self.parser.add_argument(
            "repopairs",
            help="repository pair(s) (source, destination) to be copied. e.g. repo-A,repo-B repo-C,repo-D",
            type=str,
            nargs="+",
        )

    @step("Check repos")
    def get_repos(self):
        # Returns all repo pairs to be operated on by this task.
        # Eagerly loads the repos so we fail early if the user passed any nonexistent
        # repo.
        repo_ids = []

        repo_ids = set(chain.from_iterable(self.repo_pairs))
        # Eagerly load all repos to fail early if the user passed any nonexistent repo.
        search = self.pulp_client.search_repository(Criteria.with_id(repo_ids))
        found_repos_map = {repo.id: repo for repo in search.result()}
        # Bail out if user requested repos which don't exist
        missing = repo_ids - set(found_repos_map)
        if missing:
            self.fail("Requested repo(s) don't exist: %s", ", ".join(sorted(missing)))

        # Bail out if we'd be processing any container image repos.
        # We don't support this now because:
        #
        # - recording push items isn't implemented yet and it's not clear
        #   how to implement it (as we traditionally used docker-image-*.tar.gz
        #   filenames from brew as push item filename, but those aren't available
        #   in pulp metadata)
        #
        # - no known use-case for copying them
        container_repo_ids = sorted(
            [
                repo.id
                for repo in found_repos_map.values()
                if isinstance(repo, ContainerImageRepository)
            ]
        )
        if container_repo_ids:
            self.fail(
                "Container image repo(s) provided, not supported: %s"
                % ", ".join(sorted(container_repo_ids))
            )

        return [
            (found_repos_map[repo_id_src], found_repos_map[repo_id_dest])
            for repo_id_src, repo_id_dest in self.repo_pairs
        ]

    @step("Copy content")
    def copy_content(self, repo_pairs):
        fts = []
        criteria = self.content_type_criteria

        def repo_copy(copy_tasks, repo):
            tasks = list(chain.from_iterable(copy_tasks))
            return RepoCopy(tasks=tasks, repo=repo)

        def repo_pair_comparator(repo_pair):
            # comapres the repo pairs of differnt types to sort them
            # e.g. comapre FileRepository to YumRepository repo pair
            src, dest = repo_pair
            return (src.type, src.id, dest.id)

        for src_repo, dest_repo in sorted(repo_pairs, key=repo_pair_comparator):
            one_pair_copies = []
            tasks_f = f_return()
            for item in criteria or [None]:
                # ensure the criterias are processed and completed/resolved in order
                # so that non-rpm copy completes before rpm copy
                tasks_f = f_flat_map(
                    tasks_f,
                    freeze_arguments(
                        self.pulp_client.copy_content,
                        src_repo,
                        dest_repo,
                        criteria=item,
                    ),
                )
                one_pair_copies.append(tasks_f)
            f = f_map(f_sequence(one_pair_copies), partial(repo_copy, repo=dest_repo))
            f = f_map(f, self.log_copy)
            fts.append(f)

        return fts

    def run(self):
        # Get a list of repo pairs we'll be dealing with.
        # This is blocking so we'll fail early on missing/bad repos.
        repo_pairs = self.get_repos()

        # Start copying repos.
        repo_copies_fs = self.copy_content(repo_pairs)

        # As copying completes, record pushitem info on what was copied.
        # We don't have to wait on this before continuing.
        to_await = self.record_push_items(repo_copies_fs, "PUSHED")

        # Don't need the repo copying tasks for anything more.
        repos_fs = [f_proxy(f_map(f, lambda cr: cr.repo)) for f in repo_copies_fs]

        # Now move repos into the desired state:
        # They should be published.
        publish_fs = self.publish(repos_fs)

        # Wait for all repo publishes to complete before continuing.
        # Why: cache flush is what makes changes visible, and we want that to be
        # as near atomic as we can get (i.e. changes appear in every repo "at once",
        # rather than as each repo is published).
        f_sequence(publish_fs).result()

        # They should have UD cache flushed.
        to_await.extend(self.flush_ud(repos_fs))

        # Now make sure we wait for everything to finish.
        for f in to_await:
            f.result()


def entry_point(cls=CopyRepo):
    with cls() as instance:
        instance.main()


def doc_parser():
    return CopyRepo().parser
