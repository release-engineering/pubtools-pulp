import logging
from collections import namedtuple
from functools import partial
from itertools import chain

import attr
from more_executors.futures import f_map, f_sequence, f_proxy
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


@attr.s(slots=True)
class RepoCopy(object):
    """Represents a copy of a single repo."""

    tasks = attr.ib()
    """The completed Pulp tasks for copying."""

    repo = attr.ib()
    """The repo to which content was copied."""


class CopyRepo(CollectorService, PulpClientService, PulpRepositoryOperation):
    @property
    def content_type_criteria(self):
        # Only return non-None if there were really any types given.
        # Otherwise, return None to let library defaults apply
        out = None

        def str_to_content_type(content_type_id):
            out = None
            for item in CONTENT_TYPES:
                if content_type_id in item.content_type_ids:
                    out = item
                    break

            if out is None:
                self.fail("Unsupported content type: %s", content_type_id)

            return out

        if self.args.content_type:
            # replace srpm with rpm - we don't need to specify it separately and remove duplicated entries
            content_types = set(
                map(lambda x: x.replace("srpm", "rpm"), self.args.content_type)
            )
            content_types = [
                str_to_content_type(t.lower().strip()) for t in content_types
            ]
            criteria = []
            in_matcher = []  # to aggregate content types for Criteria.with_field()

            for item in sorted(content_types):
                if item.klass:
                    criteria.append(
                        Criteria.with_unit_type(item.klass, unit_fields=item.fields)
                    )
                else:
                    in_matcher.extend(item.content_type_ids)
            if in_matcher:
                criteria.append(
                    Criteria.with_field("content_type_id", Matcher.in_(in_matcher))
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

        for src_repo, dest_repo in repo_pairs:
            one_pair_copies = []
            for item in criteria or [None]:
                tasks_f = self.pulp_client.copy_content(
                    src_repo, dest_repo, criteria=item
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

        # They should have CDN cache flushed.
        to_await.extend(self.flush_cdn(repos_fs))

        # Now make sure we wait for everything to finish.
        for f in to_await:
            f.result()


def entry_point(cls=CopyRepo):
    with cls() as instance:
        instance.main()


def doc_parser():
    return CopyRepo().parser
