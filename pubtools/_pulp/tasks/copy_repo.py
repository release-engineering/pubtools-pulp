import logging
from functools import partial

import attr
from more_executors.futures import f_map, f_sequence
from pubtools.pulplib import (ContainerImageRepository, Criteria, ErratumUnit,
                              FileUnit, ModulemdUnit, RpmUnit)

from pubtools._pulp.arguments import SplitAndExtend
from pubtools._pulp.services import CollectorService, PulpClientService
from pubtools._pulp.task import PulpTask
from pubtools._pulp.tasks.common import PulpRepositoryOperation

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


@attr.s(slots=True)
class RepoCopy(object):
    """Represents a copy of a single repo."""

    tasks = attr.ib()
    """The completed Pulp tasks for copying."""

    repo = attr.ib()
    """The repo to which content was copied."""


class CopyRepo(CollectorService, PulpClientService, PulpRepositoryOperation):
    @property
    def content_type(self):
        # Only return non-None if there were really any types given.
        # Otherwise, return None to let library defaults apply
        c = self.args.content_type or None
        # Normalize content types (e.g., "ISO" -> "iso").
        if c:
            c = [t.lower() for t in c]
        return c

    @property
    def repo_pairs(self):
        out = []
        for pair in self.args.repopairs:
            pair = list(map(str, pair.split(",")))
            if not len(pair) == 2 or any([not r_id.strip() for r_id in pair]):
                self.fail(
                    "Pair(s) must contain two repository IDs, source and destination. Got: %s",
                    pair,
                )
            out.append(pair)
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
        found_repos = []
        repo_pairs = []

        # Eagerly load all repos to fail early if the user passed any nonexistent repo.
        for id_pair in self.repo_pairs:
            # We'll need a flat list of given IDs later.
            repo_ids.extend(id_pair)

            search = self.pulp_client.search_repository(Criteria.with_id(id_pair))

            src = None
            dest = None
            for repo in search.result():
                # We'll need a flat list of all search results later.
                found_repos.append(repo)

                if repo.id == id_pair[0]:
                    src = repo
                if repo.id == id_pair[1]:
                    dest = repo

            repo_pairs.append((src, dest))

        # Bail out if user requested repos which don't exist
        missing = set(repo_ids) - {repo.id for repo in found_repos}
        missing = sorted(list(missing))
        if missing:
            self.fail("Requested repo(s) don't exist: %s", ", ".join(missing))

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
                for repo in found_repos
                if isinstance(repo, ContainerImageRepository)
            ]
        )
        if container_repo_ids:
            self.fail(
                "Container image repo(s) provided, not supported: %s"
                % ", ".join(sorted(container_repo_ids))
            )

        return repo_pairs

    @step("Copy content")
    def copy_content(self, src_repo, dest_repo):
        futures = []
        content_types = self.content_type or ["None"]
        for t in content_types:
            crit = None
            if t == "iso":
                crit = Criteria.with_unit_type(FileUnit)
            if t in ("rpm", "srpm"):
                crit = Criteria.with_unit_type(RpmUnit)
            if t == "erratum":
                crit = Criteria.with_unit_type(ErratumUnit)
            if t == "modulemd":
                crit = Criteria.with_unit_type(ModulemdUnit)

            f = self.pulp_client.copy_content(src_repo, dest_repo, criteria=crit)
            f = f_map(f, partial(RepoCopy, repo=dest_repo))
            f = f_map(f, self.log_copy)
            futures.append(f)

        return futures

    def run(self):
        # Get a list of repo pairs we'll be dealing with.
        # This is blocking so we'll fail early on missing/bad repos.
        repo_pairs = self.get_repos()

        # Start copying repos.
        repos_to_flush = []
        repo_copies_fs = []

        for pair in repo_pairs:
            repo_copies_fs.extend(self.copy_content(pair[0], pair[1]))

            # We shouldn't need to flush the source repos, just the updated dest.
            repos_to_flush.append(pair[1])

        # As copying completes, record pushitem info on what was copied.
        # We don't have to wait on this before continuing.
        to_await = self.record_push_items(repo_copies_fs, "PUSHED")

        # Don't need the repo copying tasks for anything more.
        repos_fs = [f_map(f, lambda cr: cr.repo) for f in repo_copies_fs]

        # Now move repos into the desired state:

        # They should be published.
        publish_fs = self.publish(repos_fs)

        # Wait for all repo publishes to complete before continuing.
        # Why: cache flush is what makes changes visible, and we want that to be
        # as near atomic as we can get (i.e. changes appear in every repo "at once",
        # rather than as each repo is published).
        f_sequence(publish_fs).result()

        # They should have UD cache flushed.
        to_await.extend(self.flush_ud(repos_to_flush))

        # They should have CDN cache flushed.
        to_await.extend(self.flush_cdn(repos_to_flush))

        # Now make sure we wait for everything to finish.
        for f in to_await:
            f.result()


def entry_point(cls=CopyRepo):
    with cls() as instance:
        instance.main()


def doc_parser():
    return CopyRepo().parser
