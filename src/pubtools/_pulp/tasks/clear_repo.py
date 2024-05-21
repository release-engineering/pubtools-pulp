import logging
from functools import partial

import attr
from more_executors.futures import f_map, f_sequence
from pubtools.pulplib import ContainerImageRepository, Criteria

from pubtools._pulp.arguments import SplitAndExtend
from pubtools._pulp.services import CollectorService, PulpClientService
from pubtools._pulp.task import PulpTask
from pubtools._pulp.tasks.common import PulpRepositoryOperation

step = PulpTask.step


LOG = logging.getLogger("pubtools.pulp")


# Due to some false positives such as:
# E1101: Instance of 'CollectorProxy' has no 'search_repository' member (no-member)
# Confused by multiple inheritance in the Service classes?
#
# pylint: disable=no-member


@attr.s(slots=True)
class ClearedRepo(object):
    """Represents a single repo which has been cleared."""

    tasks = attr.ib()
    """The completed Pulp tasks for clearing this repo."""

    repo = attr.ib()
    """The repo which was cleared."""


class ClearRepo(CollectorService, PulpClientService, PulpRepositoryOperation):
    """Remove all contents from one or more Pulp repositories.

    This command will remove contents from repositories and record
    information on what was removed.  Removal may optionally be
    filtered to selected content types.
    """

    @property
    def content_type(self):
        # Only return non-None if there were really any types given.
        # Otherwise, return None to let library defaults apply
        return self.args.content_type or None

    def add_args(self):
        super(ClearRepo, self).add_args()

        self.parser.add_argument(
            "--content-type",
            help="remove only content of these comma-separated type(s). e.g. --content-type=(rpm, srpm, modulemd, iso, modulemd_defaults, package_langpacks, or erratum and so on)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )
        self.parser.add_argument("repo", nargs="+", help="Repositories to be cleared")

    @step("Check repos")
    def get_repos(self):
        # Returns all repos to be operated on by this task.
        # Eagerly loads the repos so we fail early if the user passed any nonexistent
        # repo.
        repo_ids = self.args.repo
        found_repo_ids = []

        out = []
        search = self.pulp_client.search_repository(Criteria.with_id(repo_ids))
        for repo in search.result():
            out.append(repo)
            found_repo_ids.append(repo.id)

        # Bail out if user requested repos which don't exist
        missing = set(repo_ids) - set(found_repo_ids)

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
        # - no known use-case for clearing them
        #
        container_repo_ids = sorted(
            [repo.id for repo in out if isinstance(repo, ContainerImageRepository)]
        )
        if container_repo_ids:
            self.fail(
                "Container image repo(s) provided, not supported: %s"
                % ", ".join(sorted(container_repo_ids))
            )

        return out

    @step("Clear content")
    def clear_content(self, repos):
        out = []

        for repo in repos:
            f = repo.remove_content(type_ids=self.content_type)
            f = f_map(f, partial(ClearedRepo, repo=repo))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    def run(self):
        # Get the repos we'll be dealing with.
        # This is blocking so we'll fail early on missing/bad repos.
        repos = self.get_repos()

        # Start clearing repos.
        cleared_repos_fs = self.clear_content(repos)

        # As clearing completes, record pushitem info on what was removed.
        # We don't have to wait on this before continuing.
        to_await = self.record_push_items(cleared_repos_fs, "DELETED")

        # Don't need the repo clearing tasks for anything more.
        repos_fs = [f_map(f, lambda cr: cr.repo) for f in cleared_repos_fs]

        # Now move repos into the desired state:

        # They should be published.
        publish_fs = self.publish(repos_fs, clean=True)

        # Wait for all repo publishes to complete before continuing.
        # Why: cache flush is what makes changes visible, and we want that to be
        # as near atomic as we can get (i.e. changes appear in every repo "at once",
        # rather than as each repo is published).
        f_sequence(publish_fs).result()

        # They should have UD cache flushed.
        to_await.extend(self.flush_ud(repos))

        # They should have CDN cache flushed.
        to_await.extend(self.flush_cdn(repos))

        # Now make sure we wait for everything to finish.
        for f in to_await:
            f.result()


def entry_point(cls=ClearRepo):
    with cls() as instance:
        instance.main()


def doc_parser():
    return ClearRepo().parser
