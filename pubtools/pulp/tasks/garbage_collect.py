import logging
from datetime import datetime, timedelta

from pubtools.pulplib import Criteria, Matcher

from pubtools.pulp.task import PulpTask


LOG = logging.getLogger("garbage-collect")


class GarbageCollect(PulpTask):
    """Perform garbage collection on Pulp data.

    Garbage collection consists of deleting temporary Pulp repositories
    (created by certain tools) older than a certain age.  Future versions
    of this command may also perform other types of garbage collection.

    This command is suitable for use periodically; for example, from a weekly
    scheduled trigger.
    """

    def add_args(self):
        self.parser.add_argument(
            "--gc-threshold",
            help="delete repos older than this many days",
            type=int,
            default=5,
        )

    def run(self):
        LOG.debug("Garbage collection begins")
        criteria = Criteria.and_(
            Criteria.with_field("notes.created", Matcher.exists()),
            Criteria.with_field("notes.pub_temp_repo", True),
        )

        # fetch repos for the criteria
        repos = self.pulp_client.search_repository(criteria).result()
        LOG.debug("repos fetched")

        gc_threshold = self.args.gc_threshold
        deleted_repos = []
        # initiate deletion task for the repos
        for repo in repos.as_iter():
            repo_age = datetime.utcnow() - repo.created
            if repo_age > timedelta(days=gc_threshold):
                LOG.info("Deleting %s (created on %s)", repo.id, repo.created)
                deleted_repos.append(repo.delete())

        if not deleted_repos:
            LOG.info("No repo(s) found older than %s day(s)", gc_threshold)

        # log for error during deletion
        for task in deleted_repos:
            out = task.result()[0]
            if out.error_details or out.error_summary:
                LOG.error(out.error_details or out.error_summary)

        LOG.info("Temporary repo(s) deletion completed")


def entry_point():
    GarbageCollect().main()


def doc_parser():
    return GarbageCollect().parser
