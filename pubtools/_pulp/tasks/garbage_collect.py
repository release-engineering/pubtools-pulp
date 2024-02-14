import logging
import os
from datetime import datetime, timedelta

from pubtools.pulplib import Criteria, Matcher, RpmUnit

from pubtools._pulp.services import PulpClientService
from pubtools._pulp.task import PulpTask

LOG = logging.getLogger("pubtools.pulp")
step = PulpTask.step

UNASSOCIATE_BATCH_LIMIT = int(os.getenv("PULP_GC_UNASSOCIATE_BATCH_LIMIT", "10000"))


class GarbageCollect(PulpClientService, PulpTask):
    """Perform garbage collection on Pulp data.

    Garbage collection consists of deleting temporary Pulp repositories
    (created by certain tools) older than a certain age.  Future versions
    of this command may also perform other types of garbage collection.

    This command is suitable for use periodically; for example, from a weekly
    scheduled trigger.
    """

    def add_args(self):
        super(GarbageCollect, self).add_args()

        self.parser.add_argument(
            "--gc-threshold",
            help="delete repos older than this many days",
            type=int,
            default=5,
        )

        self.parser.add_argument(
            "--arc-threshold",
            help="delete all-rpm-content older than this many days",
            type=int,
            default=14,
        )

    def run(self):
        self.delete_temp_repos()
        self.clean_all_rpm_content()

    @step("Delete temporary repos")
    def delete_temp_repos(self):
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
        for repo in repos:
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

    @step("Clean all-rpm-content")
    def clean_all_rpm_content(self):
        # Clear out old all-rpm-content
        LOG.info("Start old all-rpm-content deletion")
        arc_threshold = self.args.arc_threshold
        clean_repos = list(
            self.pulp_client.search_repository(
                Criteria.with_field("id", "all-rpm-content")
            )
        )
        if not clean_repos:
            LOG.info("No repos found for cleaning.")
            return
        arc_repo = clean_repos[0]

        search_criteria = Criteria.and_(
            Criteria.with_unit_type(RpmUnit, unit_fields=["unit_id"]),
            Criteria.with_field(
                "cdn_published",
                Matcher.less_than(datetime.utcnow() - timedelta(days=arc_threshold)),
            ),
        )

        LOG.info("Collecting unit batches for deletion")
        units = list(arc_repo.search_content(criteria=search_criteria))
        if not units:
            LOG.info("No all-rpm-content found older than %s", arc_threshold)
            return

        unit_batches = [
            units[i : i + UNASSOCIATE_BATCH_LIMIT]
            for i in range(0, len(units), UNASSOCIATE_BATCH_LIMIT)
        ]
        for batch in unit_batches:
            LOG.info("Submitting batch for deletion")
            deletion_criteria = Criteria.and_(
                Criteria.with_unit_type(RpmUnit),
                Criteria.with_field(
                    "unit_id",
                    Matcher.in_([unit.unit_id for unit in batch]),
                ),
            )
            LOG.debug("Submitting batch for deletion")
            deletion_task = arc_repo.remove_content(criteria=deletion_criteria).result()
            for task in deletion_task:
                if task.repo_id == "all-rpm-content":
                    for unit in task.units:
                        LOG.info("Old all-rpm-content deleted: %s", unit.name)


def entry_point():
    with GarbageCollect() as instance:
        instance.main()


def doc_parser():
    return GarbageCollect().parser
