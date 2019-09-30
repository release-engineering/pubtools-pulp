import logging

from pubtools.pulplib import Criteria, Matcher
from pubtools._pulp.task import PulpTask
from .base import SetMaintenance


step = PulpTask.step

LOG = logging.getLogger("set-maintenance")


class SetMaintenanceOn(SetMaintenance):
    """Sets repositories into 'maintenance mode'.

    When a repository is in maintenance mode, publishing onto customer-visible locations
    should be forbidden.
    """

    def add_args(self):
        super(SetMaintenanceOn, self).add_args()

        self.parser.add_argument(
            "--message", help="Describes why set to maintenance mode"
        )

    @step("Adjust maintenance report")
    def adjust_maintenance_report(self, report):
        to_add = []
        if self.args.repo_ids:
            found_ids = self._ensure_repos_exist(self.args.repo_ids)
            to_add.extend(found_ids)

        if self.args.repo_url_regex:
            # search distributors with relative_url, get the repo id from distributors
            crit = Criteria.with_field(
                "relative_url", Matcher.regex(self.args.repo_url_regex.pattern)
            )
            dists = self.pulp_client.search_distributor(crit).result()
            to_add.extend(set([dist.repo_id for dist in dists]))

        if to_add:
            LOG.info("Setting following repos to maintenance mode:")
            for repo_id in to_add:
                LOG.info(" - %s", repo_id)

            report = report.add(
                to_add, owner=self.args.owner, message=self.args.message
            )

        return report

    def _ensure_repos_exist(self, repo_ids):
        """Checks if repositories are existed in Pulp server, if not, users will be warned and
        corresponding ids will be removed.
        """
        found_repos = self.pulp_client.search_repository(
            Criteria.with_id(repo_ids)
        ).result()
        found_ids = [r.id for r in found_repos]
        missing_ids = set(repo_ids) - set(found_ids)

        if missing_ids:
            LOG.warning("Didn't find following repositories:")
            for repo_id in missing_ids:
                LOG.warning(" - %s", repo_id)

        return sorted(found_ids)


def entry_point(cls=SetMaintenanceOn):
    cls().main()


def doc_parser():
    return SetMaintenanceOn().parser
