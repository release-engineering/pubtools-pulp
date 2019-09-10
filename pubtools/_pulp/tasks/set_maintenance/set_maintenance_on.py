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
        repo_ids = []
        if self.args.repo_ids:
            found_ids = self._ensure_repos_exist(self.args.repo_ids)
            repo_ids.extend(found_ids)

        if self.args.repo_regex:
            crit = Criteria.with_field("id", Matcher.regex(self.args.repo_regex))
            repos = self.pulp_client.search_repository(crit).result()
            repo_ids.extend([repo.id for repo in repos])

        report = report.add(repo_ids, owner=self.args.owner, message=self.args.message)
        LOG.info(
            "Setting following repos to maintenance mode: \n%s", "\n".join(repo_ids)
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
            LOG.warning(
                "Didn't find following repositories: \n%s", "\n".join(missing_ids)
            )

        return sorted(found_ids)


def entry_point(cls=SetMaintenanceOn):
    cls().main()  # pragma: no cover


def doc_parser():
    return SetMaintenanceOn().parser
