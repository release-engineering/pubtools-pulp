import logging

from pubtools.pulplib import Criteria, Matcher

from .base import SetMaintenance


LOG = logging.getLogger("set-maintenance")


class SetMaintenanceOn(SetMaintenance):
    """Sets repositories into 'maintenance mode'.
    When a repository is in maintenance mode, publishing onto customer-visible locations
    should be forbidden.

    Requires 'maintenance_mode' permission.

    NOTE: Publishing is blocked both from Pub and from other commands when it's enabled.
    """

    def add_args(self):
        super(SetMaintenanceOn, self).add_args()

        self.parser.add_argument(
            "--message", help="Describes why set to maintenance mode"
        )

    def run(self):
        """Set repositories matched repo_ids and repo_regex to maintenance mode"""
        report = self.pulp_client.get_maintenance_report().result()
        repo_ids = []
        if self.args.repo_ids:
            self._ensure_repos_exist(self.args.repo_ids)
            repo_ids.extend(self.args.repo_ids)

        if self.args.repo_regex:
            crit = Criteria.with_field("id", Matcher.regex(self.args.repo_regex))
            repos = self.pulp_client.search_repository(crit).result()
            repo_ids.extend([repo.id for repo in repos.as_iter()])

        report = report.add(repo_ids, owner=self.args.owner, message=self.args.message)

        LOG.info(
            "Setting following repos to Maintenance Mode: \n%s", "\n".join(repo_ids)
        )

        self.pulp_client.set_maintenance(report).result()

    def _ensure_repos_exist(self, repo_ids):
        """Check if repositories are existed in Pulp server, if not, PulpException will
        be raised.
        """
        repo_ft = []
        for repo in repo_ids:
            repo_ft.append(self.pulp_client.get_repository(repo))

        for ft in repo_ft:
            ft.result()


def entry_point():
    SetMaintenanceOn().main()


def doc_parser():
    return SetMaintenanceOn().parser
