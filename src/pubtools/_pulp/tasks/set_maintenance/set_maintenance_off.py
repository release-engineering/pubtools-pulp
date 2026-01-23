import logging
import re

from pubtools.pulplib import Criteria
from pubtools._pulp.task import PulpTask

from .base import SetMaintenance

step = PulpTask.step
LOG = logging.getLogger("pubtools.pulp")


class SetMaintenanceOff(SetMaintenance):
    """Unset repositories maintenance mode."""

    @step("Adjust maintenance report")
    def adjust_maintenance_report(self, report):
        """Remove entries from maintenance report by repo ids or repo url regex or both"""
        to_remove = []
        existed_repo_ids = [entry.repo_id for entry in report.entries]

        if self.args.repo_url_regex:
            # search all repos with id existed in the report
            existed_repos = self.pulp_client.search_repository(
                Criteria.with_id(existed_repo_ids)
            ).result()
            for repo in existed_repos:
                if repo.relative_url and re.search(
                    self.args.repo_url_regex, repo.relative_url
                ):
                    to_remove.append(repo.id)
        if self.args.repo_ids:
            for repo_id in self.args.repo_ids:
                if repo_id in existed_repo_ids:
                    to_remove.append(repo_id)
                else:
                    LOG.warning("Repository %s is not in maintenance mode", repo_id)

        if to_remove:
            LOG.info("Following repositories will be removed from maintenance mode:")
            for repo_id in to_remove:
                LOG.info(" - %s", repo_id)

            report = report.remove(to_remove, owner=self.args.owner)

        return report


def entry_point(cls=SetMaintenanceOff):
    with cls() as instance:
        instance.main()


def doc_parser():
    return SetMaintenanceOff().parser
