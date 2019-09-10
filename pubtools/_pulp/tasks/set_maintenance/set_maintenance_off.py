import logging
import re

from .base import SetMaintenance
from pubtools._pulp.task import PulpTask


step = PulpTask.step
LOG = logging.getLogger("set-maintenance")


class SetMaintenanceOff(SetMaintenance):
    """Unset repositories maintenance mode."""

    @step("Adjust maintenance report")
    def adjust_maintenance_report(self, report):
        to_remove = []
        if self.args.repo_regex:
            for entry in report.entries:
                if re.match(self.args.repo_regex, entry.repo_id):
                    to_remove.append(entry.repo_id)
        if self.args.repo_ids:
            existed_repo_ids = [entry.repo_id for entry in report.entries]
            for repo_id in self.args.repo_ids:

                if repo_id in existed_repo_ids:
                    to_remove.append(repo_id)
                else:
                    LOG.warning("Repository %s is not in maintenance mode", repo_id)
        LOG.info(
            "Following repositories will be removed from maintenance mode: \n%s",
            "\n".join(to_remove),
        )
        report = report.remove(to_remove, owner=self.args.owner)

        return report


def entry_point(cls=SetMaintenanceOff):
    cls().main()  # pragma: no cover


def doc_parser():
    return SetMaintenanceOff().parser
