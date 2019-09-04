import logging
import re

from .base import SetMaintenance


LOG = logging.getLogger("set-maintenance")


class SetMaintenanceOff(SetMaintenance):
    """Unset repositories maintenance mode.

    See "pub maintenance-on --help" for more information on maintenance mode.
    """

    def run(self):
        report = self.pulp_client.get_maintenance_report().result()

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
                    LOG.warn("Repository %s is not in Maintenance Mode", repo_id)

        LOG.info(
            "Following repositories will be removed from Maintenance Mode: \n%s",
            "\n".join(to_remove),
        )

        report = report.remove(to_remove, owner=self.args.owner)

        self.pulp_client.set_maintenance(report).result()


def entry_point():
    SetMaintenanceOff().main()


def doc_parser():
    return SetMaintenanceOff().parser
