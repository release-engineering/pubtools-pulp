import logging
import re

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService


step = PulpTask.step
LOG = logging.getLogger("pubtools.pulp")


class SetMaintenance(PulpClientService, PulpTask):
    def add_args(self):
        super(SetMaintenance, self).add_args()

        self.parser.add_argument("--owner", help="who sets/unsets maintenance mode")

        self.parser.add_argument(
            "--repo-url-regex",
            help="adjust maintenance mode for repositories with a publish URL matching this pattern",
            type=re.compile,
        )

        self.parser.add_argument(
            "--repo-ids",
            nargs="+",
            help="repository to be set/unset to maintenance mode",
        )

    @step("Get maintenance report")
    def get_maintenance_report(self):
        return self.pulp_client.get_maintenance_report()

    @step("Adjust maintenance report")
    def adjust_maintenance_report(self, report):
        raise NotImplementedError

    @step("Set maintenance report")
    def set_maintenance(self, report):
        return self.pulp_client.set_maintenance(report)

    def run(self):
        report = self.get_maintenance_report().result()

        report = self.adjust_maintenance_report(report)

        self.set_maintenance(report).result()
