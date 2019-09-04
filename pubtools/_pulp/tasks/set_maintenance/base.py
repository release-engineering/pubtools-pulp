from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService


class SetMaintenance(PulpClientService, PulpTask):
    def add_args(self):
        super(SetMaintenance, self).add_args()

        self.parser.add_argument("--owner", help="who sets/unsets maintenance mode")

        self.parser.add_argument(
            "--repo-regex",
            help="only set repositories matched this regex to maintenance mode",
        )

        self.parser.add_argument(
            "--repo-ids",
            nargs="+",
            help="repository to be set/unset to maintenance mode",
        )
