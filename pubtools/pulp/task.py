import logging
import os
from argparse import ArgumentParser
import attr

from pubtools.pulplib import Client


LOG = logging.getLogger("pulp-task")
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"


@attr.s
class PulpTask(object):
    """Base class for Pulp CLI tasks

    Instances for PulpTask subclass may be obtained to request a Pulp
    tasks like garbage-collect, publish etc. via CLI or entrypoints.

    This class provides a CLI parser and the pulp client. Parser is
    configured with minimal options which can be extended by subclass.
    The pulp client uses the args from cli and connects to the url
    provided in the request.

    """

    parser = attr.ib(init=False, default=attr.Factory(ArgumentParser))
    """CLI parser for the task """

    _args = attr.ib(init=False, default=None)
    # internal attribute that stores the parsed args from cli

    _pulp_client = attr.ib(init=False, default=None)
    # internal attribute to store the instance of the pulp client

    @property
    def args(self):
        """Parsed args from the cli

        returns the args if avaialble from previous parse
        else parses with defined options and return the args
        """
        if not self._args:
            self._basic_args()
            self.add_args()
            self._args = self.parser.parse_args()
        return self._args

    @property
    def pulp_client(self):
        """Pulp client for the task

        returns the client if available
        else gets one using the info from parsed args
        Pulp password for the client can also be provided as
        environment variable PULP_PASSWORD
        """
        if not self._pulp_client:
            self._pulp_client = self._get_pulp_client()
        return self._pulp_client

    def _get_pulp_client(self):
        auth = None

        # checks if pulp password is available as enviornment variable
        if self.args.user:
            if not self.args.password:
                self.args.password = os.environ.get("PULP_PASSWORD")
                if not self.args.password:
                    LOG.warning("No password provided for %s", self.args.user)
            auth = (self.args.user, self.args.password)

        return Client(self.args.url, auth=auth, verify=False)

    def _basic_args(self):
        # minimum args required for a pulp CLI task

        self.parser.add_argument("--url", help="pulp server URL", required=True)
        self.parser.add_argument("--user", help="pulp user", default=None)
        self.parser.add_argument("--password", help="pulp password", default=None)
        self.parser.add_argument("--verbose", action="store_true", help="show logs")
        self.parser.add_argument(
            "--debug",
            action="store_true",
            help="show debug statements. " "Used along --verbose",
        )

    def _setup_logging(self):
        level = logging.INFO
        if self.args.debug:
            level = logging.DEBUG
        logging.basicConfig(level=level, format=LOG_FORMAT)

    def add_args(self):
        """Add parser options/arguments for a task

        e.g. self.parser.add_argument("option", help="help text")
        """
        raise NotImplementedError()

    def run(self):
        """Implement a specific task"""

        raise NotImplementedError()

    def main(self):
        """Main method called by the entrypoint of the task."""

        # setup the logging as required
        if self.args and self.args.verbose:
            self._setup_logging()

        self.run()
        return 0
