import logging
import os
import textwrap
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from pubtools.pulplib import Client


LOG = logging.getLogger("pulp-task")
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"


class PulpTask(object):
    """Base class for Pulp CLI tasks

    Instances for PulpTask subclass may be obtained to request a Pulp
    tasks like garbage-collect, publish etc. via CLI or entrypoints.

    This class provides a CLI parser and the pulp client. Parser is
    configured with minimal options which can be extended by subclass.
    The pulp client uses the args from cli and connects to the url
    provided in the request.
    """

    def __init__(self):
        self._args = None
        self._pulp_client = None

        self.parser = ArgumentParser(
            description=self.description, formatter_class=RawDescriptionHelpFormatter
        )
        self._basic_args()
        self.add_args()

    @property
    def description(self):
        """Description for argument parser; shows up in generated docs.

        Defaults to the class doc string with some whitespace fixes."""

        # Doc strings are typically written having the first line starting
        # without whitespace, and all other lines starting with whitespace.
        # That would be formatted oddly when copied into RST verbatim,
        # so we'll dedent all lines *except* the first.
        split = self.__doc__.splitlines(True)
        firstline = split[0]
        rest = "".join(split[1:])
        rest = textwrap.dedent(rest)
        out = "".join([firstline, rest]).strip()

        # To keep separate paragraphs, we use RawDescriptionHelpFormatter,
        # but that means we have to wrap it ourselves, so do that here.
        paragraphs = out.split("\n\n")
        chunks = ["\n".join(textwrap.wrap(p)) for p in paragraphs]
        return "\n\n".join(chunks)

    @property
    def args(self):
        """Parsed args from the cli

        returns the args if avaialble from previous parse
        else parses with defined options and return the args
        """
        if not self._args:
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
        if self.args.pulp_user:
            pulp_password = self.args.pulp_password or os.environ.get("PULP_PASSWORD")
            if not pulp_password:
                LOG.warning("No pulp password provided for %s", self.args.pulp_user)
            auth = (self.args.pulp_user, pulp_password)

        return Client(self.args.pulp_url, auth=auth)

    def _basic_args(self):
        # minimum args required for a pulp CLI task

        self.parser.add_argument("--pulp-url", help="Pulp server URL", required=True)
        self.parser.add_argument("--pulp-user", help="Pulp username", default=None)
        self.parser.add_argument(
            "--pulp-password",
            help="Pulp password (or set PULP_PASSWORD environment variable)",
            default=None,
        )
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
