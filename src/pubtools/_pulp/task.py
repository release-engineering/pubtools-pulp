import logging
import textwrap
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from pubtools.pluggy import task_context

from .step import StepDecorator, UNSET

LOG = logging.getLogger("pubtools.pulp")
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
        super(PulpTask, self).__init__()

        self._args = None

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
        split = (self.__doc__ or "<undocumented task>").splitlines(True)
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

    @classmethod
    def step(cls, name, depends_on=None, skipped_value=UNSET):
        """A decorator to mark an instance method as a discrete workflow step.

        Marking a method as a step has effects:

        - Log messages will be produced when entering and leaving the method
        - The method can be skipped if requested by the caller (via --skip argument)
        - Methods that depend on other methods are implicitly skipped if depends_on
          is supplied and that dependant is being skipped
        - If the method is skipped, it returns either `skipped_value` (if that has
          been set), or the method's first argument.

        Steps may be written as plain blocking functions, as non-blocking
        functions which accept or return Futures, or as generators.
        When futures are accepted or returned, a single Future or a list of
        Futures may be used.

        When Futures are used, the following semantics apply:

        - The step is considered *started* once *any* of the input futures has finished
        - The step is considered *failed* once *any* of the output futures has failed
        - The step is considered *finished* once *all* of the output futures have finished

        When generators are used, the following semantics apply:

        - The step is considered *started* once the input generator has yielded at least
          one item, or has completed; or, immediately if the input is not a generator.
        - The step is considered *failed* if it raised an exception.
        - The step is considered *finished* once all items have been yielded.
        """
        return StepDecorator(name, depends_on, skipped_value)

    def _basic_args(self):
        # minimum args required for a pulp CLI task
        self.parser.add_argument(
            "--debug",
            "-d",
            action="count",
            default=0,
            help=(
                "Show debug logs; can be provided up to three times "
                "to enable more logs"
            ),
        )

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

        # All loggers will now log at INFO or higher.
        # If we were given --debug, enable DEBUG level from some loggers,
        # depending on how many were given.
        debug_loggers = []
        if self.args.debug >= 1:
            # debug level 1: enable DEBUG from this project
            debug_loggers.append("pubtools.pulp")
        if self.args.debug >= 2:
            # debug level 2: enable DEBUG from closely related projects.
            debug_loggers.extend(["pubtools"])
        if self.args.debug >= 3:
            # debug level 3: enable DEBUG from root logger
            # (potentially very, very verbose!)
            debug_loggers.append(None)

        for logger_name in debug_loggers:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)

    def add_args(self):
        """Add parser options/arguments for a task

        e.g. self.parser.add_argument("option", help="help text")
        """
        # Calling super add_args if it exists allows this class and
        # Service classes to be inherited in either order without breaking.
        from_super = getattr(super(PulpTask, self), "add_args", lambda: None)
        from_super()

    def run(self):
        """Implement a specific task"""

        raise NotImplementedError()

    def main(self):
        """Main method called by the entrypoint of the task."""

        with task_context():
            # setup the logging as required
            self._setup_logging()

            self.run()
            return 0

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        pass
