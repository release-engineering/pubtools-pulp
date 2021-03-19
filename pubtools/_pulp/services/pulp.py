import threading
import os
import logging
import warnings

from pubtools import pulplib

from .base import Service

LOG = logging.getLogger("pubtools.pulp")


def pulp_throttle(str_pulp_throttle):
    val = int(str_pulp_throttle)
    if val <= 0:
        raise ValueError
    return val


class PulpClientService(Service):
    """A service providing a Pulp client.

    If this service is inherited, Pulp-related arguments become mandatory
    in order to run the task.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(PulpClientService, self).__init__(*args, **kwargs)

    def add_service_args(self, parser):
        super(PulpClientService, self).add_service_args(parser)

        group = parser.add_argument_group("Pulp environment")
        group.add_argument("--pulp-url", help="Pulp server URL", required=True)
        group.add_argument("--pulp-user", help="Pulp username", default=None)
        group.add_argument(
            "--pulp-password",
            help="Pulp password (or set PULP_PASSWORD environment variable)",
            default=None,
        )
        group.add_argument(
            "--pulp-insecure",
            action="store_true",
            help="Allow unverified HTTPS connection to Pulp",
        )
        group.add_argument(
            "--pulp-throttle",
            help="Allows to enqueue or run only specified number of Pulp tasks at one moment",
            default=None,
            type=pulp_throttle,
        )

    @property
    def pulp_client(self):
        """A Pulp client used during task, instantiated on demand."""
        with self.__lock:
            if not self.__instance:
                self.__instance = self.__get_instance()
        return self.__instance

    def __get_instance(self):
        auth = None
        args = self._service_args

        # checks if pulp password is available as environment variable
        if args.pulp_user:
            pulp_password = args.pulp_password or os.environ.get("PULP_PASSWORD")
            if not pulp_password:
                LOG.warning("No pulp password provided for %s", args.pulp_user)
            auth = (args.pulp_user, pulp_password)

        kwargs = {"auth": auth}

        if args.pulp_insecure:
            kwargs["verify"] = False

            # Thank you, but we don't need to hear about this for every single request
            warnings.filterwarnings("once", r"Unverified HTTPS request is being made")

        if args.pulp_throttle:
            kwargs["task_throttle"] = args.pulp_throttle

        return pulplib.Client(args.pulp_url, **kwargs)
