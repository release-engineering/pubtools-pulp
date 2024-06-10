import threading

# Note: *this* file is named "fastpurge_" to avoid the below import
# breaking on Python 2.x
from fastpurge import FastPurgeClient

from pubtools._pulp.arguments import from_environ
from .base import Service


# Because class is designed as a mix-in...
# pylint: disable=no-member


class FastPurgeClientService(Service):
    """A service providing a FastPurge client.

    The client will only be available if the caller provides
    at least the --fastpurge-root-url argument.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(FastPurgeClientService, self).__init__(*args, **kwargs)

    def add_service_args(self, parser):
        super(FastPurgeClientService, self).add_service_args(parser)

        group = parser.add_argument_group("Akamai FastPurge environment")

        group.add_argument(
            "--fastpurge-host", help="FastPurge hostname (xxx.purge.akamaiapis.net)"
        )
        group.add_argument("--fastpurge-client-token", help="Fast Purge client token")
        group.add_argument(
            "--fastpurge-client-secret",
            help=(
                "FastPurge client secret "
                "(or set FASTPURGE_SECRET environment variable)"
            ),
            default="",
            type=from_environ("FASTPURGE_SECRET"),
        )
        group.add_argument("--fastpurge-access-token", help="FastPurge access token")

        group.add_argument(
            "--fastpurge-root-url",
            help=(
                "Root URL of CDN for all cache purges "
                "(or set FASTPURGE_ROOT_URL environment variable). "
                "If omitted, FastPurge features are disabled."
            ),
            default="",
            type=from_environ("FASTPURGE_ROOT_URL"),
        )

    @property
    def fastpurge_root_url(self):
        """Root URL for all FastPurge cache flushes (e.g. "https://cdn.example.com/").

        May be None if not passed by user, in which case cache flushing should be skipped.
        """
        return self._service_args.fastpurge_root_url

    @property
    def fastpurge_client(self):
        """A FastPurge client used during task, instantiated on demand.

        May be None depending on command-line arguments, in which case cache flushing
        should be skipped.
        """
        with self.__lock:
            if not self.__instance:
                self.__instance = self.__get_instance()
        return self.__instance

    def __get_instance(self):
        if not self.fastpurge_root_url:
            # If no root URL is defined, we can't flush anything;
            # return None for client to indicate that flushing is disabled
            return None

        fastpurge_args = {}

        for key in ["host", "client_secret", "client_token", "access_token"]:
            arg_name = "fastpurge_" + key
            arg_value = getattr(self._service_args, arg_name)
            if arg_value:
                fastpurge_args[key] = arg_value

        # If there's any argument provided, then we pass args to the client.
        # Otherwise, we pass None and we expect ~/.edgerc to be used.
        return FastPurgeClient(auth=(fastpurge_args or None))

    def __exit__(self, *exc_details):
        if self.__instance:
            self.__instance.__exit__(*exc_details)

        super(FastPurgeClientService, self).__exit__(*exc_details)
