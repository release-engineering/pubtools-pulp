import threading

from pubtools._pulp.ud import UdCacheClient
from pubtools._pulp.arguments import from_environ

from .base import Service

# Because class is designed as a mix-in...
# pylint: disable=no-member


class UdCacheClientService(Service):
    """A service providing a UD cache flush client.

    A client will only be available if the user provided UD-related
    arguments to the command.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(UdCacheClientService, self).__init__(*args, **kwargs)

    def add_service_args(self, parser):
        super(UdCacheClientService, self).add_service_args(parser)

        group = parser.add_argument_group("Unified Downloads Cache environment")

        group.add_argument(
            "--udcache-url",
            help=(
                "Base URL of UD cache flush API; "
                "if omitted, UD cache flush features are disabled."
            ),
        )
        group.add_argument("--udcache-user", help="Username for UD cache flush")
        group.add_argument(
            "--udcache-password",
            help="Password for UD cache flush (or set UDCACHE_PASSWORD)",
            default="",
            type=from_environ("UDCACHE_PASSWORD"),
        )
        group.add_argument(
            "--udcache-certificate",
            help="Client certificate for UD cache flush (or set UDCACHE_CERT)",
            default="",
            type=from_environ("UDCACHE_CERT"),
        )
        group.add_argument(
            "--udcache-certificate-key",
            help="Client key for UD cache flush (or set UDCACHE_KEY)",
            default="",
            type=from_environ("UDCACHE_KEY"),
        )

    @property
    def udcache_client(self):
        """A UD cache client used during task, instantiated on demand.

        May return None if needed arguments for UD cache flush are not provided,
        in which case cache flush should be skipped.
        """
        with self.__lock:
            if not self.__instance:
                self.__instance = self.__get_instance()
        return self.__instance

    def __get_instance(self):
        cert = None
        auth = None
        args = self._service_args
        kwargs = {}
        if not args.udcache_url:
            # UD cache flushing will be disabled
            return None

        if args.udcache_certificate:
            if args.udcache_certificate_key:
                cert = (args.udcache_certificate, args.udcache_certificate_key)
            else:
                cert = args.udcache_certificate

        else:
            auth = (args.udcache_user, args.udcache_password)

        if cert:
            kwargs["cert"] = cert
        else:
            kwargs["auth"] = auth

        return UdCacheClient(
            args.udcache_url,
            **kwargs,
        )

    def __exit__(self, *exc_details):
        if self.__instance:
            self.__instance.__exit__(*exc_details)

        super(UdCacheClientService, self).__exit__(*exc_details)
