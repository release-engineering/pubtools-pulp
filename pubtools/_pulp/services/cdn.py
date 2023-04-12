import threading

from pubtools._pulp.cdn import CdnClient

from .base import Service

# Because class is designed as a mix-in...
# pylint: disable=no-member


class CdnClientService(Service):
    """A service providing a CDN client.

    A client will only be available if the user provided CDN-related
    arguments to the command.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(CdnClientService, self).__init__(*args, **kwargs)

    def add_service_args(self, parser):
        super(CdnClientService, self).add_service_args(parser)

        group = parser.add_argument_group("CDN Client environment")

        group.add_argument(
            "--cdn-url",
            help=(
                "Base URL of CDN, "
                "if omitted, CDN won't be requested for special data (e.g. headers for ARLs)"
            ),
        )
        group.add_argument("--cdn-cert", help="Client certificate for CDN client")
        group.add_argument(
            "--cdn-key",
            help="Client key for CDN client",
        )
        group.add_argument(
            "--cdn-ca-cert",
            help="CA certificate for CDN",
        )
        group.add_argument(
            "--cdn-arl-template",
            help="ARL template used for flushing cache by ARL",
            nargs="*",
        )

    @property
    def cdn_client(self):
        """A CDN client used during task, instantiated on demand.

        May return None if needed arguments for CDN cache flush are not provided,
        in which case getting extra data from CDN is skipped.
        """
        with self.__lock:
            if not self.__instance:
                self.__instance = self.__get_instance()
        return self.__instance

    def __get_instance(self):
        args = self._service_args
        if not args.cdn_url:
            # disable requests made to CDN
            return None

        return CdnClient(
            url=args.cdn_url,
            cert=(args.cdn_cert, args.cdn_key),
            verify=args.cdn_ca_cert,
        )

    def __exit__(self, *exc_details):
        if self.__instance:
            self.__instance.__exit__(*exc_details)

        super(CdnClientService, self).__exit__(*exc_details)
