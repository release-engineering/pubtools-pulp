import threading
import os
import sys
import logging

from pubtools import pulplib

from .base import Service

LOG = logging.getLogger("pubtools.pulp")


class Pulp3ClientService(Service):
    """A service providing a Pulp3 client.

    If this service is inherited, Pulp-related arguments become mandatory
    in order to run the task.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.RLock()
        self.__instance = None
        self.__fake_controller = None
        super(Pulp3ClientService, self).__init__(*args, **kwargs)

    def add_service_args(self, parser):
        super(Pulp3ClientService, self).add_service_args(parser)

        group = parser.add_argument_group("Pulp3 environment")
        group.add_argument("--pulp3-url", help="Pulp3 server URL")
        group.add_argument("--domain", help="Domain name for Pulp3 server")
        group.add_argument("--pulp3-user", help="Pulp3 username", default=None)
        group.add_argument(
            "--pulp3-password",
            help="Pulp3 password (or set PULP3_PASSWORD environment variable)",
            default=None,
        )
        group.add_argument("--pulp3-cert", help="Pulp3 certificate", default=None)
        group.add_argument(
            "--pulp3-cert-key", help="Pulp3 certificate key", default=None
        )

    @property
    def pulp3_client(self):
        """A shared Pulp3 client used during task, instantiated on demand.

        Note: The client is an async context manager but we return it without
        entering the context here. The caller must properly enter/exit the context
        in their async code using 'async with' or manually calling __aenter__/__aexit__.
        """
        with self.__lock:
            if not self.__instance:
                self.__instance = self.new_pulp3_client()
        return self.__instance

    def new_pulp3_client(self):
        """Creates and returns a new Pulp3 client with appropriate config."""
        args = self._service_args
        auth = cert = None
        if not (args.pulp3_url and args.domain):
            LOG.error("Both pulp3-url and domain must be provided")
            sys.exit(41)

        # pulp-certificate provided as argument
        if args.pulp3_cert:
            LOG.info("Pulp certificate %s was provided as argument", args.pulp3_cert)
            if args.pulp3_cert_key:
                LOG.info(
                    "Pulp3 certificate key %s was provided as argument",
                    args.pulp3_cert_key,
                )
                cert = (args.pulp3_cert, args.pulp3_cert_key)
            else:
                cert = args.pulp3_cert
        # checks if pulp password is available as environment variable
        if args.pulp3_user:
            pulp3_password = args.pulp3_password or os.environ.get("PULP3_PASSWORD")
            if not pulp3_password:
                LOG.error("No pulp3 password provided for %s", args.pulp3_user)
                sys.exit(41)
            auth = (args.pulp3_user, pulp3_password)

        return pulplib.Pulp3Client(
            args.pulp3_url, domain=args.domain, auth=auth, cert=cert
        )

    def __exit__(self, *exc_details):
        # Note: The pulp3_client is an async context manager and must be
        # properly exited in async code (e.g., using 'async with').
        # We cannot call __aexit__ here as this is a synchronous method.
        # Callers are responsible for managing the async context.
        pass

    def get_pulp3_credentials(self):
        out = None, (None, None)
        if self._service_args.pulp3_user:
            out = "basic", (
                self._service_args.pulp3_user,
                self._service_args.pulp3_password or os.environ.get("PULP3_PASSWORD"),
            )

        elif self._service_args.pulp3_cert:
            out = "pki", (
                self._service_args.pulp3_cert,
                self._service_args.pulp3_cert_key,
            )
        return out
