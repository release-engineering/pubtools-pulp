import os
import pytest

from mock import patch

from pubtools._pulp.ud import UdCacheClient
from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import UdCacheClientService


class TaskWithUdClient(UdCacheClientService, PulpTask):
    pass


def test_ud_client():
    """Checks that the client in the task is an instance of pubtools._pulp.ud.UdCacheClient"""
    with TaskWithUdClient() as task:
        arg = [
            "",
            "--udcache-url",
            "http://some.url",
            "--udcache-user",
            "user",
            "--udcache-password",
            "somepass",
        ]
        with patch("sys.argv", arg):
            client = task.udcache_client

    assert isinstance(client, UdCacheClient)


def test_password_arg_environ():
    """Checks that UD password can be passed via env. variable"""
    with patch.dict(os.environ, {"UDCACHE_PASSWORD": "somepass"}):
        with TaskWithUdClient() as task:
            arg = ["", "--udcache-url", "http://some.url", "--udcache-user", "user"]
            with patch("sys.argv", arg):
                with patch(
                    "pubtools._pulp.services.udcache.UdCacheClient"
                ) as mock_client:
                    assert task.udcache_client

                    client_kwargs = mock_client.mock_calls[0].kwargs
                    assert client_kwargs["auth"] == (
                        "user",
                        "somepass",
                    )


@pytest.mark.parametrize(
    "args_cert, args_key, expected_kwargs",
    [
        ("args_crt", "args_key", ("args_crt", "args_key")),
        ("args_pem", None, "args_pem"),
    ],
    ids=("args_crt_and_key", "args_cert_pem"),
)
def test_cert_key_args(args_cert, args_key, expected_kwargs):
    """Checks that cert/key args are properly passed"""
    with TaskWithUdClient() as task:
        arg = ["", "--udcache-url", "http://some.url"]

        if args_cert:
            arg.extend(
                [
                    "--udcache-certificate",
                    str(args_cert),
                ]
            )
        if args_key:
            arg.extend(
                [
                    "--udcache-certificate-key",
                    str(args_key),
                ]
            )

        with patch("sys.argv", arg):
            with patch("pubtools._pulp.services.udcache.UdCacheClient") as mock_client:

                assert task.udcache_client
                client_kwargs = mock_client.mock_calls[0].kwargs

                assert client_kwargs.get("auth") is None
                assert client_kwargs["cert"] == expected_kwargs


def test_cert_key_args_environ_():
    """Checks that cert/keys args can be passed via env. variables"""
    with patch.dict(
        os.environ,
        {
            "UDCACHE_CERT": "/fake/path/client.crt",
            "UDCACHE_KEY": "/fake/path/client.key",
        },
    ):
        with TaskWithUdClient() as task:
            arg = ["", "--udcache-url", "http://some.url"]
            with patch("sys.argv", arg):
                with patch(
                    "pubtools._pulp.services.udcache.UdCacheClient"
                ) as mock_client:

                    assert task.udcache_client
                    client_kwargs = mock_client.mock_calls[0].kwargs

                    assert client_kwargs.get("auth") is None
                    assert client_kwargs["cert"] == (
                        "/fake/path/client.crt",
                        "/fake/path/client.key",
                    )
