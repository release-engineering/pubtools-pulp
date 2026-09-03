import sys
import pytest

from mock import patch

from pubtools.pulplib import Client, Pulp3Client
from pubtools._pulp.task import PulpTask, task_context
from pubtools._pulp.services import PulpClientService, Pulp3ClientService


class TaskWithPulpClient(PulpClientService, PulpTask):
    pass

# intentionally inherited from TaskWithPulpClient to test that the clients are not mixed up
class TaskWithPulp3Client(TaskWithPulpClient, Pulp3ClientService): 
    pass

def test_task_run():
    """raises if run() is not implemeted"""
    task = PulpTask()
    with pytest.raises(NotImplementedError):
        task.run()


def test_init_args():
    """Checks whether the args from cli are available for the task"""
    task = TaskWithPulpClient()
    arg = ["", "--pulp-url", "http://some.url", "--debug"]
    with patch("sys.argv", arg):
        task_args = task.args

    cli_args = [
        "pulp_url",
        "pulp_user",
        "pulp_password",
        "pulp_certificate",
        "pulp_certificate_key",
        "debug",
    ]
    for a in cli_args:
        assert hasattr(task_args, a)


def test_pulp_client():
    """Checks that the client in the task is an instance of pubtools.pulplib.Client"""
    with TaskWithPulpClient() as task:
        arg = ["", "--pulp-url", "http://some.url", "--pulp-user", "user"]
        with patch("sys.argv", arg):
            client = task.pulp_client

    assert isinstance(client, Client)

def test_pulp3_client():
    """Checks that the client in the task is an instance of pubtools.pulplib.Client"""
    with TaskWithPulp3Client() as task:
        arg = ["", "--pulp3-url", "http://some.url", "--domain", "test", "--pulp3-user", "user", "--pulp3-password", "password"]
        with patch("sys.argv", arg):
            client = task.pulp3_client

    assert isinstance(client, Pulp3Client)

@pytest.mark.parametrize(
    "args_cert, args_key, expected_kwargs",
    [
        ("args_crt", "args_key", ("args_crt", "args_key")),
        ("args_pem", None, "args_pem"),
    ],
    ids=("args_crt_and_key", "args_cert_pem"),
)
@patch("pubtools.pluggy.pm.hook.get_cert_key_paths")
def test_pub_client_args_cert(mock_hook, args_cert, args_key, expected_kwargs):
    """
    Assuming certs are not passed in any way.
    Checks if certificate is used when passed as argument.
    """
    # making sure certs are not passed through hook
    mock_hook.return_value = ("does_not_exist", "does_not_exist")
    with TaskWithPulpClient() as task:
        arg = [
            "",
            "--pulp-url",
            "http://some.url",
        ]
        if args_cert:
            arg.extend(
                [
                    "--pulp-certificate",
                    str(args_cert),
                ]
            )
        if args_key:
            arg.extend(
                [
                    "--pulp-certificate-key",
                    str(args_key),
                ]
            )
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.services.pulp.pulplib.Client") as mock_client:
                with patch("pubtools._pulp.task.PulpTask.run"):
                    assert task.main() == 0
                    assert task.pulp_client

                    client_kwargs = mock_client.mock_calls[0].kwargs
                    assert client_kwargs["cert"] == expected_kwargs

@pytest.mark.parametrize(
    "args_cert_pulp, args_key_pulp, args_cert_pulp3, args_key_pulp3, expected_kwargs_pulp, expected_kwargs_pulp3",
    [
        ("args_crt", "args_key", "args_cert_pulp3", "args_key_pulp3", ("args_crt", "args_key"), ("args_cert_pulp3", "args_key_pulp3")),
        ("args_pem_pulp", None, "args_pem_pulp3", None, "args_pem_pulp", "args_pem_pulp3"),
    ],
    ids=("args_crt_and_key", "args_cert_pem"),
)
@patch("pubtools.pluggy.pm.hook.get_cert_key_paths")
def test_pub_client_args_cert_with_pulp3(mock_hook, args_cert_pulp, args_key_pulp, args_cert_pulp3, args_key_pulp3, expected_kwargs_pulp, expected_kwargs_pulp3):
    """
    Assuming certs are not passed in any way.
    Checks if certificate is used when passed as argument.
    Combination of pulp and pulp3 clients are tested.
    """
    # making sure certs are not passed through hook
    mock_hook.return_value = ("does_not_exist", "does_not_exist")
    with TaskWithPulp3Client() as task:
        arg = [
            "",
            "--pulp-url",
            "http://some.url",
            "--domain",
            "test",
            "--pulp3-url",
            "http://some.url3",
        ]
        if args_cert_pulp:
            arg.extend(
                [
                    "--pulp-certificate",
                    str(args_cert_pulp),
                    "--pulp3-cert",
                    str(args_cert_pulp3),
                ]
            )
        if args_key_pulp:
            arg.extend(
                [
                    "--pulp-certificate-key",
                    str(args_key_pulp),
                    "--pulp3-cert-key",
                    str(args_key_pulp3),
                ]
            )
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.services.pulp.pulplib.Client") as mock_client:
                with patch("pubtools._pulp.services.pulp3.pulplib.Pulp3Client") as mock_pulp3_client:
                    with patch("pubtools._pulp.task.PulpTask.run"):
                        assert task.main() == 0
                        assert task.pulp_client
                        assert task.pulp3_client

                        client_kwargs = mock_client.mock_calls[0].kwargs
                        assert client_kwargs["cert"] == expected_kwargs_pulp
                        client_kwargs_pulp3 = mock_pulp3_client.mock_calls[0].kwargs
                        assert client_kwargs_pulp3["cert"] == expected_kwargs_pulp3

@pytest.mark.parametrize(
    "hook_cert, hook_key",
    [("fake_hook_crt", "fake_hook_key"), ("fake_hook_pem", None)],
    ids=("hook_cert_crt_and_key", "hook_cert_pem"),
)
@patch("pubtools.pluggy.pm.hook.get_cert_key_paths")
def test_pub_client_hook_cert(mock_hook, tmp_path, hook_cert, hook_key):
    """
    Checks if cert is returned when the hook is used.
    Assuming password is not passed as argument.
    """
    # use tmp_path pytest fixture to create the fake hook certs
    fake_hook_crt_pem = tmp_path / hook_cert
    fake_hook_crt_pem.touch()
    fake_hook_key = tmp_path / hook_key if hook_key else None
    if fake_hook_key:
        fake_hook_key.touch()
    mock_hook.return_value = (str(fake_hook_crt_pem), str(fake_hook_key))
    with TaskWithPulpClient() as task:
        arg = [
            "",
            "--pulp-url",
            "http://some.url",
        ]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.services.pulp.pulplib.Client") as mock_client:
                with patch("pubtools._pulp.task.PulpTask.run"):
                    assert task.main() == 0
                    assert task.pulp_client

                    client_kwargs = mock_client.mock_calls[0].kwargs
                    # verify if kwargs contains the certificate file(s)
                    # with a key file present, we should get a (crt, key) tuple
                    if hook_key:
                        assert client_kwargs["cert"] == (
                            str(fake_hook_crt_pem),
                            str(fake_hook_key),
                        )
                    # without a key file present, we should only get the crt/pem file
                    else:
                        assert client_kwargs["cert"] == str(fake_hook_crt_pem)


def test_pulp_fake_client(monkeypatch, tmpdir):
    """Checks that a fake client is created if --pulp-fake is given"""

    # Ensure we use a clean home dir so the fake can't be affected by
    # any of the caller's persisted state.
    monkeypatch.setenv("HOME", str(tmpdir))

    with TaskWithPulpClient() as task:
        arg = ["", "--pulp-fake"]
        with patch("sys.argv", arg):
            with task_context():
                client = task.pulp_client

        # Fake client doesn't advertise itself in any obvious way.
        # Just do some rough checks...
        assert "Fake" in type(client).__name__

        # Should be able to use the API even though it's obviously not connected
        # to a real Pulp server
        assert "rpm" in client.get_content_type_ids().result()

        # Some repos should exist, because the fake creates a handful of repos
        # by default.
        assert list(client.search_repository().result())


def test_pulp_missing_args(caplog):
    """An error occurs if task is invoked with neither --pulp-url nor --pulp-fake."""

    with TaskWithPulpClient() as task:
        arg = [""]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                with pytest.raises(SystemExit) as excinfo:
                    task.pulp_client

    assert excinfo.value.code == 41
    assert "At least one of --pulp-url or --pulp-fake must be provided" in caplog.text

def test_pulp3_missing_args(caplog):
    """An error occurs if task is invoked with neither --pulp3-url nor --domain."""

    with TaskWithPulp3Client() as task:
        arg = ["",  "--pulp-url", "http://some.url"]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                with pytest.raises(SystemExit) as excinfo:
                    task.pulp3_client

    assert excinfo.value.code == 41
    assert "Both pulp3-url and domain must be provided" in caplog.text

@pytest.mark.parametrize(
    "args_cert, args_key, expected_creds",
    [
        ("args_crt", "args_key", ("pki", ("args_crt", "args_key"))),
        ("args_pem", None, ("pki", ("args_pem", None))),
    ],
    ids=("args_crt_and_key", "args_cert_pem"),
)
def test_pulp3_get_credentials_cert_and_key(args_cert, args_key, expected_creds):
    """Checks that the credentials are returned correctly for pulp3 service"""
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "--domain",
        "test",
        "--pulp3-url",
        "http://some.url3",
        "--pulp3-cert",
        str(args_cert),
    ]
    if args_key:
        arg.extend(
            [
                "--pulp3-cert-key",
                str(args_key),
            ]
        )
    with TaskWithPulp3Client() as task:
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                assert task.get_pulp3_credentials() == expected_creds

def test_pulp3_get_credentials_basic():
    """Checks that the credentials are returned correctly for pulp3 service"""
    arg = [
        "",
        "--pulp-url",
        "http://some.url",
        "--domain",
        "test",
        "--pulp3-url",
        "http://some.url3",
        "--pulp3-user",
        "user",
        "--pulp3-password",
        "password",
    ]
    with TaskWithPulp3Client() as task:
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                assert task.get_pulp3_credentials() == ("basic", ("user", "password"))

def test_main():
    """Checks main returns without exception when invoked with minimal args
    assuming run() and add_args() are implemented
    """
    with TaskWithPulpClient() as task:
        arg = ["", "--pulp-url", "http://some.url", "-d"]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                assert task.main() == 0

def test_main_pulp3():
    """Checks main returns without exception when invoked with minimal args
    assuming run() and add_args() are implemented, using also pulp3 service
    """
    with TaskWithPulp3Client() as task:
        arg = ["", "--pulp-url", "http://some.url", "--domain", "test", "--pulp3-url", "http://some.url3", "-d"]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                assert task.main() == 0

def test_description():
    """description is initialized from subclass docstring, de-dented."""

    class MyTask(PulpTask):
        """This is an example task subclass.

        It has a realistic multi-line doc string:

            ...and may have several levels of indent.
        """

    assert MyTask().description == (
        "This is an example task subclass.\n\n"
        "It has a realistic multi-line doc string:\n\n"
        "    ...and may have several levels of indent."
    )


@pytest.mark.parametrize(
    "throttle", [None, 8], ids=("throttle_from_env", "throttle_option")
)
def test_pulp_throttle(monkeypatch, throttle):
    """Checks main returns without exception when invoked with --pulp-throttle arg
    or PULP_THROTTLE value from environment variable, and checks whether the arg is
    correctly promoted to pulp_client.
    """
    pulp_throttle = 7
    monkeypatch.setenv("PULP_THROTTLE", str(pulp_throttle))
    with TaskWithPulpClient() as task:
        arg = [
            "",
            "--pulp-url",
            "http://some.url",
            "-d",
        ]
        if throttle:
            arg.extend(
                [
                    "--pulp-throttle",
                    str(throttle),
                ]
            )
            pulp_throttle = throttle

        monkeypatch.setattr(sys, "argv", arg)

        with patch("pubtools._pulp.services.pulp.pulplib.Client") as mock_client:
            with patch("pubtools._pulp.task.PulpTask.run"):
                assert task.main() == 0
                assert task.args.pulp_throttle == throttle

                # Should be able to create a pulp client
                assert task.pulp_client

                # The client should be created with the specified throttle
                client_kwargs = mock_client.mock_calls[0].kwargs
                assert client_kwargs["task_throttle"] == pulp_throttle


@pytest.mark.parametrize(
    "throttle, exception",
    [(None, ValueError), ("xyz", SystemExit)],
    ids=("from_env", "from_option"),
)
def test_pulp_throttle_invalid(monkeypatch, throttle, exception):
    """Checks main raises SystemExit when a non-int string is passed with --pulp-throttle
    or ValueError when PULP_THROTTLE env variable is non-it.
    """
    monkeypatch.setenv("PULP_THROTTLE", "abc")
    with TaskWithPulpClient() as task:
        arg = [
            "",
            "--pulp-url",
            "http://some.url",
            "-d",
        ]
        if throttle:
            arg.extend(["--pulp-throttle", "xyz"])
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                with pytest.raises(exception):
                    task.main()
                    assert task.pulp_client is None


def test_pulp_throttle_negative():
    """Checks main raises SystemExit when a negative int is passed with --pulp-throttle."""
    with TaskWithPulpClient() as task:
        arg = ["", "--pulp-url", "http://some.url", "-d", "--pulp-throttle", "-1"]
        with patch("sys.argv", arg):
            with patch("pubtools._pulp.task.PulpTask.run"):
                with pytest.raises(SystemExit):
                    task.main()
