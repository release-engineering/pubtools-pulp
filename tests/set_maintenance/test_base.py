import pytest
from mock import patch

from pubtools.pulplib import (
    Client,
    FakeController,
    Distributor,
    Repository,
    FileRepository,
)

from pubtools._pulp.tasks.set_maintenance.base import SetMaintenance


def test_no_implemented(command_tester):
    task_instance = SetMaintenance()

    controller = FakeController()
    controller.insert_repository(FileRepository(id="redhat-maintenance"))
    client = controller.client

    arg = [
        "test-maintenance",
        "--pulp-url",
        "http://some.url",
        "--verbose",
        "--repo-ids",
        "repo1",
    ]

    with patch("pubtools._pulp.services.PulpClientService.pulp_client", client):
        with pytest.raises(NotImplementedError):
            with patch("sys.argv", arg):
                task_instance.main()
