import pytest

from pubtools._pulp.tasks.common import PulpRepositoryOperation


def test_task_run():
    """raises if run() is not implemented"""
    task = PulpRepositoryOperation()
    with pytest.raises(NotImplementedError):
        task.run()
