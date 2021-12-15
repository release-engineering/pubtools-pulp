import os

from pubtools.pluggy import task_context
from pubtools.pulplib import ModulemdDefaultsUnit, ModulemdUnit

from pubtools._pulp.services.fakepulp import new_fake_client


def test_state_persisted(tmpdir, data_path):
    """Fake client automatically saves/loads state across tasks."""
    state_path1 = str(tmpdir.join("state/pulpfake.yaml"))
    state_path2 = str(tmpdir.join("state/pulpfake-other.yaml"))

    module_file = os.path.join(data_path, "sample-modules.yaml")

    # Simulate task 1 creating some state
    with task_context():
        client = new_fake_client(state_path1)

        # It should already have a few repos since there is some default
        # state.
        repo_ids = sorted([repo.id for repo in client.search_repository()])
        assert repo_ids == ["all-iso-content", "all-rpm-content", "redhat-maintenance"]

        # Now add a bit more state.
        # We use modules here because that's one of the more complex types
        # to serialize.
        repo = client.get_repository("all-rpm-content")
        repo.upload_modules(module_file).result()

        # And sanity check the resulting units.
        units = sorted(repo.search_content(), key=repr)
        unit_keys = sorted(["%s-%s" % (u.content_type_id, u.name) for u in units])
        assert unit_keys == [
            "modulemd-avocado-vt",
            "modulemd-dwm",
            "modulemd_defaults-ant",
            "modulemd_defaults-dwm",
        ]

    # Now see if another task can see that same state.
    with task_context():
        # We'll look at two different state paths to prove that the path
        # affects the behavior.
        client1 = new_fake_client(state_path1)
        client2 = new_fake_client(state_path2)

        # In client1, the units previously persisted should be available again
        # exactly as before.
        new_units = sorted(
            client1.get_repository("all-rpm-content").search_content(), key=repr
        )
        assert units == new_units

        # client2 on the other hand has no content.
        assert [] == list(client2.get_repository("redhat-maintenance").search_content())


def test_state_clean(tmpdir):
    """Persisted state does not include default values."""
    state_path = str(tmpdir.join("pulpfake.yaml"))

    # Do no-op tasks to trigger a persistence round-trip
    with task_context():
        new_fake_client(state_path)

    with task_context():
        client = new_fake_client(state_path)

    # Now let's have a look at this repo which has gone through a save/load cycle...
    repo = client.get_repository("all-rpm-content").result()

    # It should have a value for this field
    assert repo.type == "rpm-repo"

    # But actually that's just the default value, so the fake should
    # have been smart enough to not persist it.
    persisted_raw = open(state_path, "rt").read()
    assert "rpm-repo" not in persisted_raw

    # (And just to sanity check that this kind of test makes sense, see that
    # other non-default fields are present.)
    assert "id: all-rpm-content" in persisted_raw
