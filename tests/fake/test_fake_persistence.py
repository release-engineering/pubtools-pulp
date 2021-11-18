import os

from pubtools.pluggy import task_context
from pubtools.pulplib import ModulemdDefaultsUnit, ModulemdUnit

from pubtools._pulp.services.fakepulp import new_fake_client


def test_state_persisted(tmpdir, data_path):
    """Fake client automatically saves/loads state across tasks."""
    state_path1 = str(tmpdir.join("pulpfake.yaml"))
    state_path2 = str(tmpdir.join("pulpfake-other.yaml"))

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

        # And check the resulting units.
        units = list(repo.search_content())
        units.sort(key=lambda u: (u.content_type_id, u.name))
        assert units == [
            ModulemdUnit(
                name="avocado-vt",
                stream="82lts",
                version=3420210902113311,
                context="035be0ad",
                arch="x86_64",
                content_type_id="modulemd",
                repository_memberships=["all-rpm-content"],
                artifacts=[
                    "avocado-vt-0:82.0-3.module_f34+12808+b491ffc8.src",
                    "python3-avocado-vt-0:82.0-3.module_f34+12808+b491ffc8.noarch",
                ],
                profiles={
                    "default": {
                        "description": "Common profile installing the avocado-vt plugin.",
                        "rpms": ["python3-avocado-vt"],
                    }
                },
            ),
            ModulemdUnit(
                name="dwm",
                stream="6.0",
                version=3420210201213909,
                context="058368ca",
                arch="x86_64",
                content_type_id="modulemd",
                repository_memberships=["all-rpm-content"],
                artifacts=[
                    "dwm-0:6.0-1.module_f34+11150+aec78cf8.src",
                    "dwm-0:6.0-1.module_f34+11150+aec78cf8.x86_64",
                    "dwm-debuginfo-0:6.0-1.module_f34+11150+aec78cf8.x86_64",
                    "dwm-debugsource-0:6.0-1.module_f34+11150+aec78cf8.x86_64",
                    "dwm-user-0:6.0-1.module_f34+11150+aec78cf8.x86_64",
                ],
                profiles={
                    "default": {
                        "description": "The minimal, distribution-compiled dwm binary.",
                        "rpms": ["dwm"],
                    },
                    "user": {
                        "description": "Includes distribution-compiled dwm as well as a helper script to apply user patches and configuration, dwm-user.",
                        "rpms": ["dwm", "dwm-user"],
                    },
                },
            ),
            ModulemdDefaultsUnit(
                name="ant",
                repo_id="all-rpm-content",
                stream="1.10",
                profiles={"1.10": ["default"]},
                content_type_id="modulemd_defaults",
                repository_memberships=["all-rpm-content"],
            ),
            ModulemdDefaultsUnit(
                name="dwm",
                repo_id="all-rpm-content",
                stream=None,
                profiles={
                    "6.0": ["default"],
                    "6.1": ["default"],
                    "6.2": ["default"],
                    "latest": ["default"],
                },
                content_type_id="modulemd_defaults",
                repository_memberships=["all-rpm-content"],
            ),
        ]

    # Now see if another task can see that same state.
    with task_context():
        # We'll look at two different state paths to prove that the path
        # affects the behavior.
        client1 = new_fake_client(state_path1)
        client2 = new_fake_client(state_path2)

        # In client1, the units previously persisted should be available again
        # exactly as before.
        new_units = list(client1.get_repository("all-rpm-content").search_content())
        new_units.sort(key=lambda u: (u.content_type_id, u.name))
        assert units == new_units

        # client2 on the other hand has no content.
        assert [] == list(client2.get_repository("redhat-maintenance").search_content())
