import os
import logging

import yaml
import attr
from pubtools.pluggy import pm, hookimpl

from pubtools import pulplib
from pubtools.pulplib import FakeController, FileRepository, YumRepository

LOG = logging.getLogger("pubtools-pulp")


def serialize(value):
    """Serialize pulplib model objects to a form which can be stored
    in YAML and later deserialized.
    """

    if isinstance(value, list):
        return [serialize(elem) for elem in value]

    if isinstance(value, dict):
        out = {}
        for key, elem in value.items():
            out[key] = serialize(elem)
        return out

    if attr.has(type(value)):
        # We do not use the recursion feature in asdict because it
        # doesn't put enough metadata in the output for deserialization
        # to work (and attr library itself doesn't provide an inverse
        # of asdict either).
        out = attr.asdict(value, recurse=False)
        out["_class"] = type(value).__name__

        # Private attrs field which cannot be (de)serialized
        if "_client" in out:
            del out["_client"]

        for key in out.keys():
            out[key] = serialize(out[key])
        return out

    return value


def deserialize(value):
    """Inverse of 'serialize'."""

    if isinstance(value, list):
        return [deserialize(elem) for elem in value]

    if isinstance(value, dict) and "_class" not in value:
        # Plain old dict
        out = {}
        for key, elem in value.items():
            out[key] = deserialize(elem)
        return out

    if isinstance(value, dict) and "_class" in value:
        value = value.copy()

        model_class = getattr(pulplib, value.pop("_class"))
        assert attr.has(model_class)

        # Deserialize everything inside it first using the plain dict
        # logic. This is where we recurse into nested attr classes, if any.
        value = deserialize(value)

        return model_class(**value)

    return value


class PersistentFake(object):
    """Wraps pulplib fake client adding persistence of state."""

    def __init__(self, state_path):
        self.ctrl = FakeController()
        self.state_path = state_path

        # Register ourselves with pubtools so we can get the task stop hook,
        # at which point we will save our current state.
        pm.register(self)

    def load_initial(self):
        """Initial load of data into the fake, in the case where no state
        has previously been persisted.

        This will populate a hardcoded handful of repos which are expected
        to always be present in a realistically configured rhsm-pulp server.
        """
        self.ctrl.insert_repository(FileRepository(id="redhat-maintenance"))
        self.ctrl.insert_repository(FileRepository(id="all-iso-content"))
        self.ctrl.insert_repository(YumRepository(id="all-rpm-content"))

    def load(self):
        """Load data into the fake from previously serialized state (if any).

        If no state has been previously serialized, load_initial will be used
        to seed the fake with some hardcoded state.
        """

        if not os.path.exists(self.state_path):
            return self.load_initial()

        with open(self.state_path, "rt") as f:  # pylint:disable=unspecified-encoding
            raw = yaml.load(f, Loader=yaml.SafeLoader)

        repos = raw.get("repos") or []
        for repo in deserialize(repos):
            self.ctrl.insert_repository(repo)

        units = raw.get("units") or []
        for unit in deserialize(units):
            for repo_id in unit.repository_memberships:
                repo = self.ctrl.client.get_repository(repo_id).result()
                self.ctrl.insert_units(repo, [unit])

    def save(self):
        """Serialize the current state of the fake and save it to persistent storage."""

        serialized = {}

        serialized["repos"] = serialize(self.ctrl.repositories)
        serialized["repos"].sort(key=lambda repo: repo["id"])

        all_units = list(self.ctrl.client.search_content())
        serialized["units"] = serialize(all_units)
        serialized["units"].sort(key=repr)

        path = self.state_path

        state_dir = os.path.dirname(path)
        if not os.path.isdir(state_dir):
            os.makedirs(state_dir)

        with open(path, "wt") as f:  # pylint:disable=unspecified-encoding
            yaml.dump(serialized, f, Dumper=yaml.SafeDumper)

        LOG.info("Fake pulp state persisted to %s", path)

    @hookimpl
    def task_stop(self, failed):  # pylint:disable=unused-argument
        """Called when a task is ending."""
        pm.unregister(self)
        self.save()


def new_fake_client(state_path=os.path.expanduser("~/.config/pubtools-pulp/fake.yaml")):
    """Create and return a new fake Pulp client.

    On top of the fake built in to pulplib library, this adds persistent state
    stored under ~/.config/pubtools-pulp by default.

    The state is persisted in a somewhat human-accessible form; the idea is that
    you can manually view and edit the YAML to see how the commands behave.
    """
    fake = PersistentFake(state_path)
    fake.load()
    return fake.ctrl.client
