import functools
import os
from unittest import mock

import pytest
import attr

from pushsource import Source, FilePushItem
from pubtools.pulplib import FileUnit

from pubtools._pulp.tasks.push import entry_point
from pubtools._pulp.tasks.push.phase.associate import Associate
from .util import hide_unit_ids


def test_association_race(
    fake_controller,
    data_path,
    fake_push,
    fake_state_path,
    command_tester,
    stub_collector,
    caplog,
):
    """
    We check that even if some items are moved from one repo to another during
    the association phase, they still get correctly copied into all desired
    repos.
    """

    # Sanity check that the Pulp server is, initially, empty.
    client = fake_controller.client
    assert list(client.search_content()) == []

    # Set it up to find content from our staging dir, which contains a mixture
    # of just about every content type
    stagedir = os.path.join(data_path, "staged-mixed")

    # Modify the constructor for Associate phase so that it changes the
    # repository of one rpm item from in_queue (walrus rpm) to one that does
    # not contain it
    old_iter = Associate.iter_for_associate

    def new_iter(self):
        for batch in old_iter(self):
            # find walrus rpm in batch
            indices = [
                x[0]
                for x in enumerate(batch)
                if getattr(x[1], "rpm_nvr", ()) == ("walrus", "5.21", "1")
            ]
            # modify (each) walrus package, so that it is presumed to be in
            # an incorrect repository
            for index in indices:
                walrus_item = batch[index]
                batch[index] = attr.evolve(
                    walrus_item,
                    pulp_unit=attr.evolve(
                        walrus_item.pulp_unit, repository_memberships=["dest1"]
                    ),
                )
            yield batch

    with mock.patch(
        "pubtools._pulp.tasks.push.phase.Associate.iter_for_associate", new_iter
    ):
        compare_extra = {
            "pulp.yaml": {
                "filename": fake_state_path,
                "normalize": hide_unit_ids,
            }
        }
        args = [
            "",
            "--skip",
            "publish",
            "--source",
            "staged:%s" % stagedir,
            "--allow-unsigned",
            "--pulp-url",
            "https://pulp.example.com/",
        ]

        run = functools.partial(entry_point, cls=lambda: fake_push)

        # It should be able to run without crashing.
        command_tester.test(
            run,
            args,
            compare_plaintext=False,
            compare_jsonl=False,
            # This will ensure the Pulp state matches the baseline.
            compare_extra=compare_extra,
        )

    # We can determine that publish didn't occur by checking all
    # encountered states of push items.
    all_states = set([item["state"] for item in stub_collector])

    # Everything should be either PENDING (before upload to Pulp)
    # or EXISTS (after upload), but nothing should be PUSHED since
    # publish didn't happen.
    assert all_states == set(["PENDING", "EXISTS"])

    # Assert there was exactly one retry of association.
    msg = "Retrying association for 1 item(s). Attempt 1/2"
    assert msg in caplog.text

    msg = "Retrying association for 1 item(s). Attempt 2/2"
    assert msg not in caplog.text
