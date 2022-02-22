import functools

import pytest

from pushsource import Source, FilePushItem
from pubtools.pulplib import FileUnit

from pubtools._pulp.tasks.push import entry_point


def test_push_copy_fails(
    fake_controller, fake_nocopy_push, fake_state_path, command_tester, caplog
):
    """Test that push detects and fails in the case where a Pulp content copy
    claims to succeed, but doesn't put expected content in the target repo.

    While not expected to happen under normal conditions, there have historically
    been a handful of Pulp bugs or operational issues which can trigger this.
    """
    client = fake_controller.client

    iso_dest1 = client.get_repository("iso-dest1").result()
    iso_dest2 = client.get_repository("iso-dest2").result()

    # Make this file exist but not in all the desired repos.
    existing_file = FileUnit(
        path="some-file",
        sha256sum="db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb",
        size=46,
    )
    fake_controller.insert_units(iso_dest1, [existing_file])

    # Unit is now in iso-dest1.
    # Set up a pushsource backend which requests push of the same content
    # to both (iso-dest1, iso-dest2).
    Source.register_backend(
        "test",
        lambda: [
            FilePushItem(
                # Note: a real push item would have to have 'src' pointing at an
                # existing file here. It's OK to omit that if the checksum exactly
                # matches something already in Pulp.
                name="some-file",
                sha256sum="db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb",
                dest=["iso-dest1", "iso-dest2"],
            )
        ],
    )

    args = [
        "",
        "--source",
        "test:",
        "--pulp-url",
        "https://pulp.example.com/",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_nocopy_push)

    # Ask it to push.
    with pytest.raises(SystemExit) as excinfo:
        command_tester.test(
            run,
            args,
            # Can't guarantee a stable log order.
            compare_plaintext=False,
            compare_jsonl=False,
        )

    # It should have failed.
    assert excinfo.value.code == 59

    # It should tell us why it failed.
    msg = (
        "Fatal error: Pulp unit not present in repo(s) iso-dest2 "
        "after copy: FileUnit(path='some-file'"
    )
    assert msg in caplog.text
