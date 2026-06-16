import functools

import pytest

from pushsource import Source, RpmPushItem
from pubtools.pulplib import RpmUnit

from pubtools._pulp.tasks.push import entry_point
from pubtools._pulp.tasks.push.phase import constants


def test_push_rpm_duplicate_fail(
    fake_controller, fake_push, command_tester, caplog, monkeypatch
):
    """Test that push detects and fails in the case where a RPM with the same cdn_path but different checksum is pushed to Pulp in the destination repository."""
    monkeypatch.setattr(constants, "ALLOW_DUPLICATE_UNITS", False)

    client = fake_controller.client

    rpm_dest = client.get_repository("dest1").result()

    # Make this file exist.
    existing_rpm = RpmUnit(
        name="some-rpm",
        version="1.0.0",
        release="1",
        arch="noarch",
        sha256sum="db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb",
        filename="some-rpm-1.0.0-1.noarch.rpm",
        cdn_path="/content/origin/rpms/some-rpm/1.0.0/1/f21541eb/some-rpm-1.0.0-1.noarch.rpm",
        signing_key="F21541EB",
    )
    fake_controller.insert_units(rpm_dest, [existing_rpm])

    # Unit is now in dest1 repository.
    # Set up a pushsource backend which requests push of the RPM with the same cdn_path/NVR/signing key but different checksum
    # to different destination repository.
    Source.register_backend(
        "test",
        lambda: [
            RpmPushItem(
                name="some-rpm-1.0.0-1.noarch.rpm",
                dest=["dest2"],
                # different checksum but same GPG key
                sha256sum="e823456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                md5sum=32 * "a",
                src="fake/path",
                signing_key="F21541EB",
            ),
        ],
    )

    args = [
        "",
        "--source",
        "test:",
        "--pulp-url",
        "https://pulp.example.com/",
        "--allow-unsigned",
    ]

    run = functools.partial(entry_point, cls=lambda: fake_push)

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
    msg = "Duplicate RPM present in Pulp: some-rpm-1.0.0-1.noarch.rpm, sha256: db68c8a70f8383de71c107dca5fcfe53b1132186d1a6681d9ee3f4eea724fabb, cdn_path: /content/origin/rpms/some-rpm/1.0.0/1/f21541eb/some-rpm-1.0.0-1.noarch.rpm"

    assert msg in caplog.text
