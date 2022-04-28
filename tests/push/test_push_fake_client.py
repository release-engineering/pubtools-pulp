import sys

from pubtools._pulp.tasks.push import Push


def test_push_fake_client_for_phase(monkeypatch):
    """--pulp-fake causes Push to use a new fake client for each phase."""

    monkeypatch.setattr(sys, "argv", ["", "--pulp-fake"])

    push = Push()

    client1 = push.pulp_client_for_phase()
    client2 = push.pulp_client_for_phase()

    # It should give me two different clients
    assert client1 is not client2

    # And they both should be fakes
    assert "Fake" in str(type(client1))
    assert "Fake" in str(type(client2))
