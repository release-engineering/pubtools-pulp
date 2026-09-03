from pubtools._pulp.tasks.push.phase import Sync
from mock import MagicMock


def test_sync_phase_get_creds_basic():
    """
    Checks that the default sync arguments are returned correctly for basic authentication.
    """
    sync_phase = Sync(context=MagicMock(),
     pulp_client=None,
      pulp3_client=None,
       pulp3_credentials=("basic", ("user", "password")), 
       pre_push=None, 
       allow_unsigned=True, in_queue=None, out_queue=None)

    assert sync_phase._default_sync_args == {
        "require_signature": False,
        "skip": ["erratum", "distribution"],
        "basic_auth_username": "user",
        "basic_auth_password": "password",
    }

def test_sync_phase_get_creds_pki():
    """
    Checks that the default sync arguments are returned correctly for PKI authentication.
    """
    sync_phase = Sync(context=MagicMock(),
     pulp_client=None,
      pulp3_client=None,
       pulp3_credentials=("pki", ("cert", "key")), 
       pre_push=None, 
       allow_unsigned=True, in_queue=None, out_queue=None)

    assert sync_phase._default_sync_args == {
        "require_signature": False,
        "skip": ["erratum", "distribution"],
        "ssl_client_cert": "cert",
        "ssl_client_key": "key",
    }
