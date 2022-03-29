from pushsource import ProductIdPushItem

import rhsm.certificate
from pubtools.pulplib import FakeController, YumRepository
from pubtools._pulp.tasks.push.items import PulpProductIdPushItem
from pubtools._pulp.tasks.push.items.base import UploadContext


class FakeProduct(object):
    def __init__(self, version):
        self.version = version


class FakeCert(object):
    def __init__(self, versions):
        self.products = [FakeProduct(v) for v in versions]


def test_updates_product_versions(monkeypatch, tmpdir):
    """Uploading a productid to a repo will update the product_versions field
    on that repo and related repos.
    """

    ctrl = FakeController()

    # Set up a family of repos with various product_versions.
    repo1 = YumRepository(id="repo1")
    repo2 = YumRepository(
        id="repo2",
        arch="x86_64",
        eng_product_id=1234,
        platform_full_version="xyz",
        product_versions=["a", "b"],
    )
    repo3 = YumRepository(
        id="repo3",
        arch="x86_64",
        eng_product_id=1234,
        platform_full_version="xyz",
        product_versions=["b", "c"],
    )
    repo4 = YumRepository(
        id="repo4",
        arch="x86_64",
        eng_product_id=1234,
        platform_full_version="xyz",
        product_versions=None,
    )
    repo5 = YumRepository(
        id="repo5",
        arch="x86_64",
        eng_product_id=1234,
        platform_full_version="xyz",
        product_versions=["c", "d"],
    )
    repo6 = YumRepository(
        id="repo6",
        arch="x86_64",
        eng_product_id=1234,
        product_versions=["d", "e"],
    )
    repo7 = YumRepository(
        id="repo7",
        arch="s390x",
        product_versions=["b"],
    )
    repo8 = YumRepository(
        id="repo8",
        arch="s390x",
        product_versions=[],
    )

    ctrl.insert_repository(repo1)
    ctrl.insert_repository(repo2)
    ctrl.insert_repository(repo3)
    ctrl.insert_repository(repo4)
    ctrl.insert_repository(repo5)
    ctrl.insert_repository(repo6)
    ctrl.insert_repository(repo7)
    ctrl.insert_repository(repo8)

    # Set up cert parser to use a fake cert object (saves us having to explicitly
    # generate certs with certain values)
    monkeypatch.setattr(
        rhsm.certificate, "create_from_file", lambda _: FakeCert(["a", "d"])
    )

    # make a fake productid.
    # content doesn't matter since we patched the cert parser, it just has
    # to be an existing file.
    productid = tmpdir.join("productid")
    productid.write("")

    # make an item targeting two of the repos
    item = PulpProductIdPushItem(
        pushsource_item=ProductIdPushItem(
            name="test", src=str(productid), dest=["repo2", "repo7"]
        )
    )

    upload_ctx = UploadContext(client=ctrl.client)

    # Try uploading the item
    upload_f = item.ensure_uploaded(upload_ctx)

    # It should succeed
    uploaded = upload_f.result()

    # It should be present in the target repos
    assert uploaded.in_pulp_repos == ["repo2", "repo7"]

    # Now what we're really interested in: what side effect did that
    # have on the repos?
    # Let's use this little helper to find out
    def get_pv(repo_id):
        return ctrl.client.get_repository(repo_id).product_versions

    # nothing changed here as this repo doesn't have any matching notes
    assert get_pv("repo1") == None

    # These were all updated to insert ["a", "d"] as expected
    assert get_pv("repo2") == ["a", "b", "d"]
    assert get_pv("repo3") == ["a", "b", "c", "d"]
    assert get_pv("repo4") == ["a", "d"]
    assert get_pv("repo5") == ["a", "c", "d"]

    # This was not changed due to platform_full_version mismatch
    assert get_pv("repo6") == ["d", "e"]

    # These two were changed. Note that repos 2..5 and repos 7..8
    # have different sets of notes, showing that repo 2 found related
    # repos (3,4,5) and repo 7 found related repo 8.
    assert get_pv("repo7") == ["a", "b", "d"]
    assert get_pv("repo8") == ["a", "d"]
