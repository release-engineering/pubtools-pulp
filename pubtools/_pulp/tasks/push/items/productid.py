from pushsource import ProductIdPushItem
import attr

from .base import supports_type
from .direct import PulpDirectUploadPushItem


@supports_type(ProductIdPushItem)
@attr.s(frozen=True, slots=True)
class PulpProductIdPushItem(PulpDirectUploadPushItem):
    """Handler for productids which are uploaded directly to each dest repo."""

    def upload_to_repo(self, repo):
        return repo.upload_metadata(self.pushsource_item.src, metadata_type="productid")
