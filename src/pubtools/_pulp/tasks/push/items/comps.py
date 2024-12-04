from pushsource import CompsXmlPushItem
import attr

from .base import supports_type
from .direct import PulpDirectUploadPushItem


@supports_type(CompsXmlPushItem)
@attr.s(frozen=True, slots=True)
class PulpCompsXmlPushItem(PulpDirectUploadPushItem):
    """Handler for comps.xml files which are uploaded directly to each dest repo."""

    def upload_to_repo(self, repo):
        return repo.upload_comps_xml(self.pushsource_item.content())
