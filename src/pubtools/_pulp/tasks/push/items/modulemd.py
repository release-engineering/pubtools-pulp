from pushsource import ModuleMdPushItem
import attr

from .base import supports_type
from .direct import PulpDirectUploadPushItem


@supports_type(ModuleMdPushItem)
@attr.s(frozen=True, slots=True)
class PulpModuleMdPushItem(PulpDirectUploadPushItem):
    """Handler for modulemd YAML files which are uploaded directly to each dest repo."""

    def upload_to_repo(self, repo):
        return repo.upload_modules(self.pushsource_item.content())
