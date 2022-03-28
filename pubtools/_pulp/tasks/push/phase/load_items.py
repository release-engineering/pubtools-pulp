import logging
import attr

from pushsource import Source

from .base import Phase
from ..items import PulpPushItem


LOG = logging.getLogger("pubtools.pulp")


class LoadPushItems(Phase):
    """Phase for loading push items.

    This should be the very first phase, as it is responsible for discovering
    the content which should be operated on by all the later phases.

    Input queue:
    - none.

    Output queue:
    - items which should be pushed by this task. The items might be missing
      checksums and will definitely be missing any info regarding the Pulp state.

    Side-effects:
    - populates items_known, items_count on the context.
    """

    def __init__(self, context, source_urls, allow_unsigned, **_):
        super(LoadPushItems, self).__init__(context, name="Load push items")
        self._source_urls = source_urls
        self._allow_unsigned = allow_unsigned

    def check_signed(self, item):
        if item.supports_signing and not item.is_signed and not self._allow_unsigned:
            raise RuntimeError(
                "Unsigned content is not permitted: %s" % item.pushsource_item.src
            )

    @property
    def raw_items(self):
        for source_url in self._source_urls:
            with Source.get(source_url) as source:
                LOG.info("Loading items from %s", source_url)
                for item in source:
                    yield item

    @property
    def filtered_items(self):
        for item in self.raw_items:
            # The destination can possibly contain a mix of Pulp repo IDs
            # and absolute paths. Paths occur at least in the Errata Tool
            # case, as used for FTP push.
            #
            # In this command we only want to deal with repo IDs, so we'll
            # filter out the rest.
            dest = [val for val in item.dest if "/" not in val]

            # Note, dest could now be empty, but if so we still yield the
            # item because an upload of item with no dest still makes sense
            # at least in the pre-push case.
            yield attr.evolve(item, dest=dest)

    def run(self):
        count = 0

        for item in self.filtered_items:
            pulp_item = PulpPushItem.for_item(item)
            if pulp_item:
                self.check_signed(pulp_item)
                self.put_output(pulp_item)
                count += 1
            else:
                LOG.info("Skipping unsupported type: %s", item)

        # We know exactly how many items we're dealing with now.
        # Set this on the context, which allows for more accurate progress
        # info.
        self.context.items_count = count
        self.context.items_known.set()
