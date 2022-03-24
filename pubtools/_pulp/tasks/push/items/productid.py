import logging
from threading import Lock

from pubtools.pulplib import Criteria
from more_executors.futures import f_flat_map, f_map, f_sequence
from pushsource import ProductIdPushItem
import attr
import rhsm.certificate

from .base import supports_type
from .direct import PulpDirectUploadPushItem

LOG = logging.getLogger("pubtools.pulp")


@attr.s(slots=True)
class RepoFinder(object):
    """A helper for looking up related repos for product_versions update,
    in a reasonably efficient manner.
    """

    client = attr.ib()
    repos = attr.ib(type=list, default=attr.Factory(list))
    searches = attr.ib(type=dict, default=attr.Factory(dict))
    lock = attr.ib(default=attr.Factory(Lock))

    def find_related(self, repo):
        """Start searching for the repos which should have product_versions updated
        after a productid has been uploaded to 'repo'.

        Returns None, but influences the return value of a later call to all_results.
        """
        arch = repo.arch
        eng_product_id = repo.eng_product_id
        platform_full_version = repo.platform_full_version
        search_key = (arch, eng_product_id, platform_full_version)

        with self.lock:
            self.repos.append(repo)
            if not self.searches.get(search_key):
                search_f = self.client.search_repository(
                    Criteria.and_(
                        Criteria.with_field("arch", arch),
                        Criteria.with_field("eng_product_id", eng_product_id),
                        Criteria.with_field(
                            "platform_full_version", platform_full_version
                        ),
                    )
                )
                self.searches[search_key] = search_f

    @property
    def all_results(self):
        """Yields repos:

        - all repos passed in to find_related, and:
        - all repos found to be "related" to those, for the purpose of
          product_versions update.

        Intended to be called once only after all find_related calls have
        been performed.
        """
        seen_ids = set()

        for repo_iterable in [self.repos] + list(self.searches.values()):
            for repo in repo_iterable:
                if repo.id not in seen_ids:
                    seen_ids.add(repo.id)
                    yield repo


@supports_type(ProductIdPushItem)
@attr.s(frozen=True, slots=True)
class PulpProductIdPushItem(PulpDirectUploadPushItem):
    """Handler for productids which are uploaded directly to each dest repo."""

    product_versions = attr.ib(type=list)
    """The product versions included in this push item's productid cert."""

    @product_versions.default
    def _product_versions_from_cert(self):
        if self.pushsource_item and self.pushsource_item.src:
            cert = rhsm.certificate.create_from_file(self.pushsource_item.src)
            versions = [p.version for p in cert.products]
            return sorted(set(versions))
        return []

    def upload_to_repo(self, repo):
        return repo.upload_metadata(self.pushsource_item.src, metadata_type="productid")

    def ensure_uploaded(self, ctx, repo_f=None):
        # Overridden to add the post-upload step of product_versions update.
        uploaded_item = super(PulpProductIdPushItem, self).ensure_uploaded(ctx, repo_f)

        return f_flat_map(
            uploaded_item, lambda item: item.ensure_product_versions_uptodate(ctx)
        )

    def ensure_product_versions_uptodate(self, ctx):
        # Ensures that the product_versions field contains all the product
        # versions from this cert, in all repos containing this productid as well
        # as any repos sharing a certain relationship.

        # First we need to figure out the repos to handle.
        # We start from the repos we're contained in.
        repo_ids = self.in_pulp_repos
        repo_fs = [ctx.client.get_repository(repo_id) for repo_id in repo_ids]

        # Then start looking up any 'related repos' for them.
        # This finder class manages the searches and avoids duplicate searches.
        finder = RepoFinder(client=ctx.client)
        find_related_fs = [f_map(repo_f, finder.find_related) for repo_f in repo_fs]

        # Once all the find_related searches have been set up, we can get the
        # iterable over all repos.
        repo_iter_f = f_map(f_sequence(find_related_fs), lambda _: finder.all_results)

        return f_flat_map(
            repo_iter_f,
            lambda repos: self.ensure_product_versions_uptodate_in_repos(ctx, repos),
        )

    def ensure_product_versions_uptodate_in_repos(self, ctx, repos):
        # Ensures that the product_versions field contains all the product
        # versions from this cert, in all repos in 'repos', which should be
        # the fully calculated set of applicable repos.
        updates = []

        def pvs_to_add(repo):
            out = []
            for pv in self.product_versions:
                if pv not in (repo.product_versions or []):
                    out.append(pv)
            return out

        for repo in repos:
            pvs = pvs_to_add(repo)
            if pvs:
                LOG.info(
                    "%s: adding product_versions %s from %s",
                    repo.id,
                    ", ".join(sorted(pvs)),
                    self.pushsource_item.src,
                )
                updates.append(
                    ctx.client.update_repository(
                        attr.evolve(
                            repo,
                            product_versions=(repo.product_versions or []) + pvs,
                        )
                    )
                )

        # once all updates have completed...
        all_updated = f_sequence(updates)

        # Just return ourselves again, as is the convention for
        # the ensure_* methods.
        return f_map(all_updated, lambda _: self)
