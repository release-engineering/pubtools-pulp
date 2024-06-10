import attr
from more_executors.futures import f_map, f_flat_map, f_sequence

from .base import PulpPushItem, State


# Let pylint know, because we override ensure_uploaded,
# we do not need to override upload_to_repo:
#
# pylint:disable=abstract-method


@attr.s(frozen=True, slots=True)
class PulpDirectUploadPushItem(PulpPushItem):
    """A specialization of PulpPushItems for content types with the following behavior:

    - we don't support checking if the content is already in Pulp
    - we upload to each destination repo individually (rather than uploading to one repo
      and associating to others)

    This tends to be used for content types which are (1) small, so it's not expensive
    to upload them repeatedly, and (2) have a single file which may map to multiple units
    when parsed by Pulp.

    Example: for modulemd YAML streams, we do not support looking "inside" of the stream
    to figure out which modules it refers to, then searching in Pulp to see what's missing
    and uploading just those. Instead we just upload the entire stream to Pulp every time
    and expect Pulp to work it out.

    This class needs to be subclassed with the `upload_to_repo` method overridden to
    implement the upload behavior associated with the specific content type.
    """

    uploaded_repos = attr.ib(type=list, default=None)
    """IDs of repo(s) we've uploaded to."""

    @property
    def unit_type(self):
        return None

    @property
    def in_pulp_repos(self):
        # The only repos we consider this to be 'in' are the repos we've uploaded to.
        return sorted(self.uploaded_repos or [])

    def ensure_uploaded(self, ctx, repo_f=None):
        # ensure_uploaded is overridden to upload to *all* destination repos rather than
        # only one.
        repo_ids = self.pushsource_item.dest

        repo_fs = [ctx.client.get_repository(repo_id) for repo_id in repo_ids]

        upload_fs = [f_flat_map(f, self.upload_to_repo) for f in repo_fs]
        all_uploaded_f = f_sequence(upload_fs)

        # Once uploaded to all repos, as long as those uploads were successful, we'll
        # simply mark ourselves as IN_REPOS without doing any Pulp queries.
        return f_map(
            all_uploaded_f,
            lambda _: attr.evolve(
                self, uploaded_repos=repo_ids, pulp_state=State.IN_REPOS
            ),
        )
