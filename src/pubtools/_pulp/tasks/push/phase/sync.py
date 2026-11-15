import logging
import uuid
import anyio

from pubtools.pulplib import YumRepository, YumSyncOptions
from .base import Phase
from ..items import State

LOG = logging.getLogger("pubtools.pulp")

# possible to skip only these types due to pulp implementation details
UNIT_TYPES_SKIP = ["erratum", "distribution"]


class Sync(Phase):
    def __init__(
        self,
        context,
        in_queue,
        pulp_client,
        pulp3_client_factory,
        pulp3_credentials,
        pre_push,
        allow_unsigned,
        **kwargs,
    ):
        super(Sync, self).__init__(
            context, in_queue=in_queue, name="Sync from external source", **kwargs
        )

        self.pulp_client = pulp_client
        # The Pulp3 client is created lazily (on first sync) via this factory so
        # that pushes with no items to sync don't require any Pulp3 config.
        self._pulp3_client_factory = pulp3_client_factory
        self.pulp3_client = None
        self.pre_push = pre_push
        self._default_sync_args = self._make_default_sync_args(
            pulp3_credentials, allow_unsigned
        )

    def _make_default_sync_args(self, pulp3_credentials, allow_unsigned):
        out = {
            "require_signature": not allow_unsigned,
            "skip": UNIT_TYPES_SKIP,
        }

        creds_type, creds = pulp3_credentials
        if creds_type == "basic":
            out.update(
                {
                    "basic_auth_username": creds[0],
                    "basic_auth_password": creds[1],
                }
            )
        elif creds_type == "pki":
            out.update(
                {
                    "ssl_client_cert": creds[0],
                    "ssl_client_key": creds[1],
                }
            )

        return out

    def run(self):
        anyio.run(self._run, backend="trio")

    async def _run(self):
        # PHASE I
        # create repositories in external source for sync, populate with requested RPMs, publish
        synced = 0
        syncing = 0
        prepush_skipped = 0
        not_for_sync = 0

        items_to_add = set()
        for item in self.iter_input():  # TODO use batch approach for bigger input data
            # Whether an item is synced is determined by the item itself (i.e. by
            # the source of its content), not by any configuration. Items whose
            # content is available locally are uploaded by the Upload phase, so
            # there's nothing for us to do with them here.
            if not item.needs_sync:
                not_for_sync += 1
                self.put_output(item)
                continue

            if self.pre_push and not item.can_pre_push:
                # We're doing a pre-push, but this item doesn't support that.
                prepush_skipped += 1
                self.put_output(item)
                continue

            # units are already in pulp, no need to re-sync them
            if item.pulp_state in [
                State.IN_REPOS,
                State.PARTIAL,
                State.NEEDS_UPDATE,
            ]:
                synced += 1
                self.put_output(item)
                continue

            items_to_add.add(item)
            syncing += 1

        if items_to_add:
            self.pulp3_client = self._pulp3_client_factory()
            async with self.pulp3_client:
                repo_suffix = str(uuid.uuid4())
                repo, dist_url = await self._create_ext_repo(repo_suffix)

                async with anyio.create_task_group() as tg:
                    # TODO when using batches and multiple repos, use chaining of tasks
                    # with separated_update_ext_repo() and _publish_ext_repo()
                    # need refactor to be able to use AnyIO's memory streams
                    tg.start_soon(
                        self._update_and_publish_repo,
                        repo["pulp_href"],
                        [item.pushsource_item.src for item in items_to_add],
                    )

                # PHASE II
                # Create repository in internal pulp to sync from ext source
                # TODO support for multiple repos
                # TODO chain this with futures
                repo = self._create_tmp_repo(repo_suffix)
                self._sync_ext_repo(repo, dist_url)

                for item in items_to_add:
                    self.put_future_output(
                        item.with_pulp_refreshed_after_upload(self.pulp_client)
                    )

        event = {
            "type": "syncing-pulp",
            "items-present": synced,
            "items-syncing": syncing,
            "items-not-for-sync": not_for_sync,
        }
        messages = [
            "%s already synced" % synced,
            "%s syncing" % syncing,
            "%s not for sync" % not_for_sync,
        ]

        if self.pre_push:
            messages.append("%s skipped during pre-push" % prepush_skipped)
            event["items-prepush-skipped"] = prepush_skipped

        LOG.info("Sync items: %s", ", ".join(messages), extra={"event": event})

    async def _update_and_publish_repo(self, repo_href, to_add):
        modify_task = await self.pulp3_client.modify_repo_content(repo_href, to_add)
        await self.pulp3_client.poll_task(modify_task)
        publ_task = await self.pulp3_client.create_publication(repo_href)
        return await self.pulp3_client.poll_task(publ_task)

    async def _create_ext_repo(self, suffix):
        pulp_labels = {
            "tmp_rhsm_pulp": "true",
        }
        repo_name = f"rhsm-pulp-sync-{suffix}"
        distr_name = f"rhsm-pulp-sync-{suffix}"

        repo = await self.pulp3_client.create_repository(
            repo_name, pulp_labels=pulp_labels
        )
        task = await self.pulp3_client.create_distribution(
            repo["pulp_href"],
            name=distr_name,
            base_path=distr_name,
        )

        await self.pulp3_client.poll_task(task)
        distr = await self.pulp3_client.get_distribution(distr_name)
        return (
            repo,
            distr["base_url"],
        )

    def _create_tmp_repo(self, suffix):
        repo = YumRepository(
            id=f"tmp-konflux-{suffix}",
            is_temporary=True,
        )
        repo = self.pulp_client.create_repository(repo).result()
        return repo

    def _sync_ext_repo(self, repo_rhsm_pulp, distr_path):
        result = repo_rhsm_pulp.sync(
            YumSyncOptions(feed=distr_path, **self._default_sync_args)
        ).result()
        return result
