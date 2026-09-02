import logging
import uuid
import anyio

from pubtools.pulplib import RpmUnit, YumRepository, YumSyncOptions
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
        pulp3_client,
        pulp3_credentials,
        pre_push,
        allow_unsigned,
        **kwargs,
    ):
        super(Sync, self).__init__(
            context, in_queue=in_queue, name="Sync from external source", **kwargs
        )

        self.pulp_client = pulp_client
        self.pulp3_client = pulp3_client
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
        non_rpm_skipped = 0
        prepush_skipped = 0

        async with self.pulp3_client:

            items_to_add = set()
            for item in self.iter_input():  # TODO use batch approach
                ## only RPM are support for sync to rshm-pulp from external source
                if item.unit_type is not RpmUnit:
                    non_rpm_skipped += 1
                    self.put_output(item)
                    continue

                if item.pulp_state in [
                    State.IN_REPOS,
                    State.PARTIAL,
                    State.NEEDS_UPDATE,
                ]:
                    synced += 1
                    self.put_output(item)
                    continue

                if self.pre_push and not item.can_pre_push:
                    # We're doing a pre-push, but this item doesn't support that.
                    prepush_skipped += 1
                    self.put_output(item)
                    continue

                items_to_add.add(item)
                syncing += 1

            if items_to_add:
                repo, dist_url = await self._create_ext_repo()

                async with anyio.create_task_group() as tg:
                    # TODO when using batches and multiple repos, use chaining of tasks
                    # with _update_ext_repo() and _publish_ext_repo() - need refactor to be able to use AnyIO's memory streams
                    tg.start_soon(
                        self._update_and_publish_repo,
                        repo["pulp_href"],
                        [item.pushsource_item.src for item in items_to_add],
                    )

                # PHASE II
                # Create repository in internal pulp to sync from ext source
                # TODO support for multiple repos
                # TODO chain this with futures
                repo = self._create_tmp_repo()
                self._sync_ext_repo(repo, dist_url)

                for item in items_to_add:
                    self.put_future_output(
                        item.with_pulp_refreshed_after_upload(self.pulp_client)
                    )

        event = {
            "type": "syncing-pulp",
            "items-present": synced,
            "items-syncing": syncing,
            "items-non-rpm-skipped": non_rpm_skipped,
        }
        messages = [
            "%s already synced" % synced,
            "%s syncing" % syncing,
            "%s non-rpm skipped" % non_rpm_skipped,
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

    async def _publish_ext_repo(self, repo_href):
        publ_task = await self.pulp3_client.create_publication(repo_href)
        return await self.pulp3_client.poll_task(publ_task)

    async def _create_ext_repo(self):
        rand_str = str(uuid.uuid4())
        pulp_labels = {
            "tmp_rhsm_pulp": "true",
        }
        repo_name = f"rhsm-pulp-sync-{rand_str}"
        distr_name = f"rhsm-pulp-sync-{rand_str}"

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

    def _create_tmp_repo(self):
        rand_str = str(uuid.uuid4())
        repo = YumRepository(
            id=f"tmp-konflux-{rand_str}",
            is_temporary=True,
        )
        repo = self.pulp_client.create_repository(repo).result()
        return repo

    def _sync_ext_repo(self, repo_rhsm_pulp, distr_path):
        result = repo_rhsm_pulp.sync(
            YumSyncOptions(feed=distr_path, **self._default_sync_args)
        ).result()
        return result
