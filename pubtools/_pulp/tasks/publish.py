import logging
import os
import sys
import re
from datetime import datetime
from functools import partial
from more_executors.futures import f_map, f_sequence

from pubtools.pulplib import Criteria, Matcher, PublishOptions

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import (
    PulpClientService,
    FastPurgeClientService,
    UdCacheClientService,
)


step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


def publish_date(str_date):
    # validates publish-before date
    return datetime.strptime(str_date, "%Y-%m-%d")


class Publish(
    PulpClientService, FastPurgeClientService, UdCacheClientService, PulpTask
):
    """Publishes the pulp repositories to the endpoints defined by the distributors

    This command will publish the pulp repositories provided in the request or
    fetched using the filters(url-regex or published-before) or an intersection
    of input repos and filters.
    """

    def add_args(self):
        super(Publish, self).add_args()

        self.parser.add_argument(
            "--repos", help="list of repos to be published", nargs="+"
        )
        self.parser.add_argument(
            "--clean",
            help="attempt to remove content not in the repo",
            type=bool,
            default=None,
        )
        self.parser.add_argument(
            "--force", help="execute a full repo publish", type=bool, default=None
        )
        self.parser.add_argument(
            "--published-before",
            help="publish the repos last published before given date e.g. 2019-08-21",
            type=publish_date,
            default=None,
        )
        self.parser.add_argument(
            "--url-regex",
            help="publish repos whose repo url match",
            default=None,
            type=re.compile,
        )

    def run(self):
        to_await = []
        LOG.info("run publish")

        # get repos applying filters
        repos = self.get_repos()

        # publish the repos found
        publish_fs = self.publish(repos)

        # wait for the publish to complete before
        # flushing caches.
        f_sequence(publish_fs).result()

        # flush UD cache
        to_await.extend(self.flush_ud(repos))

        # flush CDN cache
        to_await.extend(self.flush_cdn(repos))

        # wait for everthing to finish.
        for f in to_await:
            f.result()

        LOG.info("Publishing repositories completed")

    @step("Publish")
    def publish(self, repos):
        out = []

        publish_opts = PublishOptions(force=self.args.force, clean=self.args.clean)
        for repo in repos:
            LOG.info("Publishing %s", repo.id)
            f = repo.publish(publish_opts)
            f = f_map(f, partial(self.log_publish, repo))
            out.append(f)

        return out

    def log_publish(self, repo, tasks):
        # logs publish status
        for task in tasks:
            LOG.info(
                "Publishing %s %s",
                repo.id,
                "successful" if task.succeeded else "unsuccessful",
            )

    @step("Flush UD cache")
    def flush_ud(self, repos):
        client = self.udcache_client
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return []

        out = []
        for repo in repos:
            out.append(client.flush_repo(repo.id))
            if repo.eng_product_id:
                out.append(client.flush_product(repo.eng_product_id))

        return out

    @step("Flush CDN cache")
    def flush_cdn(self, repos):
        if not self.fastpurge_client:
            LOG.info("CDN cache flush is not enabled.")
            return []

        def purge_repo(repo):
            to_flush = []
            for url in repo.mutable_urls:
                flush_url = os.path.join(
                    self.fastpurge_root_url, repo.relative_url, url
                )
                to_flush.append(flush_url)

            LOG.debug("Flush: %s", to_flush)
            flush = self.fastpurge_client.purge_by_url(to_flush)
            return f_map(flush, lambda _: repo)

        return [purge_repo(r) for r in repos if r.relative_url]

    @step("Get repos")
    def get_repos(self):
        repos = self.args.repos
        found_repo_ids = []
        out = []

        # apply the filters and get repo_ids
        repo_ids = self._filter_repos(repos)

        # get repo objects for the repo_ids
        searched_repos = self.pulp_client.search_repository(Criteria.with_id(repo_ids))
        for repo in searched_repos:
            out.append(repo)
            found_repo_ids.append(repo.id)

        # Bail out if user requested repos which don't exist
        # or there are no repos returned to publish
        missing = set(repo_ids) - set(found_repo_ids)

        missing = sorted(list(missing))
        if missing:
            self.fail("Requested repo(s) don't exist: %s", ", ".join(missing))

        if not out:
            self.fail("No repo(s) found to publish")

        return out

    def fail(self, *args, **kwargs):
        LOG.error(*args, **kwargs)
        sys.exit(30)

    def _filter_repos(self, repos):

        repos = set(repos) if repos else set()

        if self.args.published_before or self.args.url_regex:
            repo_dist_f = self._filtered_repo_distributors()
            filtered_repos = [repo_dist.repo_id for repo_dist in repo_dist_f]

            return list(repos.intersection(filtered_repos)) if repos else filtered_repos

        return list(repos)

    def _filtered_repo_distributors(self):
        published_before = self.args.published_before
        url_regex = self.args.url_regex

        # define the criteria on available filters
        if published_before and url_regex:
            crit = Criteria.and_(
                Criteria.with_field(
                    "last_publish", Matcher.less_than(published_before)
                ),
                Criteria.with_field("relative_url", Matcher.regex(url_regex)),
            )
        elif published_before:
            crit = Criteria.with_field(
                "last_publish", Matcher.less_than(published_before)
            )
        elif url_regex:
            crit = Criteria.with_field("relative_url", Matcher.regex(url_regex))

        return self.pulp_client.search_distributor(crit)


def entry_point(cls=Publish):
    cls().main()


def doc_parser():
    return Publish().parser
