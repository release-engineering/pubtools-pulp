import logging
import sys
import re
from datetime import datetime
from more_executors.futures import f_sequence

from pubtools.pulplib import Criteria, Matcher, PublishOptions

from pubtools._pulp.task import PulpTask
from pubtools._pulp.cdn_cache import CDNCache
from pubtools._pulp.services import PulpClientService, UdCacheClientService


step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


def publish_date(str_date):
    # validates publish-before date
    return datetime.strptime(str_date, "%Y-%m-%d")


class Publish(PulpClientService, UdCacheClientService, PulpTask, CDNCache):
    """Publish one or more Pulp repositories to the endpoints defined by their distributors.

    This command will publish the Pulp repositories provided in the request or
    fetched using the filters(url-regex or published-before) or an intersection
    of input repositories and filters.
    """

    def add_args(self):
        super(Publish, self).add_args()

        self.parser.add_argument(
            "--repo-ids", help="list of repos to be published", nargs="+"
        )
        self.parser.add_argument(
            "--clean",
            help="attempt to remove content not in the repo",
            action="store_true",
        )
        self.parser.add_argument(
            "--force", help="execute a full repo publish", action="store_true"
        )
        self.parser.add_argument(
            "--published-before",
            help="publish the repos last published before given date e.g. 2019-08-21",
            type=publish_date,
            default=None,
        )
        self.parser.add_argument(
            "--repo-url-regex",
            help="publish repos whose repo url match",
            default=None,
            type=re.compile,
        )

    def run(self):
        to_await = []
        LOG.debug("Begin publishing repositories")

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

        # wait for everything to finish.
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
            out.append(f)

        return out

    @step("Flush UD cache")
    def flush_ud(self, repos):
        client = self.udcache_client
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return []

        out = []
        for repo in repos:
            out.append(client.flush_repo(repo.id))

        return out

    @step("Check repos")
    def get_repos(self):
        repo_ids = self.args.repo_ids
        found_repo_ids = []
        out = []

        # apply the filters and get repo_ids
        repo_ids = self._filter_repos(repo_ids)

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

        if self.args.published_before or self.args.repo_url_regex:
            repo_dist_f = self._filtered_repo_distributors()
            filtered_repos = [repo_dist.repo_id for repo_dist in repo_dist_f]

            return list(repos.intersection(filtered_repos)) if repos else filtered_repos

        return list(repos)

    def _filtered_repo_distributors(self):
        published_before = self.args.published_before
        url_regex = self.args.repo_url_regex

        # define the criteria on available filters
        crit = [Criteria.true()]
        if published_before:
            crit.append(
                Criteria.with_field("last_publish", Matcher.less_than(published_before))
            )
        if url_regex:
            crit.append(Criteria.with_field("relative_url", Matcher.regex(url_regex.pattern)))

        crit = Criteria.and_(*crit)
        return self.pulp_client.search_distributor(crit)


def entry_point(cls=Publish):
    cls().main()


def doc_parser():
    return Publish().parser
