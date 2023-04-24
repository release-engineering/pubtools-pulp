import logging
import sys
import re
from argparse import ArgumentTypeError
from datetime import datetime

from pubtools.pulplib import Criteria, Matcher

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService
from pubtools._pulp.tasks.common import Publisher

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


# Due to some false positives such as:
# E1101: Instance of 'Client' has no 'flush_repo' member (no-member)
#
# pylint: disable=no-member


def publish_date(str_date):
    for date_format in ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d']:
        try:
            return datetime.strptime(str_date, date_format)
        except ValueError:
            pass
    raise ArgumentTypeError("published-before date should be in YYYY-mm-ddTHH:MM:SSZ or YYYY-mm-dd format")



class Publish(PulpClientService, Publisher, PulpTask):
    """Publish one or more Pulp repositories to the endpoints defined by their distributors.

    This command will publish the Pulp repositories provided in the request or
    fetched using the filters(url-regex or published-before) or an intersection
    of input repositories and filters.
    """

    def add_args(self):
        super(Publish, self).add_args()

        self.add_publisher_args(self.parser)

        group = self.parser.add_argument_group(
            "Filter options",
            "Options affecting the selection of repos to be published.",
        )

        group.add_argument(
            "--repo-ids",
            help="comma separated repos to be published, can be specified multiple times",
            action="append",
            default=[],
        )
        group.add_argument(
            "--published-before",
            help="publish the repos last published before given date e.g. 2019-08-21",
            type=publish_date,
            default=None,
        )
        group.add_argument(
            "--repo-url-regex",
            help="publish repos whose repo url match",
            default=None,
            type=re.compile,
        )

    def _sanitize_repo_ids_args(self):
        repo_ids = []
        for item in self.args.repo_ids:
            repo_ids.extend(item.split(","))

        self.args.repo_ids = repo_ids

    def run(self):
        LOG.debug("Begin publishing repositories")

        # get repos applying filters
        repos = self.check_repos()

        # publish the repos found, including cache flushes
        publish_fs = self.publish_with_cache_flush(repos)

        # wait for everything to finish.
        for f in publish_fs:
            f.result()

        LOG.info("Publishing repositories completed")

    @step("Check repos")
    def check_repos(self):
        self._sanitize_repo_ids_args()
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
            crit.append(
                Criteria.with_field("relative_url", Matcher.regex(url_regex.pattern))
            )

        crit = Criteria.and_(*crit)
        return self.pulp_client.search_distributor(crit)


def entry_point(cls=Publish):
    with cls() as instance:
        instance.main()


def doc_parser():
    return Publish().parser
