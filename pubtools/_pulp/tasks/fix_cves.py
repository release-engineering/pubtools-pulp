import sys
import os
import logging
import random
import attr

from pubtools.pulplib import Criteria, ErratumUnit, ErratumReference

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import PulpClientService
from pubtools._pulp.tasks.common import Publisher
from pubtools._pulp.arguments import SplitAndExtend
from pubtools._pulp.tasks.push.items.erratum_conv import bump_erratum_version

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


class FixCves(PulpClientService, Publisher, PulpTask):
    """Command to fix cves"""

    def __init__(self, *args, **kwargs):
        self._random = random.Random(
            float(os.getenv("PUBTOOLS_SEED") or random.random())
        )
        super(FixCves, self).__init__(*args, **kwargs)

    def fail(self, *args, **kwargs):
        LOG.error(*args, **kwargs)
        sys.exit(30)

    @step("Get erratum")
    def get_erratum(self, advisory_id):
        erratum_page = self.get_erratum_from_pulp(advisory_id)
        erratum = [ert for ert in erratum_page]
        if not erratum:
            self.fail("No erratum found for %s", advisory_id)
        # multiple erratums should not exist
        assert len(erratum) == 1

        return erratum[0]

    def get_erratum_from_pulp(self, advisory_id):
        crit = Criteria.and_(
            Criteria.with_unit_type(ErratumUnit), Criteria.with_id(advisory_id)
        )
        return self.pulp_client.search_content(criteria=crit).result()

    @step("Process erratum")
    def process_erratum_for_upload(self, erratum, cves):
        # get non-cve erratum references
        refs = [ref for ref in erratum.references if ref.type != "cve"]

        # add new cves to erratum
        for cve in cves:
            refs.append(
                ErratumReference(
                    type="cve",
                    title=cve,
                    id=cve,
                    href="https://www.redhat.com/security/data/cve/%s.html" % cve,
                )
            )

        # updated version
        version = bump_erratum_version(erratum)

        # update erratum
        # clear repository memberships. It will be evaluated by Pulp.
        erratum = attr.evolve(
            erratum, references=refs, repository_memberships=None, version=str(version)
        )

        LOG.debug("Processed erratum: %s", attr.asdict(erratum))

        return erratum

    @step("Upload erratum")
    def upload_erratum(self, erratum, repos):
        # find repo to upload
        repo = self.get_upload_repo(repos)

        # upload erratum to the repo
        repo.upload_erratum(erratum).result()
        LOG.info("Uploaded to %s", repo.id)

    def get_repo_from_pulp(self, repo_id):
        crit = Criteria.with_id(repo_id)
        resp = self.pulp_client.search_repository(criteria=crit)

        return [r for r in resp.result()]

    def get_upload_repo(self, repos):
        return self._random.choice(repos)

    @step("Get affected repos")
    def get_affected_repos(self, erratum):
        # get repos listed in repository memberships
        repo_ids = erratum.repository_memberships

        repos = self.get_repo_from_pulp(repo_ids)
        LOG.info("Affected repos: %s", ", ".join(list(repo_ids)))

        return repos

    @step("Compare CVEs")
    def same_cves(self, erratum, cves):
        # get cves from erratum
        erratum_cves = [ref.id for ref in erratum.references if ref.type == "cve"]
        LOG.info("CVEs from erratum: %s", ", ".join(sorted(erratum_cves)))

        cves = [cve_id.upper() for cve_id in cves]
        LOG.info("CVEs for fix: %s", ", ".join(sorted(cves)))

        # compare with new cves
        if set(erratum_cves) == set(cves):
            LOG.info(
                "New CVEs are same as the ones on the advisory. Nothing to update."
            )
            return True

        return False

    def add_args(self):
        super(FixCves, self).add_args()

        self.add_publisher_args(self.parser)

        self.parser.add_argument(
            "--advisory", help="advisory to fix. e.g. --advisory RHXA-1234:56", type=str
        )

        self.parser.add_argument(
            "--cves",
            help="full list of desired CVEs for the advisory must be provided "
            "with both existing and the new ones. Current list of CVEs will "
            "be overwritten by the provided list. e.g. --cves CVE-987,CVE-456 "
            "or --cves CVE-987 --cves CVE-456",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

    def run(self):
        advisory_id = self.args.advisory
        if not advisory_id:
            self.fail("No advisory provided. Use --advisory to provide an advisory ID")
        cves = self.args.cves

        # get erratum/advisory from pulp
        erratum = self.get_erratum(advisory_id)

        # get repo memberships from erratum
        repos = self.get_affected_repos(erratum)

        # compare new cves with erratum cves
        if not self.same_cves(erratum, cves):
            # process erratum
            erratum = self.process_erratum_for_upload(erratum, cves)

            # upload new erratum
            self.upload_erratum(erratum, repos)

        # Publish repos
        self.publish_with_cache_flush(repos)


def entry_point(cls=FixCves):
    with cls() as instance:
        instance.main()


def doc_parser():
    return FixCves().parser
