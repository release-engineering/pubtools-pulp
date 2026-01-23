"""Supporting code for copy operations in Pulp."""

import logging
import textwrap

from pubtools.pulplib import Criteria

import attr

LOG = logging.getLogger("pubtools.pulp")


@attr.s(frozen=True, slots=True)
class CopyOperation(object):
    # Represents a single copy operation between one repo and another.
    src_repo_id = attr.ib(type=str)
    dest_repo_id = attr.ib(type=str)
    criteria = attr.ib(type=Criteria)

    def log_copy_start(self):
        LOG.info(
            "Copy %s => %s: %s", self.src_repo_id, self.dest_repo_id, self.criteria
        )

    def log_copy_done(self, task):
        if task.units:
            units_str = "\n" + "\n".join(["    - %s" % u for u in task.units])
            log_fn = LOG.info
        else:
            # It's not our responsibility to check the result of the copy, however
            # there is no known reason this should happen in non-error cases,
            # so it's worth getting load about
            units_str = "<NO UNITS COPIED!>"
            log_fn = LOG.error

        msg = (
            textwrap.dedent("""
                Copy completed: {src} => {dest}
                  Task:     {task_id}
                  Criteria: {crit}
                  Copied:   {units_str}
                """)
            .strip()
            .format(
                src=self.src_repo_id,
                dest=self.dest_repo_id,
                crit=self.criteria,
                task_id=task.id,
                units_str=units_str,
            )
        )

        log_fn(msg)


def asserting_copied_ok(item, fatal):
    """Given an item which has allegedly just been copied to all desired target repos:

    - raises if the item is still missing any repos, or...
    - returns the item if it's not missing any repos
    """
    missing_repos = item.missing_pulp_repos
    if missing_repos:
        msg = "Fatal error: Pulp unit not present in repo(s) %s after copy: %s" % (
            ", ".join(missing_repos),
            item.pulp_unit,
        )
        if fatal:
            raise RuntimeError(msg)
    return item


def asserting_all_copied_ok(items, fatal=True):
    """Like asserting_copied_ok, but for a list of items."""
    return [asserting_copied_ok(item, fatal) for item in items]
