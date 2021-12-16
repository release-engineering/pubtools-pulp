"""Supporting code for copy operations in Pulp."""
import logging
import textwrap

from pubtools.pulplib import Criteria

import attr

LOG = logging.getLogger("pubtools.pulp")


@attr.s(frozen=True)
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
            textwrap.dedent(
                """
                Copy completed: {src} => {dest}
                  Task:     {task_id}
                  Criteria: {crit}
                  Copied:   {units_str}
                """
            )
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
