"""Various constants referenced throughout the code (sentinels, tunables, etc.)"""

import os
from collections import namedtuple


def atom(name):
    # Helper to make an object suitable for use as a sentinel or flag.
    #
    # The point is just to get an object which has a nicer repr than just
    # object(), in case it shows up in backtraces, but is still of a type
    # unlikely to be abused for the wrong thing as a regular string could
    # be.
    return namedtuple(name, [])()


FINISHED = atom("FINISHED")
"""Sentinel representing normal completion of a phase.

If this object is received on an input queue, it means that there will be no
more items arriving in the queue.
"""

# Flags controlling when a phase is considered to have started.

STARTUP_TYPE_QUEUE = atom("STARTUP_TYPE_QUEUE")
"""A phase using this startup type is considered started as soon as any item
is received on the phase's input queue.
"""

STARTUP_TYPE_NOTIFY = atom("STARTUP_TYPE_NOTIFY")
"""A phase using this startup type must explicitly invoke Phase.notify_started
in order to be considered as started.
"""

DEFAULT_STARTUP_TYPE = STARTUP_TYPE_QUEUE
"""Default startup type for phases."""


# Flags controlling how progress is tracked.

PROGRESS_TYPE_QUEUE = atom("PROGRESS_TYPE_QUEUE")
"""A phase using this progress type will have a progress_info object automatically
updated as queue items are read or written. It is permitted for the phase to also
manually adjust its own progress.
"""

PROGRESS_TYPE_NONE = atom("PROGRESS_TYPE_NONE")
"""A phase using this progress type does not track progress at all.
"""

DEFAULT_PROGRESS_TYPE = PROGRESS_TYPE_QUEUE
"""Default progress type for phases."""


PHASE_TIMEOUT = int(os.getenv("PUBTOOLS_PULP_PHASE_TIMEOUT") or "200000")
"""How long, in seconds, we're willing to wait while joining phase threads.

Should be a large value. In fact, the code should strictly speaking not
require a timeout at all. We mainly set a timeout for reasons:

- mitigate the risk of coding errors which lead to e.g. a deadlock

- on python2, if you don't specify *some* timeout for APIs like thread.join,
  the process will do an uninterruptible sleep and cannot be woken even by
  SIGINT or SIGTERM. Fixed on python3; supplying any arbitrary timeout value
  on py2 works around it.
"""

QUEUE_SIZE = int(os.getenv("PUBTOOLS_PULP_QUEUE_SIZE") or "10")
"""The default max size of each phase's item queue.

Since each item in the queue is itself a potentially large batch of items,
this value should be fairly low.

It may need tuning per the following:
- if too small, pushes will slow down as phases won't be pipelined as much
- if too large, memory usage may be too high on pushes with large numbers
  of items as queues fill up
"""

# The following refer to *input* batching.

BATCH_SIZE = int(os.getenv("PUBTOOLS_PULP_BATCH_SIZE") or "1000")
"""Desired batch size for phases operating on batches.

Generally should be the max number of items we're willing to fetch in a single
Pulp query.
"""

BATCH_TIMEOUT = float(os.getenv("PUBTOOLS_PULP_BATCH_TIMEOUT") or "0.1")
"""
Minimum for how long, in seconds, we're willing to wait for more items to fill
up a batch before proceeding with what we have.
"""

BATCH_MAX_TIMEOUT = float(os.getenv("PUBTOOLS_PULP_BATCH_MAX_TIMEOUT") or "60.0")
"""Maximum counterpart to BATCH_TIMEOUT.

Actual timeout will fall between BATCH_TIMEOUT..BATCH_MAX_TIMEOUT depending on
the state of the phase's output queue.
"""


# The following refer to *output* batching.

OUT_BATCH_SIZE = int(os.getenv("PUBTOOLS_PULP_OUT_BATCH_SIZE") or "100")
"""Desired output batch size for phases."""

OUT_BATCH_TIMEOUT = float(os.getenv("PUBTOOLS_PULP_OUT_BATCH_TIMEOUT") or "10.0")
"""Max time to wait before sending an output batch."""

OUT_MAX_FUTURES = int(os.getenv("PUBTOOLS_PULP_OUT_MAX_FUTURES") or "10")
"""Max number of pending futures in output buffer."""
