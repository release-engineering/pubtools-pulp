import logging
import os
from functools import partial
from threading import Thread

from more_executors.futures import f_map, f_and, f_return
from monotonic import monotonic
from six.moves.queue import Empty


LOG = logging.getLogger("pubtools.pulp")

# How long, in seconds, we're willing to wait while joining phase threads.
#
# Should be a large value. In fact, the code should strictly speaking not
# require a timeout at all. We mainly set a timeout for reasons:
#
# - mitigate the risk of coding errors which lead to e.g. a deadlock
#
# - on python2, if you don't specify *some* timeout for APIs like thread.join,
#   the process will do an uninterruptible sleep and cannot be woken even by
#   SIGINT or SIGTERM. Fixed on python3; supplying any arbitrary timeout value
#   on py2 works around it.
PHASE_TIMEOUT = int(os.getenv("PUBTOOLS_PULP_PHASE_TIMEOUT") or "200000")

# Max number of items processed in a batch, for phases designed to use batching.
# Generally should be the max number of items we're willing to fetch in a single
# Pulp query.
BATCH_SIZE = int(os.getenv("PUBTOOLS_PULP_BATCH_SIZE") or "1000")

# How long, in seconds, we're willing to wait for more items to fill up a batch
# before proceeding with what we have.
BATCH_TIMEOUT = float(os.getenv("PUBTOOLS_PULP_BATCH_TIMEOUT") or "0.1")


class PhaseInterrupted(RuntimeError):
    """The exception raised when a phase needs to stop because an earlier phase
    has encountered a fatal error."""


class Phase(object):
    """Represents a 'phase' (discrete portion of business logic) making up a part
    of the push workflow.

    Each phase runs on its own thread. Generally, it will read items on an input
    queue, perform some processing, maybe perform some side-effects and send
    items on an output queue. One phase's output queue tends to be the next phase's
    input queue.

    Phases are used as context managers. The phase starts a thread when the context
    is entered and awaits for completion when the context is exited.

    This class must be subclassed. The inheriting class must override the run()
    method to implement the desired behavior for that phase.
    """

    FINISHED = object()
    """Sentinel representing normal completion of a phase.

    If this object is received on the input queue, it means that there will be no
    more items arriving in the queue.
    """

    ERROR = object()
    """Sentinel representing abnormal completion of a phase.

    If this object is received on the input queue, it means that a fatal error has
    occurred and the phase should stop processing ASAP (e.g. by raising
    PhaseInterrupted).
    """

    def __init__(self, context, in_queue=None, out_queue=True, name="<unknown phase>"):
        """Construct a new phase.

        Arguments:

            context
                A Context object.

            in_queue
                A Queue which will receive inputs for this phase. It can be None
                for phases which don't receive inputs in this way (e.g. the very
                first phase in a push).

            out_queue
                A Queue to which this phase will send outputs. By default, a new
                queue is created. None should be passed for phases which don't
                produce outputs (e.g. the very last phase).

            name
                A human-readable name for the phase, used for logging.

        Note: when subclassing Phase, it is recommended to accept unlimited
        keyword arguments (ignoring unknown arguments). This allows all phases
        to be constructed in a consistent manner without the caller having to
        know the arguments associated with each phase.
        """
        self.in_queue = in_queue
        self.out_queue = context.new_queue() if out_queue is True else out_queue
        self.name = name
        self.context = context

        # If we have an in_queue, name it after ourselves
        if in_queue:
            in_queue.name = name

        self.__thread = None
        self.__started = False

        # This string is used in logs produced if the phase is interrupted.
        # By default, if this happens we simply say the phase is "interrupted"
        # with no further details. However, when the phase interrupts itself
        # it can update this string to "failed" (or something else) to indicate
        # that the interruption comes from within rather than another source.
        self.__interrupt_reason = "interrupted"

        # future representing the completion of all delayed puts on
        # the output queue (if any). Must be awaited at the end of the phase.
        self.__future_puts = f_return(True)

    def run(self):
        """The business logic for this phase.

        Subclasses must override this to implement each phase's desired logic.
        This should generally consistent of reading from the phase's input queue
        and writing to the output queue.
        """
        raise NotImplementedError()  # pragma: no cover

    def iter_input(self):
        """Get an iterable over this phase's input queue, one item at a time.

        Stops iteration if the queue receives FINISHED (and does not yield that value).

        Raises if the queue receives ERROR.

        It is a bug to call this method on a phase with no input queue.
        """
        for items in self.iter_input_batched(batch_size=1):
            yield items[0]

    def iter_input_batched(self, batch_size=BATCH_SIZE, batch_timeout=BATCH_TIMEOUT):
        """Get an iterable over this phase's input queue, yielding items in batches
        of the specified size.

        Stops iteration if the queue receives FINISHED (and does not yield that value,
        but yields the batch leading up to it).

        Raises if the queue receives ERROR.

        It is a bug to call this method on a phase with no input queue.
        """
        while True:
            this_batch = []

            start_time = monotonic()

            def batch_ready():
                return (
                    (len(this_batch) >= batch_size)
                    or (monotonic() - start_time > batch_timeout)
                    or (this_batch[-1] in (Phase.FINISHED, Phase.ERROR))
                )

            this_batch.append(self.__get_input())

            while not batch_ready():
                try:
                    this_batch.append(self.__get_input(timeout=batch_timeout))
                except Empty:
                    # batch_ready() will now be true
                    pass

            stop = False
            if this_batch[-1] is Phase.FINISHED:
                stop = True
                this_batch.pop(-1)

            if this_batch:
                yield this_batch

            if stop:
                # all done
                return

    def put_output(self, value, task_done=True):
        """Put a value onto this phase's output queue.

        If task_done is True, also calls task_done on the phase's input queue.
        This is appropriate for the common case where each item on the input
        queue is expected to generate exactly one corresponding item on the
        output queue.

        It is a bug to call this method on a phase with no output queue.
        """
        self.out_queue.put(value, block=True, timeout=PHASE_TIMEOUT)
        if task_done and self.in_queue:
            self.in_queue.task_done()

    def put_future_output(self, value, task_done=True):
        """Like put_output, but the given value should be a future.

        Calling this method has the following effects:

        - The given future will have a callback added such that:
          - when resolved, the future's value will be put onto the output
            queue (as if put_output were called)
          - when failed, the context is set to an error state (causing all
            phases to be interrupted, including this one if possible)
        - When this phase ends, it will first wait for all futures submitted
          via this function to be resolved.

        It's recommended to tune each phase's batching so that this is called
        no more than 100,000 times in a single phase (and use put_future_outputs
        instead where possible). Beyond that, scaling issues may occur.
        """

        # Reuse put_future_outputs wrapping the value as a single-element list.
        self.put_future_outputs(f_map(value, lambda x: [x]), task_done)

    def put_future_outputs(self, values, task_done=True):
        """Like put_future_output, but works with a list of values rather
        than a single value.

        It is more efficient to call this function with a list of size N
        than to call put_future_output N times.
        """

        f = f_map(
            values,
            partial(self.__future_output_done, task_done=task_done),
            error_fn=self.__future_output_failed,
        )
        self.__future_puts = f_and(self.__future_puts, f)

    def __future_output_done(self, values, task_done):
        # Called when a Future[list[value]] has been resolved successfully.
        # Returns True so that the resulting future can be used with f_and
        # to indicate success.
        for value in values:
            self.put_output(value, task_done=task_done)
        return True

    def __future_output_failed(self, exception):
        # Called when a Future[item] has been resolved unsuccessfully.

        # Mark that this phase is being interrupted due to a failure within
        # the phase itself (and not due to an earlier phase).
        self.__interrupt_reason = "failed"

        # The responsibility for logging the exception is here as we can't
        # ensure anyone else will catch it if we re-raise.
        try:
            # raising to immediately catch for py2, which does not
            # support passing the exception directly to log record's exc_info
            raise exception
        except Exception:  # pylint: disable=broad-except
            LOG.exception("%s: fatal error occurred", self.name)

        # Immediately set context into error state to interrupt all phases
        # (including potentially ourselves) as early as possible.
        self.context.set_error()

        # set_error above will already interrupt this phase if we're still
        # reading the input queue. Raise an explicit interruption as well to
        # cover the scenario where we're not reading the input queue.
        raise PhaseInterrupted()

    def __get_input(self, timeout=PHASE_TIMEOUT):
        # Get a single item from input queue; this is currently private
        # as it seems like the inheritors ought to always use one of the
        # iterable forms.
        assert self.in_queue, "BUG: phase has no input queue"

        out = self.in_queue.get(block=True, timeout=timeout)

        if not self.__started:
            self.__started = True
            self.__log_start()

        # We need to stop immediately if ERROR arrived in the queue
        # OR if the context is in the error state. The latter case
        # avoids us continuing to work on our queue for a long time after
        # we already know there's a fatal error.
        if out is Phase.ERROR or self.context.has_error:
            raise PhaseInterrupted("Stopping %s due to error" % self.name)

        return out

    @property
    def __machine_name(self):
        return self.name.replace(" ", "-").lower()

    def __log_start(self):
        LOG.info(
            "%s: started",
            self.name,
            extra={"event": {"type": "%s-start" % self.__machine_name}},
        )

    def __log_error(self, what_happened="failed"):
        LOG.error(
            "%s: %s",
            self.name,
            what_happened,
            extra={"event": {"type": "%s-error" % self.__machine_name}},
        )

    def __log_finished(self):
        LOG.info(
            "%s: finished",
            self.name,
            extra={"event": {"type": "%s-end" % self.__machine_name}},
        )

    def __enter__(self):
        # entering our context means starting our phase's thread.
        self.__thread = Thread(
            target=self.__thread_target, name="phase-%s" % self.__machine_name
        )
        self.__thread.daemon = True
        self.__thread.start()

        # If there's no in queue then we treat the phase as immediately started
        # for logging purposes. Otherwise, we'll wait until we see at least one
        # item arrive in the queue.
        if self.in_queue is None:
            self.__started = True
            self.__log_start()

    def __exit__(self, *_):
        LOG.debug("%s: joining", self.name)
        self.__thread.join(timeout=PHASE_TIMEOUT)
        LOG.debug("%s: joined, is_alive %s", self.name, self.__thread.is_alive())

    def __thread_target(self):
        try:
            self.run()
            self.__future_puts.result()
            self.__log_finished()
            if self.out_queue:
                self.put_output(Phase.FINISHED, task_done=False)
        except PhaseInterrupted:
            # When interrupted, we need to stop, but we don't log an exception
            # with stacktrace as the relevant details will have already been
            # logged by whichever phase hit the initial error (including
            # ourselves if interrupted via put_future_output).
            self.__log_error(self.__interrupt_reason)
        except Exception:  # pylint: disable=broad-except
            # In any other case we must log this as a fatal error.
            LOG.exception("%s: fatal error occurred", self.name)
            self.__log_error()

            # Put the context into error state. This will inform all other
            # phases (at least those with an input queue) that we've hit an
            # error, and so they will be interrupted.
            self.context.set_error()
