import logging
import os
from threading import Thread

try:
    from time import monotonic
except ImportError:  # pragma: no cover
    from monotonic import monotonic
from six.moves.queue import Empty

from .buffer import OutputBuffer
from .errors import PhaseInterrupted
from .progress import ProgressInfo

from . import constants

LOG = logging.getLogger("pubtools.pulp")


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

    # Subclasses are intended to override the following class constants where needed.

    STARTUP_TYPE = constants.DEFAULT_STARTUP_TYPE
    """When should the phase be considered started?"""

    PROGRESS_TYPE = constants.DEFAULT_PROGRESS_TYPE
    """Should the phase appear in progress logger?"""

    UPDATES_PUSH_ITEMS = False
    """Should the phase's output items automatically be sent to pushcollector?"""

    def __init__(
        self, context, in_queue=None, out_queue=True, name="<unknown phase>", **kwargs
    ):
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
        self.name = name
        self.in_queue = in_queue
        self.out_queue_size = self.__tunable("QUEUE_SIZE")
        self.out_queue = (
            context.new_queue(maxsize=self.out_queue_size)
            if out_queue is True
            else out_queue
        )
        self.context = context

        # Save some tunables now at time of construction to avoid repeatedly
        # querying the environment variables.
        self.default_batch_size = self.__tunable("BATCH_SIZE")
        self.batch_timeout = self.__tunable("BATCH_TIMEOUT", float)
        self.batch_max_timeout = self.__tunable("BATCH_MAX_TIMEOUT", float)

        self.progress_info = None

        if self.PROGRESS_TYPE is constants.PROGRESS_TYPE_QUEUE:
            # Arrange for updating progress automatically as queues
            # are accessed.
            self.progress_info = ProgressInfo(self.name)
            self.context.progress_infos.append(self.progress_info)

            if self.in_queue:
                self.in_queue.after_get.append(self.__progress_queue_get)
            if self.out_queue:
                self.out_queue.after_put.append(self.__progress_queue_put)

        self.update_push_items = kwargs.get("update_push_items", lambda _: ())

        if self.UPDATES_PUSH_ITEMS:
            assert self.out_queue, (
                "BUG: phase %s declares UPDATE_PUSH_ITEMS=True but "
                "has no output queue" % self.name
            )
            self.out_queue.before_put.append(self.__update_push_items_from_queue)

        self.__thread = Thread(
            target=self.__thread_target, name="phase-%s" % self.__machine_name
        )
        self.__thread.daemon = True

        self.out_writer = OutputBuffer(
            self.out_queue,
            self.context,
            self.__thread,
            self.__tunable("OUT_BATCH_SIZE"),
            self.__tunable("OUT_BATCH_TIMEOUT", float),
            self.__tunable("OUT_MAX_FUTURES"),
        )

        self.__started = False

        # This string is used in logs produced if the phase is interrupted.
        # By default, if this happens we simply say the phase is "interrupted"
        # with no further details. However, when the phase interrupts itself
        # it can update this string to "failed" (or something else) to indicate
        # that the interruption comes from within rather than another source.
        self.__interrupt_reason = "interrupted"

    def run(self):
        """The business logic for this phase.

        Subclasses must override this to implement each phase's desired logic.
        This should generally consistent of reading from the phase's input queue
        and writing to the output queue.
        """
        raise NotImplementedError()  # pragma: no cover

    def notify_started(self):
        """By default, each phase is considered 'started' as soon as the phase's queue
        has received any data.

        In some cases that does not make sense from the end-user's point of view, and
        more fine-grained control is useful. In that case, a subclasses may set their
        STARTUP_TYPE to STARTUP_TYPE_NOTIFY and explicitly invoke this method at the
        point at which the phase is considered to have started. It is safe to invoke
        the method multiple times.

        It's a bug to invoke this method if STARTUP_TYPE is not STARTUP_TYPE_NOTIFY.
        """
        assert (
            self.STARTUP_TYPE is constants.STARTUP_TYPE_NOTIFY
        ), "BUG: notify_started() on phase %s with startup type %s" % (
            self.name,
            self.STARTUP_TYPE,
        )

        if not self.__started:
            self.__started = True
            self.__log_start()

    def iter_input(self):
        """Get an iterable over this phase's input queue, one item at a time.

        Stops iteration if the queue receives FINISHED (and does not yield that value).

        Raises if the queue receives ERROR.

        It is a bug to call this method on a phase with no input queue.
        """
        for items in self.iter_input_batched(batch_size=1):
            yield items[0]

    def iter_input_batched(self, batch_size=None):
        """Get an iterable over this phase's input queue, yielding items in batches
        of the specified size.

        Stops iteration if the queue receives FINISHED (and does not yield that value,
        but yields the batch leading up to it).

        Raises if the queue receives ERROR.

        It is a bug to call this method on a phase with no input queue.
        """
        batch_size = batch_size or self.default_batch_size
        next_batch = []

        while True:
            start_time = monotonic()
            timeout = self.__batch_timeout

            def batch_ready():
                return (
                    (len(next_batch) >= batch_size)
                    or (monotonic() - start_time > timeout)
                    or (next_batch and next_batch[-1] is constants.FINISHED)
                )

            def extend_batch(get_timeout=constants.PHASE_TIMEOUT):
                got = self.__get_input(timeout=get_timeout)
                if got is constants.FINISHED:
                    next_batch.append(got)
                else:
                    next_batch.extend(got)

            extend_batch()

            while not batch_ready():
                try:
                    extend_batch(timeout)
                except Empty:
                    # batch_ready() will now be true
                    pass

            stop = False
            if next_batch and next_batch[-1] is constants.FINISHED:
                stop = True
                next_batch.pop(-1)

            while next_batch and batch_ready():
                yield next_batch[:batch_size]
                next_batch[:] = next_batch[batch_size:]

            if stop:
                # all done - yield last batch as well
                if next_batch:
                    yield next_batch
                return

    def put_output(self, value):
        """Output a value from this phase.

        Output is buffered and might not be sent until the next call to
        self.out_writer.flush().

        It is a bug to call this method on a phase with no output queue.
        """
        self.out_writer.write(value)

    def put_future_output(self, value_f):
        """Like put_output, but the given value should be a future returning
        a single item.

        This method will block if the output buffer already contains the maximum
        number of futures.
        """
        self.out_writer.write_future(value_f)

    def put_future_outputs(self, values_f):
        """Like put_future_output, but the given future should return a list of
        items.
        """
        self.out_writer.write_future_batch(values_f)

    def __get_input(self, timeout=constants.PHASE_TIMEOUT):
        # Get a single item from input queue; this is currently private
        # as it seems like the inheritors ought to always use one of the
        # iterable forms.
        assert self.in_queue, "BUG: phase has no input queue"

        out = self.in_queue.get(block=True, timeout=timeout)

        if not self.__started and self.STARTUP_TYPE is constants.STARTUP_TYPE_QUEUE:
            self.__started = True
            self.__log_start()

        return out

    @property
    def __batch_timeout(self):
        # How long iter_input_batched should wait for a full batch before
        # proceeding with what we already have.
        #
        # Returns a value in seconds scaled between BATCH_TIMEOUT and
        # BATCH_MAX_TIMEOUT scaled according to the current size of the output
        # queue. The reasoning here is:
        #
        # - if output queue is empty, we should try to get some new work items
        #   ASAP to put something on the queue so the next phase has something
        #   to do.
        #
        # - if output queue is full, there is no point in rushing to get more
        #   items, since anything we process now will have to wait in the output
        #   queue anyway; so it's more efficient to wait for a larger batch.
        #
        out_queue_size = 0 if not self.out_queue else self.out_queue.qsize()
        out_queue_fraction = out_queue_size / float(self.out_queue_size)
        timeout = out_queue_fraction * self.batch_max_timeout
        out = max(min(timeout, self.batch_max_timeout), self.batch_timeout)
        return out

    @property
    def __machine_name(self):
        return self.name.replace(" ", "-").lower()

    def __tunable(self, key, converter=int):
        """Get value of some tunable which can be controlled by an
        environment variable.
        """
        # Look for a phase-specific env var. Allows fine-grained tuning of each phase.
        # If not present, the value of same name from constants is used.
        env_var = "PUBTOOLS_PULP_%s__%s" % (key, self.name.replace(" ", "_").upper())
        value = os.getenv(env_var, str(getattr(constants, key)))
        return converter(value)

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

    # Callbacks installed on queue to take some actions around get/put:
    def __progress_queue_get(self, item):
        if isinstance(item, list):
            self.progress_info.in_count += len(item)

    def __progress_queue_put(self, item):
        if isinstance(item, list):
            self.progress_info.out_count += len(item)

    def __update_push_items_from_queue(self, item):
        if isinstance(item, list):
            self.update_push_items(item)

    def __enter__(self):
        # If there's no in queue then we treat the phase as immediately started
        # for logging purposes. Otherwise, we'll wait until we see at least one
        # item arrive in the queue.
        if self.in_queue is None and self.STARTUP_TYPE is constants.STARTUP_TYPE_QUEUE:
            self.__started = True
            self.__log_start()

        self.__thread.start()

    def __exit__(self, exc_type, exc_val, _exc_tb):
        if exc_type and not self.context.has_error:
            # If there is any exception and the context isn't yet failed, mark it
            # failed to interrupt everything.
            #
            # Shouldn't really happen due to failures coming from within a phase run()
            # as those will have already been caught.
            # However we can get here if for example someone hits CTRL+C to generate
            # a KeyboardInterrupt.
            if not self.context.has_error:
                LOG.debug(
                    "%s: marking context failed due to exception",
                    self.name,
                    exc_info=exc_val,
                )
                self.context.set_error(self.name, exc_val)

        LOG.debug("%s: joining", self.name)
        self.__thread.join(timeout=constants.PHASE_TIMEOUT)
        LOG.debug("%s: joined, is_alive %s", self.name, self.__thread.is_alive())

    def __thread_target(self):
        try:
            self.run()
            self.out_writer.flush()
            self.__log_finished()
            if self.out_queue:
                self.out_queue.put(constants.FINISHED)
        except PhaseInterrupted:
            # When interrupted, we need to stop, but we don't log an exception
            # with stacktrace as the relevant details will have already been
            # logged by whichever phase hit the initial error (including
            # ourselves if interrupted via put_future_output).
            self.__log_error(self.__interrupt_reason)
            self.out_writer.cancel()
        except Exception as exc:  # pylint: disable=broad-except
            # In any other case we must log this as a fatal error.
            LOG.exception("%s: fatal error occurred", self.name)
            self.__log_error()
            self.out_writer.cancel()

            # Put the context into error state. This will inform all other
            # phases (at least those with an input queue) that we've hit an
            # error, and so they will be interrupted.
            self.context.set_error(phase=self.name, exception=exc)
