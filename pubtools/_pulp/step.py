import logging
import threading
from more_executors.futures import f_sequence, f_return

LOG = logging.getLogger("pubtools.pulp")


class StepDecorator(object):
    """Implementation of PulpTask.step decorator. See that method for more info."""

    def __init__(self, name):
        self._name = name

    @property
    def human_name(self):
        return self._name

    @property
    def machine_name(self):
        return self._name.replace(" ", "-").lower()

    def __call__(self, fn):
        def new_fn(instance, *args, **kwargs):
            if self.should_skip(instance):
                LOG.info(
                    "%s: skipped",
                    self.human_name,
                    extra={"event": {"type": "%s-skip" % self.machine_name}},
                )
                return args[0] if args else None

            logger = StepLogger(self)
            logger.log_start(args)

            try:
                ret = fn(instance, *args, **kwargs)
            except SystemExit as exc:
                if exc.code == 0:
                    logger.log_return()
                else:
                    logger.log_error()
                raise
            except Exception:
                logger.log_error()
                raise

            logger.log_return(ret)

            return ret

        return new_fn

    def should_skip(self, instance):
        skip = (getattr(instance.args, "skip", None) or "").split(",")
        return self.machine_name in skip


# helpers used in implementation of decorator
def is_future(x):
    return hasattr(x, "add_done_callback")


def as_futures(args):
    arg0 = args[0] if args else None
    if is_future(arg0):
        return [arg0]

    if isinstance(arg0, list) and arg0 and is_future(arg0[0]):
        return arg0

    return None


class StepLogger(object):
    # Implements logging when entering/exiting/failing a step.
    # The main point of this class is to keep track of whether
    # entering a step has been logged, and make sure exiting a step
    # can't be logged before entering.
    def __init__(self, step):
        self.step = step
        self.lock = threading.RLock()
        self.log_opened = False

    def log_start(self, args=None):
        input_future = as_futures(args)

        def do_log():
            with self.lock:
                if self.log_opened:
                    return
                self.log_opened = True

                LOG.info(
                    "%s: started",
                    self.step.human_name,
                    extra={"event": {"type": "%s-start" % self.step.machine_name}},
                )

        if not input_future:
            # This function doesn't take futures as input: then it's
            # about to start immediately
            do_log()
            return

        # This function takes future(s) as input: then the step is
        # only considered to start once *at least one* of the input futures
        # has completed
        for f in input_future:
            f.add_done_callback(lambda f: do_log() if not f.exception() else None)

    def log_error(self):
        self.log_start()

        LOG.error(
            "%s: failed",
            self.step.human_name,
            extra={"event": {"type": "%s-error" % self.step.machine_name}},
        )

    def log_return(self, return_value=None):
        return_future = as_futures([return_value]) or [f_return(None)]

        def do_log():
            self.log_start()

            LOG.info(
                "%s: finished",
                self.step.human_name,
                extra={"event": {"type": "%s-end" % self.step.machine_name}},
            )

        # The step is considered completed once *all* returned futures
        # have completed
        completed = f_sequence(return_future)
        completed.add_done_callback(
            lambda f: self.log_error() if completed.exception() else do_log()
        )
