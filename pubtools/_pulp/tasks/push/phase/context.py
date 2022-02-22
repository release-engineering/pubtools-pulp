from six.moves.queue import Queue

from .base import Phase


class Context(object):
    """A context object shared across all phases.

    The main purpose of the context object is to group all phases
    under a single execution context which can be stopped on demand
    (i.e. if an error occurs). It may also be used to share a small
    amount of out-of-band mutable state between phases.
    """

    def __init__(self):
        self._queues = []
        self._error = False

    @property
    def has_error(self):
        """True if and only if the context is in the error state.

        If `has_error` is true, phases should stop processing ASAP.
        """
        return self._error

    def set_error(self):
        """Set the context into the error state, indicating that a fatal
        error has occurred.
        """
        self._error = True
        for queue in self._queues:
            queue.put(Phase.ERROR)

    def new_queue(self, *args, **kwargs):
        """Create and return a new Queue.

        The Queue is associated with this context such that, if the context
        enters the error state, the queue will receive an ERROR object.
        """
        out = Queue(*args, **kwargs)
        self._queues.append(out)
        return out
