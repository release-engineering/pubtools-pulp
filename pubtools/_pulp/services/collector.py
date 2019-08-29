import threading

import pushcollector

from .base import Service


class CollectorService(Service):
    """A service providing a pushcollector instance.

    Tasks could just as easily call Collector.get() directly.
    The main reason for this service class is to ensure
    a single Collector instance is used for a task's duration.
    """

    def __init__(self, *args, **kwargs):
        self.__lock = threading.Lock()
        self.__instance = None
        super(CollectorService, self).__init__(*args, **kwargs)

    @property
    def collector(self):
        """A Collector instance used during a task."""
        with self.__lock:
            if not self.__instance:
                self.__instance = pushcollector.Collector.get()
        return self.__instance
