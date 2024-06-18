from argparse import ArgumentParser


class Service(object):
    """Mix-in class to be inherited for access to specific services.

    The Service class is used as follows:

    - for a particular service needed by certain tasks (e.g. a Pulp client,
      a UD Cache client), implement a subclass of Service
    - in the subclass, if there are associated command-line arguments, override
      add_service_args to configure those
    - in the subclass, add the properties which should be exposed by that service
      (often just one)

    Once done, every task which needs that service can inherit from the needed
    service implementation(s) to access it with consistent argument handling.
    """

    def add_args(self):
        # Overrides the method from PulpTask.
        # These classes can be mixed in both before and after PulpTask,
        # hence the dynamic add_args and parser lookups.
        super_add_args = getattr(super(Service, self), "add_args", lambda: None)
        super_add_args()

        parser = getattr(self, "parser", ArgumentParser())
        self.add_service_args(parser)

    def add_service_args(self, parser):
        # Implement me in subclasses to add arguments particular to a service
        # (if any).
        # Make sure to call super() when overriding.
        pass

    @property
    def _service_args(self):
        # Subclasses call this instead of self.args to avoid pylint warnings
        # everywhere.
        #
        # Expected to be mixed in with a class providing "args" property.
        assert hasattr(self, "args"), "BUG: Service inheritor must provide 'args'"
        return self.args  # pylint: disable=no-member
