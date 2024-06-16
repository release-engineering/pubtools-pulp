import os
from argparse import Action


def from_environ(key, delegate_converter=lambda x: x):
    """A converter for use as an argparse "type" argument which supports
    reading values from the environment.

    Expected usage is like this:

      add_argument('--my-password', default='', type=from_environ('MY_PASSWORD'))

    Or if you need a non-string type, you can combine with another converter:

      add_argument('--threads', default='', type=from_environ('THREADS', int))

    Reasons to do this instead of just default=os.environ.get(...) include:

    - resolve the env var when arguments are parsed rather than when the parser is
      set up; helpful for writing autotests

    - ensure the default value can't end up in output of --help (which could leak
      passwords from env vars)

    Arguments:
        key (str)
            Name of environment variable to look up.
        delegate_converter (callable)
            A converter for the looked up environment variable.

    Returns:
        object
            The argument value looked up from environment & converted.
    """
    return FromEnvironmentConverter(key, delegate_converter)


class FromEnvironmentConverter(object):
    def __init__(self, key, delegate):
        self.key = key
        self.delegate = delegate

    def __call__(self, value):
        if not value:
            value = os.environ.get(self.key)
        return self.delegate(value)


class SplitAndExtend(Action):
    """Argparse Action subclass for splitting string-type arguments.

    This action is intended to be similar to the built-in action
    ``"extend"`` (Added in 3.8), which allows for multiple instances
    of an option to be present by accumulating each instance's values
    in to a flattened list.

    Where this action adds functionality is in the ability to split
    string arguments delimited by the ``split_on`` delimiter in to
    a list before adding them to the resulting accumulated list.

    This allows each instance that is present to be further broken
    down in to a ``split_on`` delimited list of values.

    To incorporate this action, set ``action=SplitAndExtend`` in
    the call to ``ArgumentParser#add_argument``. By default this will
    handle comma-delimited string args.

    To use a different delimiter, pass ``split_on="your-delimiter"`` in
    addition to ``action=SplitAndExtend``.

    Examples:
        >>> # setup the parser
        >>> import sys
        >>> from argparse import ArgumentParser
        >>> parser = ArgumentParser()
        >>> parser.add_argument("--option", type=str, action=SplitAndExtend, split_on=",")

        Option with multiple instances, some are comma-delimited lists:
        >>> sys.argv = ["command", "--option", "value1,value2", "--option", "value3"]
        >>> args = parser.parse_args()
        >>> print(args)
        Namespace(option=['value1', 'value2', 'value3'])

        Option with single instance, single value:
        >>> sys.argv = ["command", "--option", "value1"]
        >>> args = parser.parse_args()
        >>> print(args)
        Namespace(option=["value1"])

        No option present:
        >>> sys.argv = ["command"]
        >>> args = parser.parse_args()
        >>> print(args)
        Namespace(option=None)

        Option present, value missing
        >>> sys.argv = ["command", "--option"]
        >>> args = parser.parse_args()
        usage: pydevconsole.py [-h] [--option OPTION]
        pydevconsole.py: error: argument --option: expected one argument

    Attributes:
        split_on (str): the delimiter on which to split a delimited list
            of vaules for a single instance of an option.

    See Also:
        `Built-in Argparse Actions`_
            The set of built-in argparse Actions.

    .. _Built-in Argparse Actions:
        https://docs.python.org/3/library/argparse.html#action
    """

    def __init__(self, *args, **kwargs):
        self.__split_on = kwargs.pop("split_on", ",")
        super(SplitAndExtend, self).__init__(*args, **kwargs)

    def __call__(self, _, namespace, values, options=None):
        items = getattr(namespace, self.dest, None) or []
        # if values isn't a string, don't try to split it
        # just add it to the accumulated list.
        # by default, argparse will parse each value as a string,
        # so unless this action is being used in conjunction with
        # parser.add_argument(type=<some non-string type>) this
        # should not be the case.
        split = values.split(self.split_on) if isinstance(values, str) else values
        items.extend(split)
        setattr(namespace, self.dest, items)

    @property
    def split_on(self):
        return self.__split_on
