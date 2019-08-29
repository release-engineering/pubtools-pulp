import os


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
