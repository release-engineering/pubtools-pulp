"""Helpers to deal with py2 vs py3 differences in contextlib.

# FIXME: please delete this entire module when py2 support is dropped
and just use contextlib directly!
"""

import contextlib

# As we are referring to legacy stuff removed in py3...
# pylint: disable=no-member


def exitstack(cms):
    """Returns a wrapper for multiple context managers using the
    best available method.

    Arguments:
        cms
            An iterable of objects satisfying the context manager protocol.

    Returns:
        a context manager which, on exit, will invoke __exit__ on every context
        manager provided as input.
    """
    if hasattr(contextlib, "ExitStack"):
        # modern case
        stack = contextlib.ExitStack()
        for cm in cms:
            stack.enter_context(cm)
        return stack

    # legacy case - python2
    return contextlib.nested(*cms)  # pragma: no cover
