"""Helpers for testing commands."""
from __future__ import print_function
import logging
import sys
import os
import traceback
import json
import difflib

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")


class CommandTester(object):
    """CommandTester is a helper class to run a command, capture its output and
    compare against expected.

    An instance may be obtained via command_tester fixture.
    """

    def __init__(self, node, caplog):
        self._node = node
        self._caplog = caplog

    def test(self, fn, args):
        """Put args into sys.argv, then test the given function.

        Logs at INFO level and higher will be captured and compared against
        test data under "test/logs" directory. 'extra' metadata in logs will
        also be tested using JSONL files.

        To update test logs in case of an intentional change, set the
        UPDATE_BASELINES environment variable to 1.
        """
        self._caplog.set_level(logging.INFO)

        sys.argv[:] = args
        exception = None
        try:
            fn()
        except AssertionError:
            # Let these raise directly
            raise
        except SystemExit as ex:
            exception = ex
        except Exception as ex:
            traceback.print_exc()
            exception = ex

        records = self._caplog.records
        self._compare_outcome(records, exception)

    @property
    def logfile_basename(self):
        out = self._node.nodeid

        # Example node ID:
        #   tests/clear_repo/test_clear_repo.py::test_typical
        #
        # Desired output:
        #   <logdir>/clear_repo/test_clear_repo/test_typical
        #
        if out.startswith("tests/"):
            out = out[len("tests/") :]
        out = out.replace(".py", "")
        out = out.replace("::", "/")

        return os.path.join(LOGS_DIR, out)

    def _get_actual_plaintext(self, records):
        out = ""
        for record in records:
            out += "[%8s] %s\n" % (record.levelname, record.message)
        return out

    def _get_actual_jsonl(self, records):
        out = []
        for record in records:
            if hasattr(record, "event"):
                out.append({"event": record.event})

        return "\n".join([json.dumps(x) for x in out])

    def _update_baseline(self, filename, content):
        if not content:
            # Don't bother creating empty files for tests with no output
            return

        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        open(filename, "wt").write(content)

    def _compare_expected(self, text, suffix, exception=None):
        filename = self.logfile_basename + suffix
        expected_content = None

        if not text.endswith("\n"):
            text += "\n"

        if os.path.exists(filename):
            expected_content = open(filename, "rt").read()

        if expected_content != text:
            if (
                os.environ.get("UPDATE_BASELINES", "0") == "1"
                or expected_content is None
            ):
                return self._update_baseline(filename, text)

            from_lines = expected_content.split("\n")
            to_lines = text.split("\n")
            diff = difflib.unified_diff(
                from_lines,
                to_lines,
                fromfile="<output of test>",
                tofile=filename,
                lineterm="",
            )
            diff = list(diff)

            exception_changed = diff[-2].startswith("+# Raised:")
            diff = "\n".join(diff)
            message = (
                "Output differs from expected (set UPDATE_BASELINES=1 if intended):\n"
                + diff
            )

            if exception_changed and exception:
                # if exception differs from expected, re-raise it for the sake of
                # a meaningful backtrace and ability to run "py.test --pdb"
                print(message)
                raise exception

            raise AssertionError(message)

    def _compare_outcome(self, records, exception):

        plaintext = self._get_actual_plaintext(records)
        if exception:
            plaintext += "# Raised: %s\n" % exception

        self._compare_expected(plaintext, ".txt", exception)

        jsonl = self._get_actual_jsonl(records)
        self._compare_expected(jsonl, ".jsonl")
