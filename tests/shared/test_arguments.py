import sys
from argparse import ArgumentParser

import pytest

from pubtools._pulp.arguments import SplitAndExtend

FAKE_OPTION_VALUES = ["value1", "value2", "value3"]


@pytest.fixture
def parser():
    return ArgumentParser()


def test_split_and_extend_single_delimited_instance():
    """Test a single option instance, with all values in a delimited list."""
    # test one instance, all delimited using different delimiters
    for delimiter in ",.-/":
        parser = ArgumentParser()
        parser.add_argument(
            "--option", type=str, action=SplitAndExtend, split_on=delimiter
        )

        sys.argv = ["fake-command", "--option", delimiter.join(FAKE_OPTION_VALUES)]

        args = parser.parse_args()
        assert args.option == FAKE_OPTION_VALUES


def test_split_and_extend_multiple_option_instances(parser):
    """Test multiple option instances."""
    parser.add_argument("--option", type=str, action=SplitAndExtend)

    # test multiple, individual instances
    sys.argv = ["command"]
    for value in FAKE_OPTION_VALUES:
        sys.argv.extend(["--option", value])

    args = parser.parse_args()
    assert args.option == FAKE_OPTION_VALUES


def test_split_and_extend_multiple_mix_and_match_instance():
    """Test multiple option instances, some with delimited lists."""
    # test mix-and-match, delimited-and-not, using different delimiters
    for delimiter in ",.-/":
        parser = ArgumentParser()
        parser.add_argument(
            "--option", type=str, action=SplitAndExtend, split_on=delimiter
        )

        sys.argv = [
            "fake-command",
            "--option",
            delimiter.join(FAKE_OPTION_VALUES[:-1]),
            "--option",
            FAKE_OPTION_VALUES[-1],
        ]

        args = parser.parse_args()
        assert args.option == FAKE_OPTION_VALUES


def test_split_and_extend_multiple_instances_with_trailing_delimiters(parser):
    """Test multiple instances, each with an extra trailing delimiter."""
    parser.add_argument("--option", type=str, action=SplitAndExtend)

    # test multiple, individual instances with extra
    # trailing delimiters e.g. --option value1, --option value2,
    sys.argv = ["command"]
    for value in FAKE_OPTION_VALUES:
        sys.argv.extend(["--option", "{},".format(value)])

    args = parser.parse_args()
    assert args.option == FAKE_OPTION_VALUES
