import sys
from argparse import ArgumentParser

import pytest

from pubtools._pulp.arguments import SplitAndExtend

FAKE_OPTION_VALUES = ["value1", "value2", "value3"]


@pytest.fixture
def parser():
    return ArgumentParser()


@pytest.fixture(params=[",", ".", "-", "/"])
def delimiter(request):
    """Provide parameterization for testing different delimiters."""
    return request.param


def test_split_and_extend_single_delimited_instance(parser, delimiter):
    """Test a single option instance, with all values in a delimited list."""
    # test one instance, all delimited using different delimiters
    parser.add_argument("--option", type=str, action=SplitAndExtend, split_on=delimiter)

    sys.argv = ["command", "--option", delimiter.join(FAKE_OPTION_VALUES)]

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


def test_split_and_extend_multiple_mix_and_match_instance(parser, delimiter):
    """Test multiple option instances, some with delimited lists."""
    # test mix-and-match, delimited-and-not, using different delimiters
    parser.add_argument("--option", type=str, action=SplitAndExtend, split_on=delimiter)

    sys.argv = [
        "command",
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
    # delimiters e.g. `--option value1,` or `--option value1,,value2`
    sys.argv = [
        "command",
        "--option",
        "value0,",
        "--option",
        ",value1,value2,,",
        "--option",
        "value3,,value4",
        "--option",
        ",,,value5",
    ]

    expected = ["value{}".format(i) for i in range(6)]

    args = parser.parse_args()
    assert args.option == expected
