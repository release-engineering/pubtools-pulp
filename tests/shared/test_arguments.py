import sys
from argparse import ArgumentParser

import pytest

from pubtools._pulp.arguments import SplitAndExtend


@pytest.fixture
def parser():
    return ArgumentParser()


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["--option", "a"], ["a"]),
        (["--option", "a,"], ["a", ""]),
        (["--option", "a,b"], ["a", "b"]),
        (["--option", "a,b,"], ["a", "b", ""]),
        (["--option", ",a,b"], ["", "a", "b"]),
        (["--option", "a,,b"], ["a", "", "b"]),
        (["--option", "a", "--option", "b"], ["a", "b"]),
        (["--option", "a,b", "--option", "c"], ["a", "b", "c"]),
        (["--option", "a", "--option", "b,c"], ["a", "b", "c"]),
        (["--option", "a,,b", "--option", ",c,"], ["a", "", "b", "", "c", ""]),
    ],
)
def test_split_and_extend(parser, argv, expected):
    """Test SplitAndExtend argparse Action."""
    parser.add_argument("--option", type=str, action=SplitAndExtend)
    sys.argv = ["command"] + argv
    args = parser.parse_args()
    assert args.option == expected


@pytest.mark.parametrize("delimiter", [",", ".", "-", "/"])
def test_split_and_extend_varying_delimiters(parser, delimiter):
    """Test using different delimiters using a single option instance."""
    expected = ["a", "b", "x", "y"]
    parser.add_argument("--option", type=str, action=SplitAndExtend, split_on=delimiter)
    sys.argv = ["command", "--option", delimiter.join(expected)]
    args = parser.parse_args()
    assert args.option == expected
