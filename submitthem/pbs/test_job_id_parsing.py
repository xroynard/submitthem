"""Tests for PBS job ID parsing with array brackets.

These tests verify that PBS job ID parsing handles various formats correctly,
including array brackets and domain suffixes.
"""

import pytest

from . import pbs


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("3141592653589793.domain", "3141592653589793.domain"),
        ("3141592653589793[].domain", "3141592653589793.domain"),  # Array format with empty brackets
        ("3141592653589793[]", "3141592653589793"),  # Array format without domain
        ("3141592653589793", "3141592653589793"),  # Regular job ID
        ("job 3141592653589793", "3141592653589793"),  # With "job" prefix
        ("job 3141592653589793[]", "3141592653589793"),  # With "job" prefix and empty brackets
    ],
)
def test_pbs_job_id_parsing_with_arrays(input_str: str, expected: str) -> None:
    """Test that PBS job ID parsing handles array brackets correctly."""
    result = pbs.PBSExecutor._get_job_id_from_submission_command(input_str)
    assert result == expected
