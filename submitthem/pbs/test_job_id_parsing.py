"""Tests for PBS job ID parsing with array brackets.

These tests verify that PBS job ID parsing handles various formats correctly,
including array brackets and domain suffixes.
"""

import pytest

from . import pbs


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("12345.domain", "12345.domain"),
        ("6122024[].domain", "6122024.domain"),  # Array format with empty brackets
        ("6122024[]", "6122024"),  # Array format without domain
        ("6122024", "6122024"),  # Regular job ID
        ("job 6122024", "6122024"),  # With "job" prefix
        ("job 6122024[]", "6122024"),  # With "job" prefix and empty brackets
    ],
)
def test_pbs_job_id_parsing_with_arrays(input_str: str, expected: str) -> None:
    """Test that PBS job ID parsing handles array brackets correctly."""
    result = pbs.PBSExecutor._get_job_id_from_submission_command(input_str)
    assert result == expected
