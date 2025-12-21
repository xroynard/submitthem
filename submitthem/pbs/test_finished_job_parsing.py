"""Tests for PBS job status parsing and completion detection.

These tests verify that PBS job status states are parsed correctly from qstat output,
including the F (Finished) and C (Completed) states.
"""

import pytest

from . import pbs


@pytest.mark.parametrize(
    "state_letter,expected_state",
    [
        ("F", "COMPLETED"),  # F = Finished (completed execution)
        ("C", "COMPLETED"),  # C = Completed successfully
    ],
)
def test_pbs_job_status_parsing(state_letter: str, expected_state: str) -> None:
    """Test that PBS job status letters are parsed correctly."""
    # Use simple format without leading whitespace to avoid column parsing issues
    qstat_output = f"Job ID           S\n6122024          {state_letter}\n".encode()

    watcher = pbs.PBSInfoWatcher()
    info_dict = watcher.read_info(qstat_output)

    assert "6122024" in info_dict
    job_info = info_dict["6122024"]
    assert job_info.get("State") == expected_state


def test_pbs_is_done_with_finished_job() -> None:
    """Test that is_done() returns True for finished jobs."""
    watcher = pbs.PBSInfoWatcher()

    job_id = "6122024"
    watcher.register_job(job_id)

    # Simulate qstat output with finished job (F state) in simple format, no leading whitespace
    qstat_output = b"Job ID           S\n6122024          F\n"

    watcher._info_dict.update(watcher.read_info(qstat_output))

    # Test is_done with cache mode (no qstat call)
    is_finished = watcher.is_done(job_id, mode="cache")
    assert is_finished
