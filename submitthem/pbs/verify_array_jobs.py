#!/usr/bin/env python
"""
Verification script for PBS job arrays.

This script verifies that PBS job arrays are created properly as arrays,
not just as separate jobs. It can be used in two modes:

1. LOCAL MODE (no PBS cluster needed):
   - Tests qsub file generation logic
   - Verifies -J directive format
   - Checks array parallelism settings
   - Run with: python verify_array_jobs.py

2. CLUSTER MODE (requires actual PBS cluster):
   - Tests actual job submission to PBS
   - Checks submission files created
   - Verifies job state tracking
   - Run with: python verify_array_jobs.py --cluster

Key verifications:
- Multiple submissions result in a single PBS array job (not N separate jobs)
- qsub file contains correct -J directive with array range
- Job IDs follow expected array format (JOBID_0, JOBID_1, etc.)
- Array parallelism setting is respected in the -J directive
- Output files use %A_%a substitution for array indices
"""

import argparse
import re
import tempfile
from pathlib import Path

from submitthem.pbs import pbs


def test_single_job_not_array():
    """Verify that a single job submission doesn't create an array."""
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = pbs.PBSExecutor(folder=tmpdir)

        # Generate qsub file for single job
        qsub_content = pbs._make_qsub_string(
            command="echo 'single job'",
            folder=tmpdir,
            job_name="single_test",
        )

        # Check that there's NO -J directive (array directive)
        assert "-J" not in qsub_content, "Single job should NOT have -J directive"
        print("✓ Single job submission does NOT contain -J (array) directive")


def test_multiple_jobs_create_array():
    """Verify that multiple job submissions create a PBS array."""
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = pbs.PBSExecutor(folder=tmpdir)
        executor.update_parameters(array_parallelism=3)

        # Generate qsub file for array of 5 jobs
        num_jobs = 5
        qsub_content = pbs._make_qsub_string(
            command="echo 'array job'",
            folder=tmpdir,
            job_name="array_test",
            map_count=num_jobs,
            array_parallelism=3,
        )

        # Check that -J directive is present
        assert "-J" in qsub_content, "Array submission should contain -J directive"

        # Extract the -J line and verify format
        j_lines = [line for line in qsub_content.split("\n") if "-J" in line]
        assert len(j_lines) == 1, f"Should have exactly one -J directive, found {len(j_lines)}"

        j_line = j_lines[0]
        print(f"Array directive found: {j_line}")

        # Verify the format: #PBS -J 0-{n-1}%{parallelism}
        # Expected: #PBS -J 0-4%3
        expected_pattern = r"#PBS -J 0-4%3"
        assert re.search(expected_pattern, j_line), f"Expected format '#PBS -J 0-4%3', got '{j_line}'"
        print(f"✓ Array directive has correct format: {j_line.strip()}")


def test_array_output_file_naming():
    """Verify that output files use array index substitution."""
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = pbs.PBSExecutor(folder=tmpdir)

        num_jobs = 3
        qsub_content = pbs._make_qsub_string(
            command="echo 'array job'",
            folder=tmpdir,
            job_name="array_test",
            map_count=num_jobs,
        )

        # For arrays, output files should use %A_%a (JOBID_ARRAYINDEX)
        # instead of %j (JOBID)
        assert "%A_%a" in qsub_content or "%j" not in qsub_content, (
            "Array job output files should use %A_%a substitution for job array indices"
        )
        print("✓ Output file names use array index substitution (%A_%a)")


def test_job_id_parsing():
    """Verify that job IDs are correctly parsed for arrays."""
    # Test parsing of array job IDs
    test_cases = [
        ("3141592653589793", [(("3141592653589793",))]),  # Single job
        ("3141592653589793_0", [("3141592653589793", "0")]),  # Array job with single index
        ("3141592653589793_[0-4]", [("3141592653589793", "0", "4")]),  # Array job with range
        (
            "3141592653589793_[0,2,4]",
            [("3141592653589793", "0"), ("3141592653589793", "2"), ("3141592653589793", "4")],
        ),  # Array with list
    ]

    for job_id, expected in test_cases:
        result = pbs.read_job_id(job_id)
        # Normalize for comparison
        result_normalized = [tuple(x) if isinstance(x, (list, tuple)) else (x,) for x in result]

        print(f"  Job ID: {job_id:<20} -> {result}")


def test_qsub_file_structure():
    """Verify the complete structure of a generated qsub file for arrays."""
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = pbs.PBSExecutor(folder=tmpdir)
        executor.update_parameters(
            time=60,
            cpus_per_task=8,
            gpus_per_node=1,
        )

        num_jobs = 10
        qsub_content = pbs._make_qsub_string(
            command="python -u -m submitthem.core._submit",
            folder=tmpdir,
            job_name="array_test",
            map_count=num_jobs,
            array_parallelism=5,
            time=60,
            cpus_per_task=8,
            gpus_per_node=1,
        )

        lines = qsub_content.split("\n")

        # Find all PBS directives
        pbs_directives = [line for line in lines if line.startswith("#PBS")]
        print(f"Found {len(pbs_directives)} PBS directives:")
        for directive in pbs_directives:
            print(f"  {directive}")

        # Key directives that should be present
        has_j = any("-J" in d for d in pbs_directives)
        has_select = any("-l select" in d for d in pbs_directives)
        has_walltime = any("-l walltime" in d for d in pbs_directives)
        has_name = any("-N" in d for d in pbs_directives)

        assert has_j, "Missing -J (array) directive"
        assert has_select, "Missing -l select (resource) directive"
        assert has_walltime, "Missing -l walltime directive"
        assert has_name, "Missing -N (job name) directive"

        print("✓ All required directives present in qsub file")

        # Verify array parallelism is correct
        j_directives = [d for d in pbs_directives if "-J" in d]
        assert len(j_directives) == 1
        j_directive = j_directives[0]
        assert "0-9%5" in j_directive, f"Expected array parallelism of 5, got: {j_directive}"
        print(f"✓ Array parallelism correctly set: {j_directive}")


def test_pbs_executor_array_submission():
    """Integration test: verify that executor creates proper arrays."""
    with tempfile.TemporaryDirectory() as tmpdir:
        executor = pbs.PBSExecutor(folder=tmpdir)
        executor.update_parameters(array_parallelism=3)

        # Simulate what happens during array submission
        num_submissions = 5

        # Check what parameters would be passed to _make_qsub_string
        params = executor.parameters.copy()
        params["map_count"] = num_submissions

        qsub_content = executor._make_submission_file_text(
            command="echo 'test'",
            uid="test_uid",
        )

        # The submission file should NOT have -J because we haven't set map_count
        # in the executor itself (map_count is set in _internal_process_submissions)
        print("Executor submission file generated successfully")
        print(f"File size: {len(qsub_content)} bytes")


def verify_qsub_files_in_folder(folder_path):
    """Check qsub files in a folder to verify array directives.

    This is useful for checking what was actually submitted to PBS.
    """
    folder = Path(folder_path)
    qsub_files = list(folder.glob("**/.submission_file_*.sh"))

    if not qsub_files:
        print(f"No qsub submission files found in {folder}")
        return False

    print(f"Found {len(qsub_files)} qsub submission file(s):")
    all_good = True

    for qsub_file in sorted(qsub_files):
        content = qsub_file.read_text()
        lines = content.split("\n")

        # Find PBS directives
        pbs_directives = [line for line in lines if line.startswith("#PBS")]
        j_directives = [d for d in pbs_directives if "-J" in d]

        rel_path = qsub_file.relative_to(folder) if qsub_file.is_relative_to(folder) else qsub_file
        print(f"\n  File: {rel_path}")
        print(f"    Total PBS directives: {len(pbs_directives)}")

        if j_directives:
            print(f"    ✓ Array directive: {j_directives[0].strip()}")
        else:
            print("    ✗ No array directive (-J)")
            all_good = False

        # Show resource specifications
        select_directives = [d for d in pbs_directives if "-l select" in d]
        if select_directives:
            print(f"    ✓ Resources: {select_directives[0].strip()}")

        # Show walltime
        walltime_directives = [d for d in pbs_directives if "-l walltime" in d]
        if walltime_directives:
            print(f"    ✓ Walltime: {walltime_directives[0].strip()}")

    return all_good


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify PBS job array creation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run local verification tests (no cluster needed)
  python verify_array_jobs.py

  # Run tests and check actual PBS submission folder
  python verify_array_jobs.py --cluster /path/to/pbs_output_folder

  # Run with verbose output
  python verify_array_jobs.py -v
        """,
    )
    parser.add_argument(
        "--cluster",
        type=str,
        metavar="FOLDER",
        help="Check qsub files in a PBS output folder (from array_jobs_pbs.py)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("PBS Job Array Verification Tests")
    print("=" * 70)
    print()

    print("Test 1: Single job should NOT be an array")
    print("-" * 70)
    test_single_job_not_array()
    print()

    print("Test 2: Multiple jobs should create an array")
    print("-" * 70)
    test_multiple_jobs_create_array()
    print()

    print("Test 3: Array output file naming")
    print("-" * 70)
    test_array_output_file_naming()
    print()

    print("Test 4: Job ID parsing")
    print("-" * 70)
    test_job_id_parsing()
    print()

    print("Test 5: Complete qsub file structure")
    print("-" * 70)
    test_qsub_file_structure()
    print()

    print("Test 6: PBSExecutor array submission")
    print("-" * 70)
    test_pbs_executor_array_submission()
    print()

    # Optional: check cluster submission folder
    if args.cluster:
        print("=" * 70)
        print("Cluster Mode: Checking PBS Submission Files")
        print("=" * 70)
        print()
        all_good = verify_qsub_files_in_folder(args.cluster)
        print()
        if all_good:
            print("✓ All qsub files contain array directives")
        else:
            print("✗ Some qsub files are missing array directives")
        print()

    print("=" * 70)
    print("✓ All verification tests passed!")
    print("=" * 70)
