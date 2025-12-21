#!/usr/bin/env python
"""
Example of submitting PBS array jobs with submitthem.

This demonstrates how to submit multiple jobs efficiently using array jobs,
which is important for submitting many similar jobs to a cluster.

IMPORTANT: This creates a SINGLE PBS array job with multiple tasks,
not N separate jobs. This is more efficient for job scheduling.

To verify that arrays are being created properly:
  python verify_array_jobs.py                          # Local verification
  python verify_array_jobs.py --cluster ./pbs_array_jobs  # Check submission files
"""

import re

import submitthem


def process_dataset(dataset_id: int, num_chunks: int = 100) -> dict:
    """
    Process a dataset in chunks.

    Args:
        dataset_id: ID of the dataset to process
        num_chunks: Number of chunks to process

    Returns:
        Processing results
    """
    total_processed = 0
    errors = 0

    for chunk_idx in range(num_chunks):
        try:
            # Simulate processing
            chunk_size = 1000 + chunk_idx * 10
            total_processed += chunk_size
        except Exception:
            errors += 1

    return {
        "dataset_id": dataset_id,
        "total_processed": total_processed,
        "errors": errors,
        "success": errors == 0,
    }


def main():
    """Submit array jobs to PBS cluster for parallel processing.

    IMPORTANT: To create a PBS array job, use executor.batch() context manager.
    All jobs submitted within the context are batched into a single array job.

    Without batch(), each submit() creates a separate job (not an array).
    """
    # Use AutoExecutor
    executor = submitthem.AutoExecutor(
        folder="./pbs_array_jobs",
    )

    print(f"Executor: {executor.cluster}")
    print()

    # Configure for PBS
    if executor.cluster == "pbs":
        executor.update_parameters(
            pbs_time=60,  # 1 hour per job
            cpus_per_task=8,  # 8 CPUs per task
            mem_gb=16,  # 16 GB memory
        )
        print("PBS Configuration:")
        print("  Walltime: 60 minutes")
        print("  CPUs: 8")
        print("  Memory: 16 GB")
        print()

    # Method 1: Using batch() context - creates a PBS array job
    # All submissions within the context are batched into a single array
    NB_JOBS = 2
    print(f"Submitting {NB_JOBS} dataset processing jobs as a PBS array...")
    print("=" * 70)
    jobs = []
    with executor.batch():
        for dataset_id in range(NB_JOBS):
            job = executor.submit(process_dataset, dataset_id)
            jobs.append(job)

    print(f"Submitted {len(jobs)} jobs")
    print()

    # Analyze the job IDs to verify array structure
    if len(jobs) > 1:
        job_ids = [j.job_id for j in jobs]
        print("Job IDs:")
        for job_id in job_ids:
            print(f"  {job_id}")
        print()

        # Check if all jobs share the same main job ID (indicating an array)
        # Extract main numeric ID from various formats:
        # - "3141592653589793[0].domain" -> "3141592653589793"
        # - "3141592653589793[0]" -> "3141592653589793"
        # - "3141592653589793_0" -> "3141592653589793"
        def extract_main_id(job_id: str) -> str:
            """Extract the main numeric job ID from various PBS job ID formats."""
            # Strip domain suffix first
            id_no_domain = job_id.split(".")[0]
            # Try bracket notation: "3141592653589793[0]" -> extract "3141592653589793"
            bracket_match = re.match(r"(\d+)\[", id_no_domain)
            if bracket_match:
                return bracket_match.group(1)
            # Try underscore notation: "3141592653589793_0" -> extract "3141592653589793"
            return id_no_domain.split("_")[0]

        main_job_id = extract_main_id(jobs[0].job_id)
        all_same_main = all(extract_main_id(j.job_id) == main_job_id for j in jobs)
        if all_same_main:
            print(f"✓ ARRAY JOB: All tasks belong to array job {main_job_id}")
            # Detect format: bracket or underscore
            first_id_no_domain = jobs[0].job_id.split(".")[0]
            if "[" in first_id_no_domain:
                print(f"  Format: {main_job_id}[<index>] for {len(jobs)} tasks")
            else:
                print(f"  Format: {main_job_id}_<task_index> for {len(jobs)} tasks")
        else:
            print("✗ NOT AN ARRAY: Jobs have different main IDs (separate submissions)")
        print()

    # Collect and monitor results
    print("Collecting results with batch monitoring...")
    import time

    start_time = time.time()

    results = []
    failed_jobs = []

    # Note: You may see warnings about qstat not finding the job ID.
    # This is normal for newly submitted array jobs - PBS takes a moment
    # to fully register them. The jobs are running in the queue even if
    # qstat hasn't found them yet. You can check with: qstat -n1 -w

    for job in jobs:
        job_start = time.time()
        try:
            result = job.result()
            job_elapsed = time.time() - job_start
            total_elapsed = time.time() - start_time
            results.append(result)
            if result["success"]:
                print(
                    f"[{total_elapsed:6.1f}s] Job {job.job_id}: processed {result['total_processed']} items (waited {job_elapsed:5.1f}s)"
                )
            else:
                print(
                    f"[{total_elapsed:6.1f}s] Job {job.job_id}: completed with {result['errors']} errors (waited {job_elapsed:5.1f}s)"
                )
        except Exception as e:
            job_elapsed = time.time() - job_start
            total_elapsed = time.time() - start_time
            print(
                f"[{total_elapsed:6.1f}s] Job {job.job_id}: failed after {job_elapsed:5.1f}s with error {e}"
            )
            failed_jobs.append(job.job_id)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total jobs: {len(jobs)}")
    print(f"Successful: {len(results)}")
    print(f"Failed: {len(failed_jobs)}")

    if results:
        total_items = sum(r["total_processed"] for r in results)
        print(f"Total items processed: {total_items}")

    if failed_jobs:
        print(f"Failed job IDs: {failed_jobs}")

    print()
    print("To verify array jobs were created properly:")
    print("  python ../../submitthem/pbs/verify_array_jobs.py --cluster ./pbs_array_jobs")


if __name__ == "__main__":
    main()
