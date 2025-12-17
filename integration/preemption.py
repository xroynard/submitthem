# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

"""Preemption tests, need to be run on a an actual cluster"""

import getpass
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import submitthem
from submitthem import Job

FILE = Path(__file__)
LOGS = FILE.parent / "logs" / f"{FILE.stem}_log"

log = logging.getLogger("preemption_main")
formatter = logging.Formatter("%(name)s %(levelname)s (%(asctime)s) - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(handler)


def clock(partition: str, duration: int):
    log = logging.getLogger(f"preemption_{partition}")
    tick_tack = ["tick", "tack"]
    try:
        for minute in range(duration - 5):
            log.info(tick_tack[minute % 2])
            time.sleep(60)
        logging.warning("*** Exited peacefully ***")
        return duration
    except:
        logging.warning(f"!!! Interrupted on: {datetime.now().isoformat()}")
        raise


def pascal_job(partition: str, timeout_min: int, node: str = "") -> Job:
    """Submit a job with specific constraint that we can preempt deterministically."""
    ex = submitthem.AutoExecutor(folder=LOGS, slurm_max_num_timeout=1)
    ex.update_parameters(
        name=f"submitthem_preemption_{partition}",
        timeout_min=timeout_min,
        mem_gb=7,
        slurm_constraint="pascal",
        slurm_comment="submitthem integration test",
        slurm_partition=partition,
        slurm_mail_type="REQUEUE,BEGIN",
        slurm_mail_user=f"{getpass.getuser()}+slurm@meta.com",
        # pascal nodes have 80 cpus.
        # By requesting 50 we now that their can be only one such job with this property.
        cpus_per_task=50,
        slurm_additional_parameters={},
    )
    if node:
        ex.update_parameters(nodelist=node)

    return ex.submit(clock, partition, timeout_min)


def wait_job_is_running(job: Job) -> None:
    while job.state in ("UNKNOWN", "PENDING"):
        log.info(f"{job} is not RUNNING")
        time.sleep(60)


def preemption():
    """
    Run an integration test that verifies job preemption behavior.
    This function:
    - Submits a "learnlab" job and waits until it is running.
    - Determines the node where the job runs and then submits a higher-priority "devlab"
        job pinned to the same node to force preemption.
    - Waits for the priority job to start and examines the original job's stderr for
        an interruption message.
    - Asserts that exactly one interruption line appears and that the original job's
        state reflects being preempted (expected to be PENDING).
    - Waits for the priority job to finish before returning.
    Notes on interrupted_ts:
    - The variable interrupted_ts holds the timestamp (as a parsed string) extracted from
        the interruption message line matching "!!! Interrupted on: <timestamp>".
    - Typical uses for interrupted_ts include logging and debugging, correlating the
        interruption event with other logs, asserting timing/ordering constraints between
        events (e.g., that the preemption occurred within an expected window), or making
        further assertions about the system behavior after the interruption.
    Side effects:
    - Submits real cluster jobs and blocks until the priority job completes.
    - May raise AssertionError if expected interruption conditions are not met.
    """
    job = pascal_job("learnlab", timeout_min=2 * 60)
    log.info(f"Scheduled {job}, {job.paths.stdout}")
    # log.info(job.paths.submission_file.read_text())

    wait_job_is_running(job)
    node = job.get_info()["NodeList"]
    log.info(f"{job} ({job.state}) is runnning on {node} !")
    # Schedule another pascal job on the same node, whith high priority
    priority_job = pascal_job("devlab", timeout_min=15, node=node)
    log.info(f"Schedule {priority_job} ({job.state}) on {node} with high priority.")
    wait_job_is_running(priority_job)

    # if priority_job is running, then job should have been preempted
    learfair_stderr = job.stderr()
    assert learfair_stderr is not None, job.paths.stderr

    log.info(
        f"Job {priority_job} ({priority_job.state}) started, "
        f"job {job} ({job.state}) should have been preempted: {learfair_stderr}"
    )
    interruptions = [line for line in learfair_stderr.splitlines() if "Interrupted" in line]
    assert len(interruptions) == 1, interruptions
    assert job.state in ("PENDING"), job.state

    interrupted_ts = interruptions[0].split("!!! Interrupted on: ")[-1]
    try:
        interrupted_dt = datetime.fromisoformat(interrupted_ts)
    except ValueError:
        log.warning(f"Could not parse interruption timestamp: {interrupted_ts}")
    else:
        elapsed = datetime.now() - interrupted_dt
        log.info(
            f"Interruption recorded at {interrupted_dt.isoformat()} ({elapsed.total_seconds():.1f}s ago)"
        )
        assert elapsed.total_seconds() < 3600, "Interruption timestamp is unexpectedly old"

    priority_job.result()
    print("Preemption test succeeded ✅")


def main():
    log.info("Hello !")
    if LOGS.exists():
        log.info(f"Cleaning up log folder: {LOGS}")
        shutil.rmtree(str(LOGS))

    preemption()


if __name__ == "__main__":
    main()
