"""
Unit tests for the Retry Code
"""

from execution_engine2.sdk.EE2Runjob import EE2RunJob

from test.utils_shared.test_utils import get_example_job


def test_retry_get_run_job_params_from_existing_job():
    """
    Test to see that the retried job matches the job it got retried from the db
    Not all fields are expected back
    """
    example_job = get_example_job()
    example_job_as_dict = example_job.to_mongo().to_dict()
    extracted_job = EE2RunJob._get_run_job_params_from_existing_job(
        example_job, user_id=example_job.user + "other"
    )
    # Check Top Level Fields Match
    discarded_keys = [
        "user",
        "authstrat",
        "status",
        "job_input",
        "child_jobs",
        "batch_job",
    ]
    expected_unequal_keys = [
        "updated",
        "queued",
        "scheduler_id",
    ]
    for key in example_job_as_dict.keys():
        if key in discarded_keys:
            continue
        if key in expected_unequal_keys:
            if key in extracted_job:
                assert example_job_as_dict[key] != extracted_job[key]
        else:
            assert example_job_as_dict[key] == extracted_job[key]
