"""
Unit tests for the Retry Code
"""
from unittest.mock import create_autospec, MagicMock

from pytest import raises

from execution_engine2.exceptions import CannotRetryJob, RetryFailureException
from execution_engine2.sdk.EE2Runjob import EE2RunJob
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from test.utils_shared.test_utils import assert_exception_correct
from test.utils_shared.test_utils import get_example_job


def test_preflight():
    sdkmr = create_autospec(SDKMethodRunner, instance=True, spec_set=True)
    # Passing case with nothing to assert, all goes well
    good_job = get_example_job(status="error")
    sdkmr.get_job_with_permission = MagicMock(return_value=good_job)
    rj = EE2RunJob(sdkmr=sdkmr)
    rj.run_one_job({"method": "1"})


def test_retry_db_failures():
    """
    * Test correct db update failure message, and that cancel_job is called
    * Test that on exception, the db_update failure is called
    """
    sdkmr = MagicMock()
    retry_job = get_example_job(status="error")
    parent_job = get_example_job(status="error")
    retry_job.job_input.parent_job_id = "123"
    sdkmr.get_job_with_permission = MagicMock(return_value=retry_job)
    sdkmr.cancel_job = MagicMock()
    rj = EE2RunJob(sdkmr=sdkmr)

    # Check correct exception and that safe cancel/cancel_job is called
    job1 = "job1"
    job_to_abort = "job_to_abort"

    # Check to make sure cancel_job is called on failure
    with raises(Exception) as e:
        rj._db_update_failure(
            job_that_failed_operation="job1",
            job_to_abort="job_to_abort",
            exception=Exception(123),
        )
        expected_exception = RetryFailureException(
            f"Couldn't update job record:{job1} during retry. Aborting:{job_to_abort} Exception:123 "
        )
        assert_exception_correct(e.value, expected_exception)
    assert sdkmr.cancel_job.call_count == 1

    # Check to make sure safe_cancel_call is called on failure
    with raises(Exception) as e:
        rj._safe_cancel = MagicMock()
        rj._db_update_failure(
            job_that_failed_operation="job1",
            job_to_abort="job_to_abort",
            exception=Exception(123),
        )
        expected_exception = RetryFailureException(
            f"Couldn't update job record:{job1} during retry. Aborting:{job_to_abort} Exception:123 "
        )
        assert_exception_correct(e.value, expected_exception)
    assert rj._safe_cancel.call_count == 1

    rj.run_one_job = MagicMock(return_value=retry_job)
    # One DB failure
    rj._db_update_failure = MagicMock(side_effect=Exception("Boom!"))
    with raises(Exception):
        rj._retry(job_id=retry_job.id, job=retry_job, parent_job=parent_job)
    assert rj._db_update_failure.call_count == 1

    # Two db failures
    rj._db_update_failure = MagicMock()
    rj._retry(job_id=retry_job.id, job=retry_job, parent_job=parent_job)


def test_validate_retry():
    sdkmr = create_autospec(SDKMethodRunner, instance=True, spec_set=True)

    # Passing case with nothing to assert, all goes well
    good_job = get_example_job(status="error")
    sdkmr.get_job_with_permission = MagicMock(return_value=good_job)
    rj = EE2RunJob(sdkmr=sdkmr)
    rj._validate_retry_presubmit("unknown")

    # Fail case with the wrong status
    with raises(Exception) as e:
        sdkmr.get_job_with_permission = MagicMock(
            return_value=get_example_job(status="running")
        )
        rj = EE2RunJob(sdkmr=sdkmr)
        rj._validate_retry_presubmit("unknown")
    expected_exception = CannotRetryJob(
        "Error retrying job unknown with status running: can only retry jobs with "
        "status 'error' or 'terminated'",
    )
    assert_exception_correct(e.value, expected_exception)

    # Fail case with the batch job
    with raises(Exception) as e:
        good_job.batch_job = True
        sdkmr.get_job_with_permission = MagicMock(return_value=good_job)
        rj = EE2RunJob(sdkmr=sdkmr)
        rj._validate_retry_presubmit("unknown")

    expected_exception = CannotRetryJob(
        "Cannot retry batch job parents. Must retry individual jobs"
    )
    assert_exception_correct(e.value, expected_exception)


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
