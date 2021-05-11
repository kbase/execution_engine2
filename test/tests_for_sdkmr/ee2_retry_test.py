"""
Unit tests for the Retry Code
"""

from execution_engine2.sdk.EE2Runjob import EE2RunJob

from test.utils_shared.test_utils import get_example_job, get_example_job_input


def test_get_job_input_params_from_existing_job():
    """
    Test to see that the retried job INPUTS match the job that got retried from the db
    Not all fields are expected back
    """
    wsid = 1234
    example_job_input = get_example_job_input(wsid)
    job_input_as_dict = example_job_input.to_mongo().to_dict()
    extracted_job_input = EE2RunJob._get_job_input_params_from_existing_job(
        example_job_input
    )

    expected_params = ["wsid", "method", "params", "service_ver", "narrative_cell_info"]
    for item in expected_params:
        assert job_input_as_dict[item] == extracted_job_input[item]

    # With non blank params
    example_job_input2 = get_example_job_input(wsid, params={"test": "123"})
    job_input_as_dict2 = example_job_input2.to_mongo().to_dict()
    extracted_job_input2 = EE2RunJob._get_job_input_params_from_existing_job(
        example_job_input2
    )

    for item in expected_params:
        assert job_input_as_dict2[item] == extracted_job_input2[item]


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
    for key, val in example_job_as_dict.items():
        # if isinstance(val,dict)
        if key in discarded_keys:
            continue
        if key in expected_unequal_keys:
            if key in extracted_job:
                assert example_job_as_dict[key] != extracted_job[key]
        else:

            assert example_job_as_dict[key] == extracted_job[key]

    deprecated_fields = ["requested_release"]

    # Check over each of the job input fields
    example_job_inputs = example_job_as_dict["job_input"]
    extracted_job_inputs = extracted_job["job_input"]
    for key, val in example_job_inputs.items():
        if key in deprecated_fields:
            continue

        if example_job_inputs[key]:
            ej_value = example_job_inputs[key]
            if ej_value == []:
                # It might not be copied if optional, but this behavior could be normalized with a refactor
                continue
            assert ej_value == extracted_job_inputs[key]
