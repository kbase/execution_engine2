# This file just for reference for manual test, not a unit test
import os
import time
from pprint import pprint

from installed_clients.execution_engine2Client import execution_engine2 as EE2

ee2 = EE2(url="https://ci.kbase.us/services/ee2", token=os.environ["KB_AUTH_TOKEN"])
wsid = 56410  # might need to add yours here
pprint("# This file just for reference for manual test, not a unit test")


def get_params(echo):
    return {
        "app_id": "echo_test/echo_test",
        "method": "echo_test.echo",
        "tag": "dev",
        "service_ver": "855fc0f0eedee131771b2fc65f74d6d40eda99e5",
        "cell_id": "fcbe5952-cccb-4083-b3d2-5567f52dfbb5",
        wsid: wsid,
        "params": [
            {
                "message": echo,
                "workspace_name": "You might need to add yours here",
            }
        ],
    }


def submit_echo_job2(echo):
    params = get_params(echo)
    try:
        job_id = ee2.run_job(params=params)
        print("Submitted echo job", job_id)
        return job_id
    except Exception as e:
        print("Failed to submit echo job", e)


def submit_echo_batch(echo):
    params1 = get_params("yo 1")
    params2 = get_params("yo 2")
    batch_params = {
        "wsid": wsid,
    }
    job_ids = ee2.run_job_batch(params=[params1, params2], batch_params=batch_params)
    return job_ids


def check_job(job_id):
    while True:
        time.sleep(5)
        # state = ee2.check_jobs(params={"job_ids": [job_id]})["job_states"]


def check_job_batch(job_ids):
    parent_job = [job_ids["parent_job_id"]]
    children_job_ids = job_ids["children_job_ids"]
    all_ids = parent_job + children_job_ids

    timeout = 600
    while True:
        time.sleep(5)
        timeout -= 5
        print("Checking state for", all_ids)
        # state = ee2.check_jobs(params={"job_ids": all_ids})["job_states"]
        # success_complete = True
        # for job in state:
        #     job_status = job['status']

        if timeout <= 0:
            raise Exception("Both jobs did not finish")


# This file just for reference for manual test, not a unit test

jids = submit_echo_batch("yo")
check_job_batch(jids)
