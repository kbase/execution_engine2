'''
This script is used to test running jobs and submitting jobs
'''
import os
import time
from pprint import pprint

from installed_clients.execution_engine2Client import execution_engine2 as EE2

ee2 = EE2(url="https://ci.kbase.us/services/ee3", token=os.environ['KB_AUTH_TOKEN'])
wsid = 56410


def get_params(echo):
    return {'app_id': 'echo_test/echo_test',
            'method': 'echo_test.echo',
            'tag': 'dev',
            'service_ver': "855fc0f0eedee131771b2fc65f74d6d40eda99e5",
            'cell_id': 'fcbe5952-cccb-4083-b3d2-5567f52dfbb5',

            wsid: wsid,
            'params': [{'message': echo,
                        "workspace_name": "bsadkhin:narrative_1579889684690",
                        }]
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
    start_time = time.time()
    print("Begin submit at",start_time)
    params_list = []
    for i in range(10):
        params_list.append(get_params(f"yo {i}"))
    batch_params = {'wsid': wsid, }
    job_ids = ee2.run_job_batch(params=params_list, batch_params=batch_params)

    end_time = time.time()
    print(f"End submit submit at {end_time}", end_time - start_time)
    return job_ids


def check_job(job_id):
    while (True):
        time.sleep(5)
        state = ee2.check_jobs(params={'job_ids': [job_id]})['job_states']


def check_job_batch(job_ids):
    parent_job = [job_ids['parent_job_id']]
    children_job_ids = job_ids['child_job_ids']
    all_ids = parent_job + children_job_ids

    print("Checking state for", all_ids)
    state = ee2.check_jobs(params={'job_ids': all_ids})['job_states']
    for job in state:
        print(job['job_id'])
        print(job)

    ee2.cancel_job({'job_id' :job_ids['parent_job_id']  })

    state = ee2.check_jobs(params={'job_ids': all_ids})['job_states']
    for job in state:
        print(job['job_id'])
        print(job)


jids = submit_echo_batch('yo')
check_job_batch(jids)
