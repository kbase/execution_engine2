import json
import os
from shutil import copyfile

import requests
from configparser import ConfigParser
from datetime import datetime
from dotenv import load_dotenv

from execution_engine2.db.models.models import Job, JobInput, Meta
from execution_engine2.db.models.models import Status
from execution_engine2.exceptions import MalformedTimestampException


def get_example_job(
    user: str = "boris", wsid: int = 123, authstrat: str = "kbaseworkspace"
) -> Job:
    j = Job()
    j.user = user
    j.wsid = wsid
    job_input = JobInput()
    job_input.wsid = j.wsid

    job_input.method = "method"
    job_input.requested_release = "requested_release"
    job_input.params = {}
    job_input.service_ver = "dev"
    job_input.app_id = "super_module.super_function"

    m = Meta()
    m.cell_id = "ApplePie"
    job_input.narrative_cell_info = m
    j.job_input = job_input
    j.status = "queued"
    j.authstrat = authstrat

    return j


def _create_sample_params(self):
    params = dict()
    params["job_id"] = self.job_id
    params["user"] = "kbase"
    params["token"] = "test_token"
    params["client_group_and_requirements"] = "njs"
    return params


def read_config_into_dict(config="deploy.cfg", section="execution_engine2"):
    config_parser = ConfigParser()
    config_parser.read(config)
    config = dict()
    for key, val in config_parser[section].items():
        config[key] = val
    return config


def bootstrap():
    test_env = "test.env"
    pwd = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    # Copy a fresh test env each time
    copyfile(f"{pwd}/test/env/{test_env}", f"{test_env}")
    load_dotenv("test.env", verbose=True)


# flake8: noqa: C901
def validate_job_state(state):
    """
    Validates whether a returned Job State has all the required fields with the right format.
    If all is well, returns True,
    otherwise this prints out errors to the command line and returns False.
    Can be just used with assert in tests, like "assert validate_job_state(state)"
    """
    required_fields = {
        "job_id": str,
        "user": str,
        "wsid": int,
        "authstrat": str,
        "job_input": dict,
        "updated": int,
        "created": int,
        "status": str,
    }

    optional_fields = {
        "estimating": int,
        "queued": int,
        "running": int,
        "finished": int,
        "error_code": int,
        "terminated_code": int,
        "errormsg": str,
    }

    timestamp_fields = [
        "created",
        "updated",
        "estimating",
        "queued",
        "running",
        "completed",
    ]

    # fields that have to be present based on the context of different statuses

    valid_statuses = vars(Status)["_member_names_"]

    status_context = {
        "estimating": ["estimating"],
        "running": ["running"],
        "completed": ["completed"],
        "error": ["error_code", "errormsg"],
        "terminated": ["terminated_code"],
    }

    # 1. Make sure required fields are present and of the correct type
    missing_reqs = list()
    wrong_reqs = list()
    for req in required_fields.keys():
        if req not in state:
            missing_reqs.append(req)
        elif not isinstance(state[req], required_fields[req]):
            wrong_reqs.append(req)

    if missing_reqs or wrong_reqs:
        print(f"Job state is missing required fields: {missing_reqs}.")
        for req in wrong_reqs:
            print(
                f"Job state has faulty req - {req} should be of type {required_fields[req]}, but had value {state[req]}."
            )
        return False

    # 2. Make sure that context-specific fields are present and the right type
    status = state["status"]
    if status not in valid_statuses:
        print(f"Job state has invalid status {status}.")
        return False

    if status in status_context:
        context_fields = status_context[status]
        missing_context = list()
        wrong_context = list()
        for field in context_fields:
            if field not in state:
                missing_context.append(field)
            elif not isinstance(state[field], optional_fields[field]):
                wrong_context.append(field)
        if missing_context or wrong_context:
            print(f"Job state is missing status context fields: {missing_context}.")
            for field in wrong_context:
                print(
                    f"Job state has faulty context field - {field} should be of type {optional_fields[field]}, but had value {state[field]}."
                )
            return False

    # 3. Make sure timestamps are really timestamps
    bad_ts = list()
    for ts_type in timestamp_fields:
        if ts_type in state:
            is_second_ts = is_timestamp(state[ts_type])
            if not is_second_ts:
                print(state[ts_type], "is not a second ts")

            is_ms_ts = is_timestamp(state[ts_type] / 1000)
            if not is_ms_ts:
                print(state[ts_type], "is not a millisecond ts")

            if not is_second_ts and not is_ms_ts:
                bad_ts.append(ts_type)

    if bad_ts:
        for ts_type in bad_ts:
            print(
                f"Job state has a malformatted timestamp: {ts_type} with value {state[ts_type]}"
            )
        raise MalformedTimestampException()

    return True


def is_timestamp(ts: int):
    """
    Simple enough - if dateutil.parser likes the string, it's a time string and we return True.
    Otherwise, return False.
    """
    try:
        datetime.fromtimestamp(ts)
        return True
    except ValueError:
        return False


def custom_ws_perm_maker(user_id: str, ws_perms: dict):
    """
    Returns an Adapter for requests_mock that deals with mocking workspace permissions.
    :param user_id: str - the user id
    :param ws_perms: dict of permissions, keys are ws ids, values are permission. Example:
        {123: "a", 456: "w"} means workspace id 123 has admin permissions, and 456 has
        write permission
    :return: an adapter function to be passed to request_mock
    """

    def perm_adapter(request):
        perms_req = request.json().get("params")[0].get("workspaces")
        ret_perms = []
        for ws in perms_req:
            ret_perms.append({user_id: ws_perms.get(ws["id"], "n")})
        response = requests.Response()
        response.status_code = 200
        response._content = bytes(
            json.dumps({"result": [{"perms": ret_perms}], "version": "1.1"}), "UTF-8"
        )
        return response

    return perm_adapter
