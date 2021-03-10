"""
Parameters for submitting a job to a scheduler.
"""

from maps import FrozenMap
from typing import Dict, List
from execution_engine2.utils.arg_processing import (
    check_string as _check_string,
    not_falsy as _not_falsy,
    not_falsy_in_iterable as _not_falsy_in_iterable,
)
from execution_engine2.utils.user_info import UserCreds
from execution_engine2.utils.application_info import AppInfo
from execution_engine2.exceptions import IncorrectParamsException


def _gt_zero(num: int, name: str, optional=False) -> int:
    if num is None and optional:
        return None
    if num is None or num < 1:
        raise IncorrectParamsException(f"{name} must be at least 1")
    return num


class JobRequirements:
    """
    Requirements for running a job on a scheduler.
    """

    def __init__(
        self,
        cpus: int,
        memory_MB: int,
        disk_GB: int,
        client_group: str,
        client_group_regex: bool = False,
        as_user: str = None,
        ignore_concurrency_limits: bool = False,
        scheduler_requirements: Dict[str, str] = None,
    ):
        """
        Create the job requirements.

        cpus - the number of CPUs required for the job.
        memory_MB - the amount of memory, in MB, required for the job.
        disk_GB - the amount of disk space, in GB, required for the job.
        client_group - the client group in which the job will run.
        client_group_regex - whether to treat the client group string as a regular expression
            that can match multiple client groups.
        as_user - run the job as an alternate user; take the user's username.
        ignore_concurrency_limits - allow the user to run this job even if the user's maximum
            job count has already been reached.
        scheduler_requirements - arbitrary requirements for the scheduler passed as key/value
            pairs. Requires knowledge of the scheduler API.
        """
        self.cpus = _gt_zero(cpus, "CPU count")
        self.memory_MB = _gt_zero(memory_MB, "memory in MB")
        self.disk_GB = _gt_zero(disk_GB, "disk space in GB")
        self.client_group = _check_string(client_group, "client_group")
        self.client_group_regex = client_group_regex
        self.as_user = _check_string(as_user, "as_user", optional=True)
        self.ignore_concurrency_limits = ignore_concurrency_limits
        sr = scheduler_requirements if scheduler_requirements else {}
        for key, value in sr.items():
            _check_string(key, "key in scheduler requirements structure")
            _check_string(
                value, f"value for key '{key}' in scheduler requirements structure"
            )
        self.scheduler_requirements = FrozenMap(sr)

    def __eq__(self, other):
        if type(self) == type(other):
            return (
                self.cpus,
                self.memory_MB,
                self.disk_GB,
                self.client_group,
                self.client_group_regex,
                self.as_user,
                self.ignore_concurrency_limits,
                self.scheduler_requirements,
            ) == (
                other.cpus,
                other.memory_MB,
                other.disk_GB,
                other.client_group,
                other.client_group_regex,
                other.as_user,
                other.ignore_concurrency_limits,
                other.scheduler_requirements,
            )
        return False

    def __hash__(self):
        return hash(
            (
                self.cpus,
                self.memory_MB,
                self.disk_GB,
                self.client_group,
                self.client_group_regex,
                self.as_user,
                self.ignore_concurrency_limits,
                self.scheduler_requirements,
            )
        )


# move this function somewhere else?
def _is_valid_UPA(upa: str) -> (str, bool):
    # returns an empty string if not a valid upa
    if upa is None or not upa.strip():
        return '', False
    parts = [p.strip() for p in upa.split('/')]
    if not len(parts) == 3:
        return '', False
    for p in parts:
        try:
            int(p)
        except ValueError:
            return '', False
    return '/'.join(parts), True


class JobSubmissionParameters:
    """
    Parameters for submitting a job to a job scheduler.
    """

    def __init__(
        self,
        job_id: str,
        app_info: AppInfo,
        job_reqs: JobRequirements,
        user_creds: UserCreds,
        parent_job_id: str = None,
        wsid: int = None,
        source_ws_objects: List[str] = None,
        debug_mode: bool = False,
    ):
        """
        Create the parameters.

        job_id - the ID of the job.
        app_info - information about the application to be run.
        job_reqs - requirements for the job.
        user_creds - user credentials.
        parent_job_id - the ID of the parent job to this job, if any.
        wsid - the ID of the workspace with which the job is associated, if any.
        source_ws_objects - workspace objects that are part of the job input.
        debug_mode - whether to run the job in debug mode.
        """
        self.job_id = _check_string(job_id, "job_id")
        self.app_info = _not_falsy(app_info, "app_info")
        self.job_reqs = _not_falsy(job_reqs, "job_reqs")
        self.user_creds = _not_falsy(user_creds, "user_creds")
        self.parent_job_id = _check_string(
            parent_job_id, "parent_job_id", optional=True
        )
        self.wsid = _gt_zero(wsid, "wsid", optional=True)
        source_ws_objects = source_ws_objects if source_ws_objects else []
        for i, ref in enumerate(source_ws_objects):
            upa, is_valid = _is_valid_UPA(ref)
            if not is_valid:
                raise IncorrectParamsException(
                    f"source_ws_objects index {i}, '{ref}', "
                    + 'is not a valid Unique Permanent Address')
            source_ws_objects[i] = upa
        self.source_ws_objects = tuple(source_ws_objects)
        self.debug_mode = debug_mode

    def __eq__(self, other):
        if type(self) == type(other):
            return (
                self.job_id,
                self.app_info,
                self.job_reqs,
                self.user_creds,
                self.parent_job_id,
                self.wsid,
                self.source_ws_objects,
                self.debug_mode,
            ) == (
                other.job_id,
                other.app_info,
                other.job_reqs,
                other.user_creds,
                other.parent_job_id,
                other.wsid,
                other.source_ws_objects,
                other.debug_mode,
            )
        return False

    def __hash__(self):
        return hash(
            (
                self.job_id,
                self.app_info,
                self.job_reqs,
                self.user_creds,
                self.parent_job_id,
                self.wsid,
                self.source_ws_objects,
                self.debug_mode,
            )
        )
