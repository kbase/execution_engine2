"""
Parameters for submitting a job to a scheduler.
"""

from maps import FrozenMap
from typing import Dict
from execution_engine2.utils.arg_processing import check_string as _check_string
from execution_engine2.exceptions import IncorrectParamsException


def _gt_zero(num: int, name: str) -> int:
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
