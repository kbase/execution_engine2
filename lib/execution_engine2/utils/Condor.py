"""
Authors @bsadkhin
Functions to call condor to manage jobs and extract resource requirements
"""
import logging
import os
import pathlib
import pwd
from typing import Dict, Optional, Any

import htcondor

from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements,
)
from lib.execution_engine2.utils.CondorTuples import (
    SubmissionInfo,
    JobInfo,
)
from execution_engine2.utils.arg_processing import not_falsy as _not_falsy


class Condor:
    # TODO: Should these be outside of the class?
    CG = "+CLIENTGROUP"
    EXTERNAL_URL = "external-url"
    EXECUTABLE = "executable"
    CATALOG_TOKEN = "catalog-token"
    DOCKER_TIMEOUT = "docker_timeout"
    POOL_USER = "pool_user"
    INITIAL_DIR = "initialdir"
    LEAVE_JOB_IN_QUEUE = "leavejobinqueue"
    TRANSFER_INPUT_FILES = "transfer_input_files"
    PYTHON_EXECUTABLE = "PYTHON_EXECUTABLE"

    def __init__(self, config: Dict[str, str], htc=htcondor):
        """
        Create the condor wrapper.

        config - the execution_engine2 configuration.
        htc - the htcondor module, or an alternate implementation or mock.
        """
        # TODO some nicer error messages for the required keys vs. just KeyError
        self.htcondor = htc
        self.ee_endpoint = config[self.EXTERNAL_URL]
        self.python_executable = config.get(
            self.PYTHON_EXECUTABLE, "/miniconda/bin/python"
        )
        self.initial_dir = config.get(self.INITIAL_DIR, "/condor_shared")
        self.executable = config[self.EXECUTABLE]
        if not pathlib.Path(self.executable).exists() and not pathlib.Path(
            self.initial_dir + "/" + self.executable
        ):
            raise FileNotFoundError(self.executable)
        self.catalog_token = config[self.CATALOG_TOKEN]
        self.docker_timeout = config.get(self.DOCKER_TIMEOUT, "604801")
        self.pool_user = config.get(self.POOL_USER, "condor_pool")
        self.leave_job_in_queue = config.get(self.LEAVE_JOB_IN_QUEUE, "True")
        self.transfer_input_files = config.get(
            self.TRANSFER_INPUT_FILES, "/condor_shared/JobRunner.tgz"
        )
        self.logger = logging.getLogger("ee2")

    def _setup_environment_vars(self, params: JobSubmissionParameters) -> str:
        # 7 day docker job timeout default, Catalog token used to get access to volume mounts
        environment_vars = {
            "DOCKER_JOB_TIMEOUT": self.docker_timeout,
            "KB_ADMIN_AUTH_TOKEN": self.catalog_token,
            "KB_AUTH_TOKEN": params.user_creds.token,
            "CLIENTGROUP": params.job_reqs.client_group,
            "JOB_ID": params.job_id,
            # "WORKDIR": f"{config.get('WORKDIR')}/{params.get('USER')}/{params.get('JOB_ID')}",
            "CONDOR_ID": "$(Cluster).$(Process)",
            "PYTHON_EXECUTABLE": self.python_executable,
            "DEBUG_MODE": str(params.job_reqs.debug_mode),
            "PARENT_JOB_ID": params.parent_job_id or "",
        }

        environment = ""
        for key, val in environment_vars.items():
            environment += f"{key}={val} "

        return f'"{environment}"'

    @staticmethod
    def _add_hardcoded_attributes(sub, job_id):
        sub["universe"] = "vanilla"
        sub["ShouldTransferFiles"] = "YES"
        # If a job exits incorrectly put it on hold
        sub["on_exit_hold"] = "ExitCode =!= 0"
        #  Allow up to 12 hours of no response from job
        sub["JobLeaseDuration"] = "43200"
        #  Allow up to 12 hours for condor drain
        sub["MaxJobRetirementTime"] = "43200"
        # Remove jobs running longer than 7 days
        sub["Periodic_Hold"] = "( RemoteWallClockTime > 604800 )"
        sub["log"] = "runner_logs/$(Cluster).$(Process).log"
        err_file = f"{job_id}.err"
        out_file = f"{job_id}.out"
        err_path = f"runner_logs/{err_file}"
        out_path = f"runner_logs/{out_file}"
        err_path_remap = f"cluster_logs/{err_file}"
        out_path_remap = f"cluster_logs/{out_file}"
        sub["error"] = err_path
        sub["output"] = out_path
        remap = f'"{err_path}={err_path_remap};{out_path}={out_path_remap}"'
        sub["transfer_output_remaps"] = remap
        sub["When_To_Transfer_Output"] = "ON_EXIT_OR_EVICT"
        sub["getenv"] = "false"
        return sub

    def _add_configurable_attributes(self, sub):
        sub[self.LEAVE_JOB_IN_QUEUE] = self.leave_job_in_queue
        sub["initial_dir"] = self.initial_dir
        sub["+Owner"] = f'"{self.pool_user}"'  # Must be quoted
        sub["executable"] = f"{self.initial_dir}/{self.executable}"  # Must exist
        sub["transfer_input_files"] = self.transfer_input_files
        return sub

    def _extract_resources_and_requirements(
        self, sub: Dict[str, Any], job_reqs: JobRequirements
    ) -> Dict[str, Any]:
        # Extract minimum condor resource requirements and client_group
        sub["request_cpus"] = job_reqs.cpus
        sub["request_memory"] = f"{job_reqs.memory_MB}MB"
        sub["request_disk"] = f"{job_reqs.disk_GB}GB"
        # Set requirements statement
        sub["requirements"] = self._create_requirements_statement(job_reqs)
        sub["+KB_CLIENTGROUP"] = f'"{job_reqs.client_group}"'
        return sub

    def _create_requirements_statement(self, job_reqs: JobRequirements) -> str:
        reqs = []
        if job_reqs.client_group_regex is not False:
            # Default is True, so a value of None means True
            reqs = [f'regexp("{job_reqs.client_group}",CLIENTGROUP)']
        else:
            reqs = [f'(CLIENTGROUP == "{job_reqs.client_group}")']
        for key, value in job_reqs.scheduler_requirements.items():
            reqs.append(f'({key} == "{value}")')
        return " && ".join(reqs)

    def _add_resources_and_special_attributes(
        self, params: JobSubmissionParameters
    ) -> Dict[str, str]:
        sub = dict()
        sub["JobBatchName"] = params.job_id
        sub["arguments"] = f"{params.job_id} {self.ee_endpoint}"
        sub = self._add_job_labels(sub=sub, params=params)
        # Extract special requirements
        sub = self._extract_resources_and_requirements(sub, params.job_reqs)

        btu = params.job_reqs.bill_to_user
        user = btu if btu else params.user_creds.username
        if not params.job_reqs.ignore_concurrency_limits:
            sub["Concurrency_Limits"] = user
        sub["+AccountingGroup"] = f'"{user}"'

        sub["environment"] = self._setup_environment_vars(params)

        return sub

    def _create_submit(self, params: JobSubmissionParameters) -> Dict[str, str]:
        # note some tests call this function directly and will need to be updated if the
        # signature is changed

        sub = self._add_resources_and_special_attributes(params)
        sub = self._add_hardcoded_attributes(sub=sub, job_id=params.job_id)
        sub = self._add_configurable_attributes(sub)
        # Ensure all values are a string
        for item in sub.keys():
            sub[item] = str(sub[item])
        return sub

    @staticmethod
    def _add_job_labels(sub: Dict, params: JobSubmissionParameters):
        sub["+KB_PARENT_JOB_ID"] = params.parent_job_id or ""
        sub["+KB_MODULE_NAME"] = params.app_info.module
        sub["+KB_FUNCTION_NAME"] = params.app_info.method
        sub["+KB_APP_ID"] = params.app_info.get_application_id()
        sub["+KB_APP_MODULE_NAME"] = params.app_info.application_module
        sub["+KB_WSID"] = params.wsid or ""
        sub["+KB_SOURCE_WS_OBJECTS"] = ",".join(params.source_ws_objects)

        # Ensure double quoted user inputs
        for key in sub.keys():
            if "+KB" in key:
                value = sub[key]
                if value != "":
                    sub[key] = f'"{value}"'

        return sub

    def run_job(self, params: JobSubmissionParameters) -> SubmissionInfo:
        """
        TODO: Add a retry
        TODO: Add list of required params
        :param params:  Params to run the job.
        :return: ClusterID, Submit File, and Info about Errors
        """
        # Contains sensitive information to be sent to condor
        submit = self._create_submit(_not_falsy(params, "params"))

        sub = self.htcondor.Submit(submit)
        try:
            schedd = self.htcondor.Schedd()
            with schedd.transaction() as txn:
                return SubmissionInfo(str(sub.queue(txn, 1)), sub, None)
        except Exception as e:
            return SubmissionInfo(None, sub, e)

    def get_job_resource_info(
        self, job_id: Optional[str] = None, cluster_id: Optional[str] = None
    ) -> Dict[str, str]:
        if job_id is not None and cluster_id is not None:
            raise Exception("Use only batch name (job_id) or cluster_id, not both")

        condor_stats = self._get_job_info(job_id=job_id, cluster_id=cluster_id)
        # Don't leak token into the logs here
        job_info = condor_stats.info
        if job_info is None:
            return {}

        disk_keys = ["RemoteUserCpu", "DiskUsage_RAW", "DiskUsage"]
        cpu_keys = ["CpusUsage", "CumulativeRemoteSysCpu", "CumulativeRemoteUserCpu"]
        memory_keys = ["ResidentSetSize_RAW", "ResidentSetSize", "ImageSize_RAW"]

        resource_keys = disk_keys + cpu_keys + memory_keys
        held_job_keys = ["HoldReason", "HoldReasonCode"]
        machine_keys = ["RemoteHost", "LastRemoteHost"]
        time_keys = [
            "CommittedTime",
            "CommittedSuspensionTime",
            "CompletionDate",
            "CumulativeSuspensionTime",
            "CumulativeTransferTime",
            "JobCurrentFinishTransferInputDate",
            "JobCurrentFinishTransferOutputDate",
            "JobCurrentStartDate",
            "JobCurrentStartExecutingDate",
            "JobCurrentStartTransferInputDate",
            "JobCurrentStartTransferOutputDate",
        ]

        extracted_resources = dict()
        for key in resource_keys + held_job_keys + machine_keys + time_keys:
            extracted_resources[key] = job_info.get(key)

        return extracted_resources

    def _get_job_info(
        self, job_id: Optional[str] = None, cluster_id: Optional[str] = None
    ) -> JobInfo:
        # note some tests replace this function with a MagicMock and will need to be updated if
        # the signature is changed

        if job_id is not None and cluster_id is not None:
            return JobInfo(
                {}, Exception("Use only batch name (job_id) or cluster_id, not both")
            )

        constraint = None
        if job_id:
            constraint = f'JobBatchName=?="{job_id}"'
        if cluster_id:
            constraint = f"ClusterID=?={cluster_id}"
        self.logger.debug(
            f"About to get job info for {job_id} {cluster_id} {constraint}"
        )

        try:
            job = self.htcondor.Schedd().query(constraint=constraint, limit=1)
            if len(job) == 0:
                job = [{}]
            return JobInfo(info=job[0], error=None)
        except Exception as e:
            self.logger.debug({"constraint": constraint, "limit": 1})
            raise e
            # return JobInfo(info=None, error=e)

    def cancel_job(self, job_id: str) -> bool:
        """

        :param job_id:
        :return:
        """
        return self._cancel_jobs([f"{job_id}"])

    def _cancel_jobs(self, scheduler_ids: list):
        """
        Possible return structure like this
        [
            TotalJobAds = 10;
            TotalPermissionDenied = 0;
            TotalAlreadyDone = 0;
            TotalNotFound = 0;
            TotalSuccess = 1;
            TotalChangedAds = 1;
            TotalBadStatus = 0;
            TotalError = 0
        ]
        :param scheduler_ids:  List of string of condor job ids to cancel
        :return:
        """

        if not isinstance(scheduler_ids, list):
            raise Exception("Please provide a list of condor ids to cancel")

        try:
            cancel_jobs = self.htcondor.Schedd().act(
                action=self.htcondor.JobAction.Remove, job_spec=scheduler_ids
            )
            return cancel_jobs
        except Exception:
            self.logger.error(
                f"Couldn't cancel jobs f{scheduler_ids}", exc_info=True, stack_info=True
            )
            return False
