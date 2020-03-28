import enum
import logging
import os
import pathlib
import pwd
from configparser import ConfigParser
from typing import Dict, Optional, NamedTuple

import htcondor

from execution_engine2.exceptions import MissingCondorRequirementsException
from execution_engine2.exceptions import MissingRunJobParamsException
from execution_engine2.utils.Scheduler import Scheduler

logging.getLogger()


class JobInfo(NamedTuple):
    info: Optional[Dict]
    error: Optional[Exception]


class SubmissionInfo(NamedTuple):
    clusterid: Optional[str]
    submit: Dict
    error: Optional[Exception]


class CondorResources(NamedTuple):
    request_cpus: str
    request_disk: str
    request_memory: str
    client_group: str


class Condor(Scheduler):
    # TODO: Should these be outside of the class?
    REQUEST_CPUS = "request_cpus"
    REQUEST_MEMORY = "request_memory"
    REQUEST_DISK = "request_disk"
    CG = "+CLIENTGROUP"
    EE2 = "execution_engine2"
    ENDPOINT = "kbase-endpoint"
    EXTERNAL_URL = "external-url"
    EXECUTABLE = "executable"
    CATALOG_TOKEN = "catalog-token"
    DOCKER_TIMEOUT = "docker_timeout"
    POOL_USER = "pool_user"
    INITIAL_DIR = "initialdir"
    LEAVE_JOB_IN_QUEUE = "leavejobinqueue"
    TRANSFER_INPUT_FILES = "transfer_input_files"
    PYTHON_EXECUTABLE = "PYTHON_EXECUTABLE"
    DEFAULT_CLIENT_GROUP = "default_client_group"

    class JobStatusCodes(enum.Enum):
        UNEXPANDED = 0
        IDLE = 1
        RUNNING = 2
        REMOVED = 3
        COMPLETED = 4
        HELD = 5
        SUBMISSION_ERROR = 6
        NOT_FOUND = -1

    jsc = {
        "0": "Unexepanded",
        1: "Idle",
        2: "Running",
        3: "Removed",
        4: "Completed",
        5: "Held",
        6: "Submission_err",
        -1: "Not found in condor",
    }

    def __init__(self, config_filepath):
        self.config = ConfigParser()
        self.config.read(config_filepath)
        self.ee_endpoint = self.config.get(section=self.EE2, option=self.EXTERNAL_URL)
        self.python_executable = self.config.get(
            section=self.EE2,
            option=self.PYTHON_EXECUTABLE,
            fallback="/miniconda/bin/python",
        )
        self.initial_dir = self.config.get(
            section=self.EE2, option=self.INITIAL_DIR, fallback="/condor_shared"
        )
        executable = self.config.get(section=self.EE2, option=self.EXECUTABLE)
        if not pathlib.Path(executable).exists() and not pathlib.Path(
            self.initial_dir + "/" + executable
        ):
            raise FileNotFoundError(executable)
        self.executable = executable
        self.catalog_token = self.config.get(
            section=self.EE2, option=self.CATALOG_TOKEN
        )
        self.docker_timeout = self.config.get(
            section=self.EE2, option=self.DOCKER_TIMEOUT, fallback="604801"
        )
        self.pool_user = self.config.get(
            section=self.EE2, option=self.POOL_USER, fallback="condor_pool"
        )
        self.leave_job_in_queue = self.config.get(
            section=self.EE2, option=self.LEAVE_JOB_IN_QUEUE, fallback="True"
        )
        self.transfer_input_files = self.config.get(
            section=self.EE2,
            option=self.TRANSFER_INPUT_FILES,
            fallback="/condor_shared/JobRunner.tgz",
        )

    def setup_environment_vars(self, params: Dict) -> str:
        # 7 day docker job timeout default, Catalog token used to get access to volume mounts
        dm = (
            str(params["cg_resources_requirements"].get("debug_mode", "")).lower()
            == "true"
        )

        environment_vars = {
            "DOCKER_JOB_TIMEOUT": self.docker_timeout,
            "KB_ADMIN_AUTH_TOKEN": self.catalog_token,
            "KB_AUTH_TOKEN": params.get("token"),
            "CLIENTGROUP": params.get("extracted_client_group"),
            "JOB_ID": params.get("job_id"),
            # "WORKDIR": f"{config.get('WORKDIR')}/{params.get('USER')}/{params.get('JOB_ID')}",
            "CONDOR_ID": "$(Cluster).$(Process)",
            "PYTHON_EXECUTABLE": self.python_executable,
            "DEBUG_MODE": str(dm),
        }

        environment = ""
        for key, val in environment_vars.items():
            environment += f"{key}={val} "

        return f'"{environment}"'

    @staticmethod
    def check_for_missing_runjob_params(params: Dict[str, str]) -> None:
        """
        Check for missing runjob parameters
        :param params: Params saved when the job was created
        """
        for item in ("token", "user_id", "job_id", "cg_resources_requirements"):
            if item not in params:
                raise MissingRunJobParamsException(f"{item} not found in params")

    def extract_resources(self, cgrr: Dict[str, str]) -> CondorResources:
        """
        Checks to see if request_cpus/memory/disk is available
        If not, it sets them based on defaults from the config
        :param cgrr:
        :return:
        """
        logging.debug(f"About to extract from {cgrr}")

        client_group = cgrr.get("client_group", "")
        if client_group is None or client_group == "":
            client_group = self.config.get(
                section="DEFAULT", option=self.DEFAULT_CLIENT_GROUP
            )

        if client_group not in self.config.sections():
            raise ValueError(f"{client_group} not found in {self.config.sections()}")

        # TODO Validate that they are a resource followed by a unit
        for key in [self.REQUEST_DISK, self.REQUEST_CPUS, self.REQUEST_MEMORY]:
            if key not in cgrr or cgrr[key] in ["", None]:
                cgrr[key] = self.config.get(section=client_group, option=key)

        cr = CondorResources(
            self.REQUEST_CPUS,
            str(cgrr.get(self.REQUEST_DISK)),
            str(cgrr.get(self.REQUEST_MEMORY)),
            client_group,
        )

        return cr

    def extract_requirements(
        self, cgrr: Optional[dict] = None, client_group: Optional[str] = None
    ):
        """

        :param cgrr: Client Groups and Resource Requirements
        :param client_group: Client Group
        :return: A list of condor submit file requirements in (key == value) format
        """
        if cgrr is None or client_group is None:
            raise MissingCondorRequirementsException(
                "Please provide normalized cgrr and client_group"
            )

        requirements_statement = []

        # Default to using a regex
        if str(cgrr.get("client_group_regex", True)).lower() == "true":
            requirements_statement.append(f'regexp("{client_group}",CLIENTGROUP)')
        else:
            requirements_statement.append(f'(CLIENTGROUP == "{client_group}")')

        restricted_requirements = [
            "client_group",
            "client_group_regex",
            self.REQUEST_MEMORY,
            self.REQUEST_DISK,
            self.REQUEST_CPUS,
            "debug_mode",
        ]

        for key, value in cgrr.items():
            if key.lower() not in restricted_requirements:
                requirements_statement.append(f'({key} == "{value}")')

        return requirements_statement

    def create_submit(self, params: Dict):
        self.check_for_missing_runjob_params(params)
        sub = dict()
        sub["JobBatchName"] = params.get("job_id")
        sub[self.LEAVE_JOB_IN_QUEUE] = self.leave_job_in_queue
        sub["initial_dir"] = self.initial_dir
        sub["executable"] = f"{self.initial_dir}/{self.executable}"  # Must exist
        sub["arguments"] = f"{params['job_id']} {self.ee_endpoint}"
        sub["universe"] = "vanilla"
        sub["+AccountingGroup"] = f'{params.get("user_id")}'
        sub["Concurrency_Limits"] = params.get("user_id")
        sub["+Owner"] = f'"{self.pool_user}"'  # Must be quoted
        sub["ShouldTransferFiles"] = "YES"
        sub["transfer_input_files"] = self.transfer_input_files
        sub["When_To_Transfer_Output"] = "ON_EXIT"
        # If a job exits incorrectly put it on hold
        sub["on_exit_hold"] = "ExitCode =!= 0"
        #  Allow up to 12 hours of no response from job
        sub["JobLeaseDuration"] = "43200"
        #  Allow up to 12 hours for condor drain
        sub["MaxJobRetirementTime"] = "43200"
        # Remove jobs running longer than 7 days
        sub["Periodic_Remove"] = "( RemoteWallClockTime > 604800 )"
        cgrr = params["cg_resources_requirements"]
        # Extract minimum condor resource requirements and client_group
        resources = self.extract_resources(cgrr)
        sub["request_cpus"] = resources.request_cpus
        sub["request_memory"] = resources.request_memory
        sub["request_disk"] = resources.request_disk
        client_group = resources.client_group
        # Set requirements statement
        requirements = self.extract_requirements(cgrr=cgrr, client_group=client_group)
        sub["requirements"] = " && ".join(requirements)
        params["extracted_client_group"] = client_group
        sub = self.extract_special_items(sub=sub, params=params)
        sub["+KB_CLIENTGROUP"] = f'"{client_group}"'
        sub["getenv"] = "false"
        sub["environment"] = self.setup_environment_vars(params)

        # Ensure all values are a string
        for item in sub.keys():
            sub[item] = str(sub[item])
        return sub

    @staticmethod
    def extract_special_items(sub: Dict, params: Dict[str, str]):
        sub["+KB_PARENT_JOB_ID"] = params.get("parent_job_id", "")
        sub["+KB_MODULE_NAME"] = params.get("method", "").split(".")[0]
        sub["+KB_FUNCTION_NAME"] = params.get("method", "").split(".")[-1]
        sub["+KB_APP_ID"] = params.get("app_id", "")
        sub["+KB_APP_MODULE_NAME"] = params.get("app_id", "").split("/")[0]
        sub["+KB_WSID"] = params.get("wsid", "")
        sub["+KB_SOURCE_WS_OBJECTS"] = ",".join(params.get("source_ws_objects", list()))

        # Ensure double quoted user inputs
        for key in sub.keys():
            if "+KB" in key:
                value = sub[key]
                if value != "":
                    sub[key] = f'"{value}"'

        return sub

    def run_job(self, params: Dict[str, str], submit_file: Dict[str, str] = None):
        """
        TODO: Add a retry
        TODO: Add list of required params
        :param params:  Params to run the job, such as the username, job_id, token, client_group_and_requirements
        :param submit_file:
        :return:
        """
        if submit_file is None:
            submit_file = self.create_submit(params)

        return self.run_submit(submit_file)

    def run_submit(self, submit: Dict[str, str]):

        sub = htcondor.Submit(submit)
        try:
            schedd = htcondor.Schedd()
            logging.debug(schedd)
            logging.debug(submit)
            logging.debug(os.getuid())
            logging.debug(pwd.getpwuid(os.getuid()).pw_name)
            logging.debug(submit)
            with schedd.transaction() as txn:
                return SubmissionInfo(str(sub.queue(txn, 1)), sub, None)
        except Exception as e:
            return SubmissionInfo(None, sub, e)

    def get_job_info(
        self, job_id: Optional[str] = None, cluster_id: Optional[str] = None
    ):
        if job_id is not None and cluster_id is not None:
            return JobInfo(
                {}, Exception("Use only batch name (job_id) or cluster_id, not both")
            )

        constraint = None
        if job_id:
            constraint = f"JobBatchName=?={job_id}"
        if cluster_id:
            constraint = f"ClusterID=?={cluster_id}"

        try:
            job = htcondor.Schedd().query(constraint=constraint, limit=1)[0]
            return JobInfo(job, None)
        except Exception as e:
            return JobInfo({}, e)

    def get_user_info(self, user_id, projection=None):
        pass

    def cancel_job(self, job_id: str) -> bool:
        """

        :param job_id:
        :return:
        """
        return self.cancel_jobs([f"{job_id}"])

    def cancel_jobs(self, scheduler_ids: list):
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
            cancel_jobs = htcondor.Schedd().act(
                htcondor.JobAction.Remove, scheduler_ids
            )
            logging.debug(f"{cancel_jobs}")
            return cancel_jobs
        except Exception as e:
            logging.error("Couldn't cancel jobs" + str(scheduler_ids))
            logging.error(e)
            return False
