import enum
import json
from collections import namedtuple
from configparser import ConfigParser

import htcondor
import logging

logging.basicConfig(level=logging.INFO)

from execution_engine2.exceptions import *
from execution_engine2.utils.Scheduler import Scheduler
import os, pwd
import pathlib


class Condor(Scheduler):
    job_info = namedtuple("job_info", "info error")
    submission_info = namedtuple("submission_info", "clusterid submit error")
    job_resource = namedtuple("job_resource", "amount unit")
    resource_requirements = namedtuple(
        "resource_requirements",
        "request_cpus request_disk request_memory requirements_statement",
    )
    condor_resources = namedtuple(
        "condor_resources", "request_cpus request_memory request_disk client_group"
    )

    # TODO: Should these be outside of the class?
    REQUEST_CPUS = "request_cpus"
    REQUEST_MEMORY = "request_memory"
    REQUEST_DISK = "request_disk"
    CG = "+CLIENTGROUP"
    EE2 = "execution_engine2"
    ENDPOINT = "kbase-endpoint"
    EXECUTABLE = "executable"
    AUTH_TOKEN = "KB_ADMIN_AUTH_TOKEN"
    DOCKER_TIMEOUT = "docker_timeout"
    POOL_USER = "pool_user"
    INITIAL_DIR = "initial_dir"
    LEAVE_JOB_IN_QUEUE = "leavejobinqueue"
    TRANSFER_INPUT_FILES = "transfer_input_files"

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
        self.ee_endpoint = self.config.get(section=self.EE2, option=self.ENDPOINT)

        self.initial_dir = self.config.get(
            section=self.EE2, option=self.INITIAL_DIR, fallback="/condor_shared"
        )

        executable = self.config.get(section=self.EE2, option=self.EXECUTABLE)
        if not pathlib.Path(executable).exists() and not pathlib.Path(
            self.initial_dir + "/" + executable
        ):
            raise FileNotFoundError(executable)
        self.executable = executable

        self.kb_auth_token = self.config.get(section=self.EE2, option=self.AUTH_TOKEN)
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

    def get_default_resources(self, client_group):
        """
        Search the config file for default client groups and requirements
        :param client_group: The section of the config file to search for requirements
        :return: The default requirements for that client group, or for no client group provided
        """
        default_resources = dict()
        if client_group in self.config.sections():
            section = client_group
            default_resources[self.CG] = client_group
        else:
            client_group = self.config["DEFAULT"][self.DEFAULT_CLIENT_GROUP]
            default_resources[self.CG] = client_group
            section = client_group

        # Get settings from config file based on clientgroup or default_clientgroup
        for item in [self.REQUEST_CPUS, self.REQUEST_DISK, self.REQUEST_MEMORY]:
            default_resources[item] = self.config.get(section=section, option=item)

        return default_resources

    def cleanup_submit_file(self, submit_filepath):
        pass

    def setup_environment_vars(self, params):
        # 7 day docker job timeout default, Catalog token used to get access to volume mounts
        environment_vars = {
            "DOCKER_JOB_TIMEOUT": self.docker_timeout,
            "KB_ADMIN_AUTH_TOKEN": self.kb_auth_token,
            "KB_AUTH_TOKEN": params.get("token"),
            "CLIENTGROUP": params.get("clientgroup"),
            "JOB_ID": params.get("job_id"),
            # "WORKDIR": f"{config.get('WORKDIR')}/{params.get('USER')}/{params.get('JOB_ID')}",
            "CONDOR_ID": "$(Cluster).$(Process)",
        }

        environment = ""
        for key, val in environment_vars.items():
            environment += f"{key}={val} "

        return environment

    @staticmethod
    def check_for_missing_runjob_params(params):
        for item in ("token", "user_id", "job_id", "cg_resources_requirements"):
            if item not in params:
                raise MissingRunJobParamsException(f"{item} not found in params")

    # TODO Return type
    @staticmethod
    def normalize(resources_request):
        """
        Ensure that the client_groups are processed as a dictionary and has at least one value
        :param resources_request: either an empty string, a json object, or cg,key1=value,key2=value
        :return:
        """
        if type(resources_request) is not str:
            raise TypeError(str(type(resources_request)))

        # No clientgroup provided
        if resources_request is "":
            return {}
        # JSON
        if "{" in resources_request:
            return json.loads(resources_request)

        rr = resources_request.split(",")
        # Default

        rv = {"client_group": rr.pop(0), "client_group_regex": True}
        for item in rr:
            if "=" not in item:
                raise Exception(
                    f"Malformed requirement. Format is <key>=<value> . Item is {item}"
                )
            (key, value) = item.split("=")
            rv[key] = value

            if key in ["client_group_regex"]:
                raise ValueError(
                    "Illegal argument! Old format does not support this option ('client_group_regex')"
                )

        return rv

    def extract_resources(self, cgrr):
        """
        Checks to see if request_cpus/memory/disk is available
        If not, it sets them based on defaults from the config
        :param cgrr:
        :return:
        """
        client_group = cgrr.get("client_group", None)
        if client_group is None or client_group is "":
            client_group = self.config.get(
                section="DEFAULT", option=self.DEFAULT_CLIENT_GROUP
            )

        if client_group not in self.config.sections():
            raise ValueError(f"{client_group} not found in {self.config.sections()}")

        # TODO Validate that they are a resource followed by a unit
        for key in [self.REQUEST_DISK, self.REQUEST_CPUS, self.REQUEST_MEMORY]:
            if key not in cgrr or cgrr[key] in ["", None]:
                cgrr[key] = self.config.get(section=client_group, option=key)

        cr = self.condor_resources(
            request_cpus=cgrr.get(self.REQUEST_CPUS),
            request_disk=cgrr.get(self.REQUEST_DISK),
            request_memory=cgrr.get(self.REQUEST_MEMORY),
            client_group=client_group,
        )

        return cr

    def extract_requirements(self, cgrr=None, client_group=None):
        """

        :param cgrr:
        :param client_group:
        :return: A list of condor submit file requirements in (key == value) format
        """
        if cgrr is None or client_group is None:
            raise Exception("Please provide normalized cgrr and client_group")

        requirements_statement = []

        client_group_regex = str(cgrr.get("client_group_regex", True))
        client_group_regex = json.loads(client_group_regex.lower())

        if client_group_regex is True:
            requirements_statement.append(f'regexp("{client_group}",CLIENTGROUP)')
        else:
            requirements_statement.append(f'(CLIENTGROUP == "{client_group}")')

        special_requirements = [
            "client_group",
            "client_group_regex",
            self.REQUEST_MEMORY,
            self.REQUEST_DISK,
            self.REQUEST_CPUS,
        ]

        for key, value in cgrr.items():
            if key not in special_requirements:
                requirements_statement.append(f'({key} == "{value}")')

        return requirements_statement

    def create_submit(self, params):
        self.check_for_missing_runjob_params(params)
        sub = dict()
        sub["JobBatchName"] = params.get("job_id")
        sub[self.LEAVE_JOB_IN_QUEUE] = self.leave_job_in_queue
        sub["initial_dir"] = self.initial_dir
        sub["executable"] = f"{self.initial_dir}/{self.executable}"  # Must exist
        sub["arguments"] = " ".join([params.get("job_id"), self.ee_endpoint])
        sub["environment"] = self.setup_environment_vars(params)
        sub["universe"] = "vanilla"
        sub["+AccountingGroup"] = params.get("user_id")
        sub["Concurrency_Limits"] = params.get("user_id")
        sub["+Owner"] = f'"{self.pool_user}"'  # Must be quoted
        sub["ShouldTransferFiles"] = "YES"
        sub["transfer_input_files"] = self.transfer_input_files
        sub["When_To_Transfer_Output"] = "ON_EXIT"

        # Ensure cgrr is a dictionary
        cgrr = self.normalize(params["cg_resources_requirements"])

        # Extract minimum condor resource requirements and client_group
        resources = self.extract_resources(cgrr)
        sub["request_cpus"] = resources.request_cpus
        sub["request_memory"] = resources.request_memory
        sub["request_disk"] = resources.request_disk
        client_group = resources.client_group

        # Set requirements statement
        requirements = self.extract_requirements(cgrr=cgrr, client_group=client_group)
        sub["requirements"] = " && ".join(requirements)

        return sub

    def run_job(self, params, submit_file=None):
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

    # TODO add to pyi
    def run_submit(self, submit):

        sub = htcondor.Submit(submit)
        try:
            schedd = htcondor.Schedd()
            logging.info(schedd)
            logging.info(submit)
            logging.info(os.getuid())
            logging.info(pwd.getpwuid(os.getuid()).pw_name)
            logging.info(submit)
            with schedd.transaction() as txn:
                return self.submission_info(
                    clusterid=str(sub.queue(txn, 1)), submit=sub, error=None
                )
        except Exception as e:
            return self.submission_info(clusterid=None, submit=sub, error=e)

    def get_job_info(self, job_id=None, cluster_id=None):
        if job_id is not None and cluster_id is not None:
            return self.job_info(
                info={},
                error=Exception(
                    "Please use only batch name (job_id) or cluster_id, not both"
                ),
            )

        constraint = None
        if job_id:
            constraint = f"JobBatchName=?={batch_name}"
        if cluster_id:
            constraint = f"ClusterID=?={cluster_id}"

        try:
            job = htcondor.Schedd().query(constraint=constraint, limit=1)[0]
            return self.job_info(info=job, error=None)
        except Exception as e:
            return self.job_info(info={}, error=e)

    def get_user_info(self, user_id, projection=None):
        pass

    def cancel_job(self, job_id):
        pass
