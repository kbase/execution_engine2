import json
import logging
import os
import time
from collections import namedtuple, OrderedDict
from datetime import datetime
from enum import Enum
from cachetools import TTLCache

import dateutil
from bson import ObjectId

from execution_engine2.authorization.authstrategy import (
    can_read_job,
    can_read_jobs,
    can_write_job,
)
from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import (
    Job,
    JobInput,
    JobOutput,
    Meta,
    Status,
    JobLog,
    LogLines,
    ErrorCode,
    TerminatedCode,
)
from execution_engine2.exceptions import AuthError
from execution_engine2.exceptions import (
    RecordNotFoundException,
    InvalidStatusTransitionException,
)
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.KafkaUtils import (
    KafkaClient,
    KafkaCancelJob,
    KafkaCondorCommand,
    KafkaFinishJob,
    KafkaStartJob,
    KafkaStatusChange,
    KafkaCreateJob,
    KafkaQueueChange,
)
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace
from installed_clients.authclient import KBaseAuth

debug = json.loads(os.environ.get("debug", "False").lower())

if debug:
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Set log level to {logging.DEBUG}")
    logging.debug(f"Set log level to {logging.DEBUG}")
else:
    logging.basicConfig(level=logging.WARN)
    logging.info(f"Set log level to {logging.WARN}")
    logging.warn(f"Set log level to {logging.WARN}")


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class SDKMethodRunner:

    JOB_PERMISSION_CACHE_SIZE = 500
    JOB_PERMISSION_CACHE_EXPIRE_TIME = 300  # seconds

    def allow_job_read(func):
        def inner(self, *args, **kwargs):

            job_id = kwargs.get("job_id")
            if job_id is None:
                raise ValueError("Please provide valid job_id")
            self._test_job_permission_with_cache(job_id, JobPermissions.READ)

            return func(self, *args, **kwargs)

        return inner

    def allow_job_write(func):
        def inner(self, *args, **kwargs):

            job_id = kwargs.get("job_id")
            if job_id is None:
                raise ValueError("Please provide valid job_id")
            self._test_job_permission_with_cache(job_id, JobPermissions.WRITE)

            return func(self, *args, **kwargs)

        return inner

    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """
        if method is None:
            raise ValueError("Please input module_name.function_name")

        if method is not None and "." not in method:
            raise ValueError(
                "unrecognized method: {}. Please input module_name.function_name".format(
                    method
                )
            )

        module_name, function_name = method.split(".")

        group_config = self.catalog.list_client_group_configs(
            {"module_name": module_name, "function_name": function_name}
        )

        if group_config:
            client_groups = group_config[0].get("client_groups")[0]
        else:
            client_groups = ""

        return client_groups

    def _check_ws_objects(self, source_objects):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.get_workspace().get_object_info3(
                {"objects": objects, "ignoreErrors": 1}
            )
            paths = info.get("paths")

            if None in paths:
                raise ValueError("Some workspace object is inaccessible")

    def _get_module_git_commit(self, method, service_ver=None):
        module_name = method.split(".")[0]

        if not service_ver:
            service_ver = "release"

        module_version = self.catalog.get_module_version(
            {"module_name": module_name, "version": service_ver}
        )

        git_commit_hash = module_version.get("git_commit_hash")

        return git_commit_hash

    def _init_job_rec(self, user_id, params):

        job = Job()

        inputs = JobInput()

        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get("wsid")
        job.status = "created"
        inputs.wsid = job.wsid
        inputs.method = params.get("method")
        inputs.params = params.get("params")
        inputs.service_ver = params.get("service_ver")
        inputs.app_id = params.get("app_id")
        inputs.source_ws_objects = params.get("source_ws_objects")
        inputs.parent_job_id = str(params.get("parent_job_id"))

        inputs.narrative_cell_info = Meta()
        meta = params.get("meta")
        if meta:
            inputs.narrative_cell_info.run_id = meta.get("run_id")
            inputs.narrative_cell_info.token_id = meta.get("token_id")
            inputs.narrative_cell_info.tag = meta.get("tag")
            inputs.narrative_cell_info.cell_id = meta.get("cell_id")
            inputs.narrative_cell_info.status = meta.get("status")

        job.job_input = inputs
        logging.info(job.job_input.to_mongo().to_dict())
        with self.get_mongo_util().mongo_engine_connection():
            job.save()

        self.kafka_client.send_kafka_message(
            message=KafkaCreateJob(job_id=str(job.id), user=user_id)
        )

        return str(job.id)

    def get_workspace_auth(self):
        if self.workspace_auth is None:
            self.workspace_auth = WorkspaceAuth(
                self.token, self.user_id, self.workspace_url
            )
        return self.workspace_auth

    def get_mongo_util(self):
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self):
        if self.condor is None:
            self.condor = Condor(self.deployment_config_fp)
        return self.condor

    def get_workspace(self):
        if self.workspace is None:
            self.workspace = Workspace(token=self.token, url=self.workspace_url)
        return self.workspace

    def _get_job_log(self, job_id, skip_lines):
        """
        # TODO Do I have to query this another way so I don't load all lines into memory?
        # Does mongoengine lazy-load it?

        # TODO IMPLEMENT SKIP LINES
        # TODO MAKE ONLY THE TIMESTAMP A STRING, so AS TO NOT HAVING TO LOOP OVER EACH ATTRIBUTE?
        # TODO Filter the lines in the mongo query?
        # TODO AVOID LOADING ENTIRE THING INTO MEMORY?
        # TODO Check if there is an off by one for line_count?


           :returns: instance of type "GetJobLogsResults" (last_line_number -
           common number of lines (including those in skip_lines parameter),
           this number can be used as next skip_lines value to skip already
           loaded lines next time.) -> structure: parameter "lines" of list
           of type "LogLine" -> structure: parameter "line" of String,
           parameter "is_error" of type "boolean" (@range [0,1]), parameter
           "last_line_number" of Long


        :param job_id:
        :param skip_lines:
        :return:
        """

        log = self.get_mongo_util().get_job_log_pymongo(job_id)

        lines = []
        for log_line in log.get("lines", []):  # type: LogLines
            if skip_lines and int(skip_lines) >= log_line.get("linepos", 0):
                continue
            lines.append(
                {
                    "line": log_line.get("line"),
                    "linepos": log_line.get("linepos"),
                    "error": log_line.get("error"),
                    "ts": int(log_line.get("ts", 0) * 1000),
                }
            )

        log_obj = {"lines": lines, "last_line_number": log["stored_line_count"]}
        return log_obj

    @allow_job_read
    def view_job_logs(self, job_id, skip_lines):
        """
        Authorization Required: Ability to read from the workspace
        :param job_id:
        :param skip_lines:
        :return:
        """
        return self._get_job_log(job_id, skip_lines)

    def _send_exec_stats_to_catalog(self, job_id):
        job = self.get_mongo_util().get_job(job_id)

        job_input = job.job_input

        log_exec_stats_params = dict()
        log_exec_stats_params["user_id"] = job.user
        app_id = job_input.app_id
        log_exec_stats_params["app_module_name"] = app_id.split("/")[0]
        log_exec_stats_params["app_id"] = app_id
        method = job_input.method
        log_exec_stats_params["func_module_name"] = method.split(".")[0]
        log_exec_stats_params["func_name"] = method.split(".")[-1]
        log_exec_stats_params["git_commit_hash"] = job_input.service_ver
        log_exec_stats_params["creation_time"] = job.id.generation_time.timestamp()
        log_exec_stats_params["exec_start_time"] = job.running.timestamp()
        log_exec_stats_params["finish_time"] = job.finished.timestamp()
        log_exec_stats_params["is_error"] = int(job.status == Status.error.value)
        log_exec_stats_params["job_id"] = job_id

        self.catalog.log_exec_stats(log_exec_stats_params)

    @staticmethod
    def _create_new_log(pk):
        jl = JobLog()
        jl.primary_key = pk
        jl.original_line_count = 0
        jl.stored_line_count = 0
        jl.lines = []
        return jl

    @staticmethod
    def _check_and_convert_time(time_input, assign_default_time=False):
        """
        convert input time into timestamp in epoch format
        """

        try:
            if isinstance(time_input, str):  # input time_input as string
                if time_input.replace(
                    ".", "", 1
                ).isdigit():  # input time_input as numeric string
                    time_input = (
                        float(time_input)
                        if "." in time_input
                        else int(time_input) / 1000.0
                    )
                else:  # input time_input as datetime string
                    time_input = dateutil.parser.parse(time_input).timestamp()
            elif isinstance(
                time_input, int
            ):  # input time_input as epoch timestamps in milliseconds
                time_input = time_input / 1000.0
            elif isinstance(time_input, datetime):
                time_input = time_input.timestamp()

            datetime.fromtimestamp(time_input)  # check current time_input is valid
        except Exception:
            if assign_default_time:
                logging.info(
                    "Cannot convert time_input into timestamps: {}".format(time_input)
                )
                time_input = time.time()
            else:
                raise ValueError(
                    "Cannot convert time_input into timestamps: {}".format(time_input)
                )

        return time_input

    @allow_job_write
    def add_job_logs(self, job_id, log_lines):
        """
        #TODO Prevent too many logs in memory
        #TODO Max size of log lines = 1000
        #TODO Error with out of space happened previously. So we just update line count.
        #TODO db.updateExecLogOriginalLineCount(ujsJobId, dbLog.getOriginalLineCount() + lines.size());


        # TODO Limit amount of lines per request?
        # TODO Maybe Prevent Some lines with TS and some without
        # TODO # Handle malformed requests?

        #Authorization Required : Ability to read and write to the workspace
        :param job_id:
        :param log_lines:
        :return:
        """
        logging.debug(f"About to add logs for {job_id}")
        mongo_util = self.get_mongo_util()

        try:
            log = mongo_util.get_job_log_pymongo(job_id)
        except RecordNotFoundException:
            log = self._create_new_log(pk=job_id).to_mongo().to_dict()

        olc = log.get("original_line_count")

        for input_line in log_lines:
            olc += 1
            ll = LogLines()
            ll.error = input_line.get("error", False)
            ll.linepos = olc
            ts = input_line.get("ts")
            # TODO Maybe use strpos for efficiency?
            if ts is not None:
                ts = self._check_and_convert_time(ts, assign_default_time=True)

            ll.ts = ts

            ll.line = input_line.get("line")
            ll.validate()
            log["lines"].append(ll.to_mongo().to_dict())

        log["updated"] = time.time()
        log["original_line_count"] = olc
        log["stored_line_count"] = olc

        with mongo_util.pymongo_client(self.config["mongo-logs-collection"]):
            mongo_util.update_one(log, str(log.get("_id")))

        return log["stored_line_count"]

    def __init__(self, config, user_id=None, token=None, job_permission_cache=None):
        self.deployment_config_fp = os.environ.get("KB_DEPLOYMENT_CONFIG")
        self.config = config
        self.mongo_util = None
        self.condor = None
        self.workspace = None
        self.workspace_auth = None

        self.admin_roles = config.get("admin_roles", ["EE2_ADMIN", "EE2_ADMIN_RO"])

        catalog_url = config.get("catalog-url")
        self.catalog = Catalog(catalog_url)

        self.workspace_url = config.get("workspace-url")

        self.auth_url = config.get("auth-url")
        self.legacy_auth_url = config.get("auth-service-url")
        self.auth = KBaseAuth(auth_url=self.legacy_auth_url)

        self.user_id = user_id
        self.token = token
        self.is_admin = False

        if job_permission_cache is None:
            self.job_permission_cache = TTLCache(
                maxsize=self.JOB_PERMISSION_CACHE_SIZE,
                ttl=self.JOB_PERMISSION_CACHE_EXPIRE_TIME,
            )
        else:
            self.job_permission_cache = job_permission_cache

        self.kafka_client = KafkaClient(config.get("kafka-host"))

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.debug
        )

    def cancel_job(self, job_id, terminated_code=None):
        """
        Authorization Required: Ability to Read and Write to the Workspace
        :param job_id:
        :param terminated_code:
        :return:
        """
        # Is it inefficient to get the job twice? Is it cached?
        # Maybe if the call fails, we don't actually cancel the job?
        logging.debug(f"Attempting to cancel job {job_id}")

        job = self._get_job_with_permission(job_id, JobPermissions.WRITE)

        logging.info(f"User has permission to cancel job {job_id}")
        logging.debug(f"User has permission to cancel job {job_id}")

        if terminated_code is None:
            terminated_code = TerminatedCode.terminated_by_user.value

        self.get_mongo_util().cancel_job(job_id=job_id, terminated_code=terminated_code)

        self.kafka_client.send_kafka_message(
            message=KafkaCancelJob(
                job_id=str(job_id),
                previous_status=job.status,
                new_status=Status.terminated.value,
                scheduler_id=job.scheduler_id,
                terminated_code=terminated_code,
            )
        )

        logging.info(f"About to cancel job in CONDOR using {job.scheduler_id}")
        logging.debug(f"About to cancel job in CONDOR using {job.scheduler_id}")
        rv = self.get_condor().cancel_job(job_id=job.scheduler_id)
        logging.info(rv)
        logging.debug(f"{rv}")

        self.kafka_client.send_kafka_message(
            message=KafkaCondorCommand(
                job_id=str(job_id),
                scheduler_id=job.scheduler_id,
                condor_command="condor_rm",
            )
        )

    def check_job_canceled(self, job_id):
        """
        Authorization Required: None
        Check to see if job is terminated by the user
        :return: job_id, whether or not job is canceled, and whether or not job is finished
        """
        job_status = self.get_mongo_util().get_job(job_id=job_id).status
        rv = {"job_id": job_id, "canceled": False, "finished": False}

        if Status(job_status) is Status.terminated:
            rv["canceled"] = True
            rv["finished"] = True

        if Status(job_status) in [Status.completed, Status.error, Status.terminated]:
            rv["finished"] = True
        return rv

    def run_job(self, params):
        """
        :param params: RunJobParams object (See spec file)
        :return: The condor job id
        """
        wsid = params.get("wsid")
        ws_auth = self.get_workspace_auth()
        if wsid and not ws_auth.can_write(wsid):
            logging.debug(
                f"User {self.user_id} doesn't have permission to run jobs in workspace {wsid}."
            )
            raise PermissionError(
                f"User {self.user_id} doesn't have permission to run jobs in workspace {wsid}."
            )

        method = params.get("method")
        logging.info(f"User {self.user_id} attempting to run job {method}")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(self.user_id, params)

        logging.debug("About to run job with")
        logging.debug(client_groups)
        logging.debug(params)
        params["job_id"] = job_id
        params["user_id"] = self.user_id
        params["token"] = self.token
        params["cg_resources_requirements"] = client_groups
        try:
            submission_info = self.get_condor().run_job(params)
            condor_job_id = submission_info.clusterid
            logging.debug(f"Submitted job id and got '{condor_job_id}'")
        except Exception as e:
            ## delete job from database? Or mark it to a state it will never run?
            logging.error(e)
            raise e

        if submission_info.error is not None:
            raise submission_info.error
        if condor_job_id is None:
            raise RuntimeError(
                "Condor job not ran, and error not found. Something went wrong"
            )

        logging.debug("Submission info is")
        logging.debug(submission_info)
        logging.debug(condor_job_id)
        logging.debug(type(condor_job_id))

        logging.info(f"Attempting to update job to queued  {job_id} {condor_job_id}")
        self.update_job_to_queued(job_id=job_id, scheduler_id=condor_job_id)

        return job_id

    def update_job_to_queued(self, job_id, scheduler_id):
        # TODO RETRY FOR RACE CONDITION OF RUN/CANCEL
        # TODO PASS QUEUE TIME IN FROM SCHEDULER ITSELF?
        # TODO PASS IN SCHEDULER TYPE?
        with self.get_mongo_util().mongo_engine_connection():
            j = self.get_mongo_util().get_job(job_id=job_id)
            previous_status = j.status
            j.status = Status.queued.value
            j.queued = time.time()
            j.scheduler_id = scheduler_id
            j.scheduler_type = "condor"
            j.save()

            self.kafka_client.send_kafka_message(
                message=KafkaQueueChange(
                    job_id=str(j.id),
                    new_status=j.status,
                    previous_status=previous_status,
                    scheduler_id=scheduler_id,
                )
            )

    def _run_admin_command(self, command, params):
        available_commands = ["cancel_job", "view_job_logs"]
        if command not in available_commands:
            raise ValueError(
                f"{command} not an admin command. See {available_commands} "
            )
        commands = {"cancel_job": self.cancel_job, "view_job_logs": self.view_job_logs}
        p = {
            "cancel_job": {
                "job_id": params.get("job_id"),
                "terminated_code": params.get(
                    "terminated_code", TerminatedCode.terminated_by_admin.value
                ),
            },
            "view_job_logs": {"job_id": params.get("job_id")},
        }
        return commands[command](**p[command])

    def administer(self, command, params, token):
        """
        Run commands as an administrator. Requires a token for a user with an EE2 administrative role.
        Currently allowed commands are cancel_job and view_job_logs.

        Commands are given as strings, and their parameters are given as a dictionary of keys and values.
        For example:
            administer("cancel_job", {"job_id": 12345}, auth_token)
        is the same as running
            cancel_job(12345)
        but with administrative privileges.
        :param command: The command to run (See specfile)
        :param params: The parameters for that command that will be expanded (See specfile)
        :param token: The auth token (Will be checked for the correct auth role)
        :return:
        """
        logging.info(
            f'Attempting to run administrative command "{command}" as user {self.user_id}'
        )
        # set admin privs, one way or the other
        self.is_admin = self._is_admin(token)
        if not self.is_admin:
            raise PermissionError(
                f"User {self.user_id} is not authorized to run administrative commands."
            )
        self._run_admin_command(command, params)
        self.is_admin = False

    def _is_admin(self, token: str) -> bool:
        try:
            self.is_admin = AdminAuthUtil(self.auth_url, self.admin_roles).is_admin(
                token
            )
            return self.is_admin
        except AuthError as e:
            logging.error(f"An auth error occurred: {str(e)}")
            raise e
        except RuntimeError as e:
            logging.error(
                f"A runtime error occurred while looking up user roles: {str(e)}"
            )
            raise e

    def _update_job_permission_cache(self, job_id, user_id, level, perm):

        if not self.job_permission_cache.get(job_id):
            self.job_permission_cache[job_id] = {user_id: {level: perm}}
        else:
            job_permission = self.job_permission_cache[job_id]
            if not job_permission.get(user_id):
                job_permission[user_id] = {level: perm}
            else:
                job_permission[user_id][level] = perm

    def _get_job_permission_from_cache(self, job_id, user_id, level):

        if not self.job_permission_cache.get(job_id):
            return False
        else:
            job_permission = self.job_permission_cache[job_id]
            if not job_permission.get(user_id):
                return False
            else:
                return job_permission[user_id].get(level)

    def _test_job_permission_with_cache(self, job_id, permission):
        if not self._get_job_permission_from_cache(job_id, self.user_id, permission):
            job = self.get_mongo_util().get_job(job_id=job_id)
            self._test_job_permissions(job, job_id, permission)

        logging.debug("you have permission to {} job {}".format(permission, job_id))

    def _get_job_with_permission(self, job_id, permission):
        job = self.get_mongo_util().get_job(job_id=job_id)
        if not self._get_job_permission_from_cache(job_id, self.user_id, permission):
            self._test_job_permissions(job, job_id, permission)
        return job

    def _test_job_permissions(
        self, job: Job, job_id: str, level: JobPermissions
    ) -> bool:
        """
        Tests if the currently loaded token has the requested permissions for the given job.
        Returns True if so. Raises a PermissionError if not.
        Can also raise a RuntimeError if anything bad happens while looking up rights. This
        can be triggered from either Auth or Workspace errors.

        Effectively, this can be used the following way:
        some_job = get_job(job_id)
        _test_job_permissions(some_job, job_id, JobPermissions.READ)

        ...and continue on with code. If the user doesn't have permission, a PermissionError gets
        thrown. This can either be captured by the calling function, or allowed to propagate out
        to the user and just end the RPC call.

        :param job: a Job object to seek permissions for
        :param job_id: string - the id associated with the Job object
        :param level: string - the level to seek - either READ or WRITE
        :returns: True if the user has permission, raises a PermissionError otherwise.
        """
        if self.is_admin:  # bypass if we're in admin mode.
            self._update_job_permission_cache(job_id, self.user_id, level, True)
            return True
        try:
            perm = False
            if level == JobPermissions.READ:
                perm = can_read_job(job, self.user_id, self.token, self.config)
                self._update_job_permission_cache(job_id, self.user_id, level, perm)
            elif level == JobPermissions.WRITE:
                perm = can_write_job(job, self.user_id, self.token, self.config)
                self._update_job_permission_cache(job_id, self.user_id, level, perm)
            if not perm:
                raise PermissionError(
                    f"User {self.user_id} does not have permission to {level} job {job_id}"
                )
        except RuntimeError as e:
            logging.error(
                f"An error occurred while checking permissions for job {job_id}"
            )
            raise e

    def get_job_params(self, job_id):
        """
        get_job_params: fetch SDK method params passed to job runner

        Parameters:
        job_id: id of job

        Returns:
        job_params:
        """
        job_params = dict()

        job = self._get_job_with_permission(job_id, JobPermissions.READ)

        job_input = job.job_input

        job_params["method"] = job_input.method
        job_params["params"] = job_input.params
        job_params["service_ver"] = job_input.service_ver
        job_params["app_id"] = job_input.app_id
        job_params["wsid"] = job_input.wsid
        job_params["parent_job_id"] = job_input.parent_job_id
        job_params["source_ws_objects"] = job_input.source_ws_objects

        return job_params

    def update_job_status(self, job_id, status):
        """
        #TODO Deprecate this in favor of specific methods with specific checks?
        update_job_status: update status of a job runner record.
                           raise error if job is not found or status is not listed in models.Status
        * Does not update TerminatedCode or ErrorCode
        * Does not update Timestamps
        * Allows invalid state transitions, e.g. Running -> Created

        Parameters:
        job_id: id of job
        """

        if not (job_id and status):
            raise ValueError("Please provide both job_id and status")

        job = self._get_job_with_permission(job_id, JobPermissions.WRITE)

        previous_status = job.status
        job.status = status
        with self.get_mongo_util().mongo_engine_connection():
            job.save()

        self.kafka_client.send_kafka_message(
            message=KafkaStatusChange(
                job_id=str(job_id),
                new_status=status,
                previous_status=previous_status,
                scheduler_id=job.scheduler_id,
            )
        )

        return str(job.id)

    def get_job_status(self, job_id):
        """
        get_job_status: fetch status of a job runner record.
                        raise error if job is not found

        Parameters:
        job_id: id of job

        Returns:
        returnVal: returnVal['status'] status of job
        """

        returnVal = dict()

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job = self._get_job_with_permission(job_id, JobPermissions.READ)

        returnVal["status"] = job.status

        return returnVal

    def _check_job_is_status(self, job_id, status):
        job = self.get_mongo_util().get_job(job_id=job_id)
        if job.status != status:
            raise InvalidStatusTransitionException(
                f"Unexpected job status: {job.status} . Expected {status} "
            )
        return job

    def _check_job_is_created(self, job_id):
        return self._check_job_is_status(job_id, Status.created.value)

    def _check_job_is_running(self, job_id):
        return self._check_job_is_status(job_id, Status.running.value)

    def _finish_job_with_error(self, job_id, error_message, error_code, error=None):
        if error_code is None:
            error_code = ErrorCode.unknown_error.value

        self.get_mongo_util().finish_job_with_error(
            job_id=job_id,
            error_message=error_message,
            error_code=error_code,
            error=error,
        )

    def _finish_job_with_success(self, job_id, job_output):
        output = JobOutput()
        output.version = job_output.get("version")
        output.id = ObjectId(job_output.get("id"))
        output.result = job_output.get("result")
        try:
            output.validate()
        except Exception as e:
            logging.info(e)
            error_message = "Something was wrong with the output object"
            error_code = ErrorCode.job_missing_output.value
            self.get_mongo_util().finish_job_with_error(
                job_id=job_id, error_message=error_message, error_code=error_code
            )
            raise Exception(str(e) + str(error_message))

        self.get_mongo_util().finish_job_with_success(
            job_id=job_id, job_output=job_output
        )

    @allow_job_write
    def finish_job(
        self, job_id, error_message=None, error_code=None, error=None, job_output=None
    ):
        """
        #TODO Fix too many open connections to mongoengine

        finish_job: set job record to finish status and update finished timestamp
                    (set job status to "finished" by default. If error_message is given, set job to "error" status)
                    raise error if job is not found or current job status is not "running"
                    (general work flow for job status created -> queued -> estimating -> running -> finished/error/terminated)
        Parameters:
        :param job_id: string - id of job
        :param error_message: string - default None, if given set job to error status
        :param error_code: int - default None, if given give this job an error code
        :param error: dict - default None, if given, set the error to this structure
        :param job_output: dict - default None, if given this job has some output
        """

        job = self._check_job_is_running(job_id=job_id)

        if error_message:
            if error_code is None:
                error_code = ErrorCode.job_crashed.value
            self._finish_job_with_error(
                job_id=job_id,
                error_message=error_message,
                error_code=error_code,
                error=error,
            )

            self.kafka_client.send_kafka_message(
                message=KafkaFinishJob(
                    job_id=str(job_id),
                    new_status=Status.error.value,
                    previous_status=job.status,
                    error_message=error_message,
                    error_code=error_code,
                    scheduler_id=job.scheduler_id,
                )
            )

        elif job_output is None:
            if error_code is None:
                error_code = ErrorCode.job_missing_output.value
            msg = "Missing job output required in order to successfully finish job. Something went wrong"
            self._finish_job_with_error(
                job_id=job_id, error_message=msg, error_code=error_code
            )
            raise ValueError(msg)
            # No Kafka Message Here as this finish_job call failed due to insufficient requirements
        else:
            self._finish_job_with_success(job_id=job_id, job_output=job_output)
            self.kafka_client.send_kafka_message(
                message=KafkaFinishJob(
                    job_id=str(job_id),
                    new_status=Status.completed.value,
                    previous_status=job.status,
                    scheduler_id=job.scheduler_id,
                )
            )

    def start_job(self, job_id, skip_estimation=True):
        """
        start_job: set job record to start status ("estimating" or "running") and update timestamp
                   (set job status to "estimating" by default, if job status currently is "created" or "queued".
                    set job status to "running", if job status currently is "estimating")
                   raise error if job is not found or current job status is not "created", "queued" or "estimating"
                   (general work flow for job status created -> queued -> estimating -> running -> finished/error/terminated)

        Parameters:
        job_id: id of job
        skip_estimation: skip estimation step and set job to running directly
        """

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job = self._get_job_with_permission(job_id, JobPermissions.WRITE)
        job_status = job.status

        allowed_states = [
            Status.created.value,
            Status.queued.value,
            Status.estimating.value,
        ]
        if job_status not in allowed_states:
            raise ValueError(
                f"Unexpected job status for {job_id}: {job_status}.  You cannot start a job that is not in {allowed_states}"
            )

        with self.get_mongo_util().mongo_engine_connection():
            if job_status == Status.estimating.value or skip_estimation:
                # set job to running status

                job.running = time.time()
                self.get_mongo_util().update_job_status(
                    job_id=job_id, status=Status.running.value
                )
            else:
                # set job to estimating status

                job.estimating = time.time()
                self.get_mongo_util().update_job_status(
                    job_id=job_id, status=Status.estimating.value
                )
            job.save()

        job.reload("status")

        self.kafka_client.send_kafka_message(
            message=KafkaStartJob(
                job_id=str(job_id),
                new_status=job.status,
                previous_status=job_status,
                scheduler_id=job.scheduler_id,
            )
        )

    def check_job(self, job_id, check_permission=True, projection=None):
        """
        check_job: check and return job status for a given job_id

        Parameters:
        job_id: id of job
        """

        logging.info("Start fetching status for job: {}".format(job_id))

        if projection is None:
            projection = []

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job_state = self.check_jobs(
            [job_id],
            check_permission=check_permission,
            projection=projection,
            return_list=0,
        ).get(job_id)

        return job_state

    def check_jobs(
        self, job_ids, check_permission=True, projection=None, return_list=None
    ):
        """
        check_jobs: check and return job status for a given of list job_ids
        """

        logging.info("Start fetching status for jobs: {}".format(job_ids))

        if projection is None:
            projection = []

        with self.get_mongo_util().mongo_engine_connection():
            jobs = self.get_mongo_util().get_jobs(
                job_ids=job_ids, projection=projection
            )

        if check_permission:
            try:
                perms = can_read_jobs(jobs, self.user_id, self.token, self.config)
            except RuntimeError as e:
                logging.error(
                    f"An error occurred while checking read permissions for jobs"
                )
                raise e
        else:
            perms = [True] * len(jobs)

        job_states = dict()
        for idx, job in enumerate(jobs):
            if not perms[idx]:
                job_states[str(job.id)] = {"error": "Cannot read this job"}
            mongo_rec = job.to_mongo().to_dict()
            del mongo_rec["_id"]
            mongo_rec["job_id"] = str(job.id)
            mongo_rec["created"] = int(job.id.generation_time.timestamp() * 1000)
            mongo_rec["updated"] = int(job.updated * 1000)
            if job.estimating:
                mongo_rec["estimating"] = int(job.estimating * 1000)
            if job.running:
                mongo_rec["running"] = int(job.running * 1000)
            if job.finished:
                mongo_rec["finished"] = int(job.finished * 1000)
            if job.queued:
                mongo_rec["queued"] = int(job.queued * 1000)

            job_states[str(job.id)] = mongo_rec

        job_states = OrderedDict(
            {job_id: job_states.get(job_id, []) for job_id in job_ids}
        )

        if return_list is not None and SDKMethodRunner.parse_bool_from_string(
            return_list
        ):
            job_states = {"job_states": list(job_states.values())}

        return job_states

    def check_workspace_jobs(self, workspace_id, projection=None, return_list=None):
        """
        check_workspace_jobs: check job status for all jobs in a given workspace
        """
        logging.info(
            "Start fetching all jobs status in workspace: {}".format(workspace_id)
        )

        if projection is None:
            projection = []

        ws_auth = self.get_workspace_auth()
        if not ws_auth.can_read(workspace_id):
            logging.debug(
                f"User {self.user_id} doesn't have permission to read jobs in workspace {workspace_id}."
            )
            raise PermissionError(
                f"User {self.user_id} does not have permission to read jobs in workspace {workspace_id}"
            )

        with self.get_mongo_util().mongo_engine_connection():
            job_ids = [str(job.id) for job in Job.objects(wsid=workspace_id)]

        if not job_ids:
            return {}

        job_states = self.check_jobs(
            job_ids,
            check_permission=False,
            projection=projection,
            return_list=return_list,
        )

        return job_states

    @staticmethod
    def _job_state_from_jobs(jobs):
        """
        Returns as per the spec file

        :param jobs: MongoEngine Job Objects Query
        :return: list of job states of format
        Special Cases:
        str(_id)
        str(job_id)
        float(created/queued/estimating/running/finished/updated/) (Time in MS)
        """
        job_states = []
        for job in jobs:
            mongo_rec = job.to_mongo().to_dict()
            mongo_rec["_id"] = str(job.id)
            mongo_rec["job_id"] = str(job.id)
            mongo_rec["created"] = int(job.id.generation_time.timestamp() * 1000)
            mongo_rec["updated"] = int(job.updated * 1000)
            if job.estimating:
                mongo_rec["estimating"] = int(job.estimating * 1000)
            if job.queued:
                mongo_rec["queued"] = int(job.queued * 1000)
            if job.running:
                mongo_rec["running"] = int(job.running * 1000)
            if job.finished:
                mongo_rec["finished"] = int(job.finished * 1000)
            job_states.append(mongo_rec)
        return job_states

    @staticmethod
    def parse_bool_from_string(str_or_bool):
        if isinstance(str_or_bool, bool):
            return str_or_bool

        if isinstance(str_or_bool, int):
            return str_or_bool

        if isinstance(json.loads(str_or_bool.lower()), bool):
            return json.loads(str_or_bool.lower())

        raise Exception("Not a boolean value")

    @staticmethod
    def get_sort_order(ascending):
        if ascending is None:
            return "+"
        else:
            if SDKMethodRunner.parse_bool_from_string(ascending):
                return "+"
            else:
                return "-"

    def check_is_admin(self):
        """
        Check Auth for your admin role and see if it is an allowed admin role
        :return:
        """
        return int(self._is_admin(self.token))

    def get_admin_permission(self):
        """
        Get your your type of admin permissions
        :return:
        """
        aau = AdminAuthUtil(self.auth_url, self.admin_roles)
        roles = list(aau._fetch_user_roles(self.token))
        permission = None
        if "EE2_ADMIN" in roles:
            permission = "w"
        elif "EE2_ADMIN_RO" in roles:
            permission = "r"
        return {"permission": permission}

    def check_jobs_date_range_for_user(
        self,
        creation_start_time,
        creation_end_time,
        job_projection=None,
        job_filter=None,
        limit=None,
        user=None,
        offset=None,
        ascending=None,
    ):

        """
        :param creation_start_time: Start timestamp since epoch for Creation
        :param creation_end_time: Stop timestamp since epoch for Creation
        :param job_projection:  List of fields to project alongside [_id, authstrat, updated, created, job_id]
        :param job_filter:  List of simple job fields of format key=value
        :param limit: Limit of records to return, default 2000
        :param user: Optional Username or "ALL" for all users
        :param offset: Optional offset for skipping records
        :param ascending: Sort by id ascending or descending
        :return:
        """
        sort_order = self.get_sort_order(ascending)

        if offset is None:
            offset = 0

        if self.token is None:
            raise AuthError("Please provide a token to check jobs date range")

        token_user = self.auth.get_user(self.token)
        if user is None:
            user = token_user

        # Admins can view "ALL" or check_jobs for other users
        if user != token_user:
            if not self._is_admin(self.token):
                raise AuthError(
                    f"You are not authorized to view all records or records for others. user={user} token={token_user}"
                )

        dummy_ids = self._get_dummy_dates(creation_start_time, creation_end_time)

        if job_projection is None:
            # Maybe set a default here?
            job_projection = []

        if not isinstance(job_projection, list):
            raise Exception("Invalid job projection type. Must be list")

        if limit is None:
            # Maybe put this in config
            limit = 2000

        job_filter_temp = {}
        if isinstance(job_filter, list):
            for item in job_filter:
                (k, v) = item.split("=")
                job_filter_temp[k] = v
        elif isinstance(job_filter, dict):
            job_filter_temp = job_filter
        elif job_filter is None:
            pass
        else:
            raise Exception(
                "Job filter must be a dictionary or a list of key=value pairs"
            )

        job_filter_temp["id__gt"] = dummy_ids.start
        job_filter_temp["id__lt"] = dummy_ids.stop

        if user != "ALL":
            job_filter_temp["user"] = user

        with self.get_mongo_util().mongo_engine_connection():
            count = Job.objects.filter(**job_filter_temp).count()
            jobs = (
                Job.objects[:limit]
                .filter(**job_filter_temp)
                .order_by(f"{sort_order}_id")
                .skip(offset)
                .only(*job_projection)
            )

        logging.info(
            f"Searching for jobs with id_gt {dummy_ids.start} id_lt {dummy_ids.stop}"
        )

        job_states = self._job_state_from_jobs(jobs)

        # Remove ObjectIds
        for item in job_filter_temp:
            job_filter_temp[item] = str(job_filter_temp[item])

        return {
            "jobs": job_states,
            "count": len(job_states),
            "query_count": count,
            "filter": job_filter_temp,
            "skip": offset,
            "projection": job_projection,
            "limit": limit,
            "sort_order": sort_order,
        }

        # TODO Move to MongoUtils?
        # TODO Add support for projection (validate the allowed fields to project?) (Need better api design)
        # TODO Add support for filter (validate the allowed fields to project?) (Need better api design)
        # TODO USE AS_PYMONGO() FOR SPEED
        # TODO Better define default fields
        # TODO Instead of SKIP use ID GT LT https://www.codementor.io/arpitbhayani/fast-and-efficient-pagination-in-mongodb-9095flbqr

    def _get_dummy_dates(self, creation_start_time, creation_end_time):

        if creation_start_time is None:
            raise Exception(
                "Please provide a valid start time for when job was created"
            )

        creation_start_time = self._check_and_convert_time(creation_start_time)
        creation_start_date = datetime.fromtimestamp(creation_start_time)
        dummy_start_id = ObjectId.from_datetime(creation_start_date)

        if creation_end_time is None:
            raise Exception("Please provide a valid end time for when job was created")

        creation_end_time = self._check_and_convert_time(creation_end_time)
        creation_end_date = datetime.fromtimestamp(creation_end_time)
        dummy_end_id = ObjectId.from_datetime(creation_end_date)

        if creation_start_time > creation_end_time:
            raise Exception("The start time cannot be greater than the end time.")

        dummy_ids = namedtuple("dummy_ids", "start stop")

        return dummy_ids(start=dummy_start_id, stop=dummy_end_id)
