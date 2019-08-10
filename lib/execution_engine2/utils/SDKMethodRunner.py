import json
import logging
import os
import re
import traceback
from datetime import datetime
from enum import Enum
from time import time

from execution_engine2.models.models import (
    Job,
    JobInput,
    Meta,
    Status,
    JobLog,
    LogLines,
)
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.MongoUtil import MongoUtil
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace

debug = json.loads(os.environ.get("debug", "False").lower())

if debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.WARN)


class SDKMethodRunner:
    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """
        if method is None:
            raise ValueError("Please input module_name.function_name")

        pattern = re.compile(r".*\..*")
        if method is not None and not pattern.match(method):
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

    def _check_ws_objects(self, source_objects, ctx):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.get_workspace(ctx=ctx).get_object_info3(
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

        # TODO Add Meta Fields From Params
        inputs.narrative_cell_info = Meta()

        job.job_input = inputs
        logging.info(job.job_input.to_mongo().to_dict())
        with self.get_mongo_util().mongo_engine_connection():
            job.save()

        return str(job.id)

    def get_mongo_util(self):
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self):
        if self.condor is None:
            self.condor = Condor(self.deployment_config_fp)
        return self.condor

    def get_workspace(self, ctx=None):
        if ctx is None:
            ctx = self.ctx
        if ctx is None:
            raise Exception("Need to provide credentials for the workspace")
        if self.workspace is None:
            self.workspace = Workspace(token=ctx["token"], url=self.workspace_url)
        return self.workspace

    class WorkspacePermissions(Enum):
        ADMINISTRATOR = "a"
        READ_WRITE = "w"
        READ = "r"
        NONE = "n"

    # def get_job_status(self, job_id, ctx):
    #     logging.debug(f"About to view logs for {job_id}")
    #     self.check_permission_for_job(job_id=job_id, ctx=ctx, write=False)
    #     logging.debug("Success, you have permission to view job status for " + job_id)
    #     return {}

    def _get_job_log(self, job_id, skip_lines):
        """
        # TODO Do I have to query this another way so I don't load all lines into memory?
        # Does mongoengine lazy-load it?

        # TODO IMPLEMENT SKIP LINES

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

        log = self.get_mongo_util().get_job_log(job_id)
        # if skip_lines #TODO

        # TODO Filter the lines in the mongo query?
        lines = []
        for line in log.lines:  # type: LogLines
            lines.append(line.to_mongo().to_dict())

        # TODO AVOID LOADING ENTIRE THING INTO MEMORY

        log_obj = {"lines": lines, "last_line_number": log.stored_line_count}
        return log_obj

    def view_job_logs(self, job_id, skip_lines, ctx):
        """
        Authorization Required: Ability to read from the workspace
        :param job_id:
        :param skip_lines:
        :param ctx:
        :return:
        """
        logging.debug(f"About to view logs for {job_id}")
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=False)
        logging.debug("Success, you have permission to view logs for " + job_id)
        return self._get_job_log(job_id, skip_lines)

    @staticmethod
    def _create_new_log(pk):
        jl = JobLog()
        jl.primary_key = pk
        jl.original_line_count = 0
        jl.stored_line_count = 0
        jl.lines = []
        return jl

    def add_job_logs(self, job_id, lines, ctx):
        """
        #TODO Prevent too many logs in memory
        #TODO Max size of log lines = 1000
        #TODO Error with out of space happened previously. So we just update line count.
        #TODO db.updateExecLogOriginalLineCount(ujsJobId, dbLog.getOriginalLineCount() + lines.size());

        #Authorization Required : Ability to read and write to the workspace
        :param job_id:
        :param lines:
        :param ctx:
        :return:
        """
        logging.debug(f"About to add logs for {job_id}")
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=True)
        logging.debug("Success, you have permission to view logs for " + job_id)

        log = self.get_mongo_util().get_job_log(job_id)
        if log is None:
            log = self._create_new_log(pk=job_id)

        olc = log.original_line_count

        # TODO Limit amount of lines per request?
        # TODO Maybe Prevent Some lines with TS and some without
        # TODO # Handle malformed requests?

        now = datetime.utcnow()

        for line in lines:
            olc += 1
            ll = LogLines()
            ll.error = line.get("error", False)
            ll.linepos = olc
            ll.ts = line.get("ts", now)
            ll.line = line.get("line")
            ll.validate()
            log.lines.append(ll)

        log.original_line_count = olc
        log.stored_line_count = olc

        with self.get_mongo_util().mongo_engine_connection():
            print(type(log))
            log.save()

        return log.stored_line_count

    def __init__(self, config, ctx=None):
        self.ctx = ctx
        self.deployment_config_fp = os.environ.get("KB_DEPLOYMENT_CONFIG")
        self.config = config
        self.mongo_util = None
        self.condor = None
        self.workspace = None
        catalog_url = config["catalog-url"]
        self.catalog = Catalog(catalog_url)

        self.workspace_url = config["workspace-url"]

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.debug
        )

    @staticmethod
    def status():
        return {"servertime": f"{time()}"}

    def cancel_job(self, job_id, ctx):
        """
        Authorization Required: Ability to Read and Write to the Workspace
        :param job_id:
        :param ctx:
        :return:
        """
        # Is it inefficient to get the job twice? Is it cached?
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=True)

        # Maybe cancel in condor first?
        self.get_mongo_util().update_job_status(
            job_id=job_id, status=Status.terminated.value
        )

        # Maybe if this call fails, then don't actually cancel the job?
        self.get_condor().cancel_job(job_id=job_id)

    def check_job_canceled(self, job_id, ctx):
        """
        Authorization Required: None
        Check to see if job is terminated by the user
        :return: job_id, whether or not job is canceled, and whether or not job is finished
        """

        job = self.get_mongo_util().get_job(job_id=job_id)

        if not job:
            raise ValueError("Cannot find job with id: {}".format(job_id))

        job_status = job.status

        rv = {"job_id": job_id, "canceled": False, "finished": False}

        if Status(job_status) is Status.terminated:
            rv["canceled"] = True
            rv["finished"] = True

        if Status(job_status) in [Status.finished, Status.error, Status.terminated]:
            rv["finished"] = True

        return rv

    def run_job(self, params, ctx):
        """

        :param params: RunJobParams object (See spec file)
        :param ctx: User_Id and Token from the request
        :return: The condor job id
        """
        # if 'wsid' not in params:
        #     raise Exception("Please provide wsid")

        if not self._can_write_ws(
            self.get_permissions_for_workspace(wsid=params["wsid"], ctx=ctx)
        ):
            logging.debug("You don't have permission to run jobs in this workspace")

        method = params.get("method")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"), ctx=ctx)

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(ctx["user_id"], params)

        # TODO Figure out log level
        logging.debug("About to run job with")
        logging.debug(client_groups)
        logging.debug(params)
        logging.debug(ctx)
        params["job_id"] = job_id
        params["user_id"] = ctx["user_id"]
        params["token"] = ctx["token"]
        params["cg_resources_requirements"] = client_groups
        try:
            submission_info = self.get_condor().run_job(params)
            condor_job_id = submission_info.clusterid
            logging.debug("Submitted job id and got ")
            logging.debug(condor_job_id)
        except Exception as e:
            ## delete job from database? Or mark it to a state it will never run?
            logging.error(e)
            raise e
        print("error is")
        print(type(submission_info))
        print(submission_info.error, type(submission_info.error))

        if submission_info.error is not None:
            raise submission_info.error
        if condor_job_id is None:
            raise Exception(
                "Condor job not ran, and error not found. Something went wrong"
            )

        logging.debug("Submission info is")
        logging.debug(submission_info)
        logging.debug(condor_job_id)
        logging.debug(type(condor_job_id))
        return job_id

    def get_permissions_for_workspace(self, wsid, ctx):

        username = ctx["user_id"]
        logging.debug(f"Checking permissions for workspace {wsid} for {username}")
        ws = self.get_workspace(ctx)
        logging.debug(ws)

        perms = ws.get_permissions_mass({"workspaces": [{"id": wsid}]})["perms"]

        ws_permission = self.WorkspacePermissions.NONE
        for p in perms:
            if username in p:
                ws_permission = self.WorkspacePermissions(p[username])
        return ws_permission

    @staticmethod
    def _can_read_ws(p):
        read_permissions = [
            SDKMethodRunner.WorkspacePermissions.ADMINISTRATOR,
            SDKMethodRunner.WorkspacePermissions.READ_WRITE,
            SDKMethodRunner.WorkspacePermissions.READ,
        ]
        return p in read_permissions

    @staticmethod
    def _can_write_ws(p):
        write_permissions = [
            SDKMethodRunner.WorkspacePermissions.ADMINISTRATOR,
            SDKMethodRunner.WorkspacePermissions.READ_WRITE,
        ]
        return p in write_permissions

    def check_permission_for_job(self, job_id, ctx, write=False):
        """
        Check for permissions to modify or read this record, based on WSID associated with the record
        :param job_id: The job id to look up to get it's WSID
        :param ctx: The REQUEST
        :param write: Whether or not to check for Read Permissions or Write Permissions
        :return:
        """
        with self.get_mongo_util().mongo_engine_connection():
            logging.debug(f"Getting job {job_id}")
            job = Job.objects(id=job_id)[0]
            logging.debug(f"Got {job}")
            permission = self.get_permissions_for_workspace(wsid=job.wsid, ctx=ctx)
            if write is True:
                permitted = self._can_write_ws(permission)
            else:
                permitted = self._can_read_ws(permission)

            if not permitted:
                raise PermissionError(
                    f"User {ctx['user_id']} does not have permissions to get status for wsid:{job.wsid}, job_id:{job_id} permission{permission}"
                )

    def get_job_params(self, job_id):
        job_params = dict()

        job = self.get_mongo_util().get_job(job_id=job_id)

        if not job:
            raise ValueError("Cannot find job with id: {}".format(job_id))

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

        if not (job_id and status):
            raise ValueError("Please provide both job_id and status")

        job = self.get_mongo_util().get_job(job_id=job_id)

        if not job:
            raise ValueError("Cannot find job with id: {}".format(job_id))

        job.status = status
        job.save()

        return str(job.id)

    def get_job_status(self, job_id):

        returnVal = dict()

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job = self.get_mongo_util().get_job(job_id=job_id)

        if not job:
            raise ValueError("Cannot find job with id: {}".format(job_id))

        returnVal['status'] = job.status

        return returnVal
