import json
import logging
import os
import re
from datetime import datetime
from enum import Enum

from mongoengine import connect, disconnect

from execution_engine2.models.models import Job, JobInput, JobLog, LogLines, Meta
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.MongoUtil import MongoUtil
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace

logging.basicConfig(level=logging.INFO)
logging.info(json.loads(os.environ.get("debug", "False").lower()))


class SDKMethodRunner:
    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """
        if method is None:
            raise ValueError("Please input module_name.function_name")

        pattern = re.compile(".*\..*")
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

    def _check_ws_objects(self, source_objects):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.workspace.get_object_info3(
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

        # TODO Add Meta Fields From Params
        inputs.job_input.narrative_cell_info = Meta()

        job.job_input = inputs

        with self.get_mongo_util().me_collection():
            job.save()

        return str(job.id)

    def get_mongo_util(self):
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self):
        if self.condor is None:
            self.condor = Condor(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        return self.condor

    def get_workspace(self, ctx):
        if ctx is None:
            raise Exception("Need to provide credentials for the workspace")
        if self.workspace is None:
            self.workspace = Workspace(ctx["token"])
        return self.workspace

    class WorkspacePermissions(Enum):
        ADMINISTRATOR = "a"
        READ_WRITE = "w"
        READ = "r"
        NONE = "n"

    def connect_to_mongoengine(self):
        mu = self.get_mongo_util()
        connect(
            db=mu.mongo_database,
            host=mu.mongo_host,
            port=mu.mongo_port,
            authentication_source=mu.mongo_database,
            username=mu.mongo_user,
            password=mu.mongo_pass,
            alias="ee2",
        )

    def get_workspace_permissions(self, wsid, ctx):
        # Look up permissions for this workspace
        permission = self.get_workspace(ctx).get_permissions_mass([wsid])[0]
        return self.WorkspacePermissions(permission)

    def get_job_status(self, job_id, ctx):
        self.connect_to_mongoengine()
        job = Job.objects(id=job_id)[0]
        p = self.get_workspace_permissions(wsid=job.wsid, ctx=ctx)
        if p not in [
            self.WorkspacePermissions.ADMINISTRATOR,
            self.WorkspacePermissions.READ_WRITE,
            self.WorkspacePermissions.READ,
        ]:
            raise PermissionError(
                f"User {ctx['user']} does not have permissions to get status for wsid:{job.wsid}, job_id:{job_id}"
            )

        disconnect(alias="ee2")
        # Return the job status

    def view_job_logs(self, job_id, ctx):
        job = JobLog.objects(id=job_id)[0]
        p = self.get_workspace_permissions(wsid=job.wsid, ctx=ctx)
        if p not in [
            self.WorkspacePermissions.ADMINISTRATOR,
            self.WorkspacePermissions.READ_WRITE,
            self.WorkspacePermissions.READ,
        ]:
            raise PermissionError(
                f"User {ctx['user']} does not have permissions to view job logs for wsid:{job.wsid}, job_id:{job_id}"
            )
        # Return the log

    def add_job_logs(self, job_id, lines, ctx):
        with self.get_mongo_util().me_collection():
            jl = JobLog.objects(id=job_id)[0]  # type: JobLog
            p = self.get_workspace_permissions(wsid=jl.wsid, ctx=ctx)
            if p not in [
                self.WorkspacePermissions.ADMINISTRATOR,
                self.WorkspacePermissions.READ_WRITE,
            ]:
                raise PermissionError(
                    f"User {ctx['user']} does not have permissions to view job logs for wsid:{job.wsid}, job_id:{job_id}"
                )

            linepos = jl.stored_line_count
            line_count = jl.original_line_count
            now = datetime.utcnow()

            for line in lines:
                line_count += 1
                l = LogLines()
                l.line = line.get("line", "")
                l.linepos = line_count
                l.error = line.get("error", 0)
                l.ts = line.get("timestamp", now)

        # Return the log

    def __init__(self, config):
        self.config = config
        self.mongo_util = None
        self.condor = None
        self.workspace = None
        catalog_url = config["catalog-url"]
        self.catalog = Catalog(catalog_url)

        workspace_url = config["workspace-url"]
        self.workspace = Workspace(workspace_url)

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.INFO
        )

    def run_job(self, params, ctx):
        """

        :param params: RunJobParams object (See spec file)
        :param ctx: User_Id and Token from the request
        :return: The condor job id
        """

        method = params.get("method")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(params.get("source_ws_objects"))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(ctx["user_id"], params)

        # TODO Figure out log level
        logging.info("About to run job with")
        logging.info(client_groups)
        logging.info(params)
        logging.info(ctx)
        params["job_id"] = job_id
        params["user_id"] = ctx["user_id"]
        params["token"] = ctx["token"]
        params["cg_resources_requirements"] = client_groups
        try:
            submission_info = self.get_condor().run_job(params)
            condor_job_id = submission_info.clusterid
            logging.info("Submitted job id and got ")
            logging.info(condor_job_id)
        except Exception as e:
            ## delete job from database? Or mark it to a state it will never run?
            logging.error(e)
            raise e

        if submission_info.error is not None or condor_job_id is None:
            raise (submission_info.error)

        logging.info("Submission info is")
        logging.info(submission_info)
        logging.info(condor_job_id)
        logging.info(type(condor_job_id))
        return condor_job_id
