"""
Authors @bsadkhin, @tgu
All functions related to running a job, and starting a job
"""
import logging
import time
from enum import Enum
from typing import AnyStr

# from lib.execution_engine2.SDKMethodRunner import SDKMethodRunner
from execution_engine2.db.models.models import (
    Job,
    JobInput,
    Meta,
    JobRequirements,
    Status,
)
from execution_engine2.utils.Condor import condor_resources
from execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange
from test.utils.test_utils import bootstrap

bootstrap()


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class RunJob:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr

    def _init_job_rec(
        self, user_id, params, resources: condor_resources = None
    ) -> AnyStr:
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

        if resources:
            jr = JobRequirements()
            jr.clientgroup = resources.client_group
            jr.cpu = resources.request_cpus
            # Should probably do some type checking on these before its passed in
            # Memory always in mb
            # Space always in gb

            jr.memory = resources.request_memory[:-1]
            jr.disk = resources.request_disk[:-2]
            inputs.requirements = jr

        job.job_input = inputs
        logging.info(job.job_input.to_mongo().to_dict())

        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            job.save()

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCreateJob(job_id=str(job.id), user=user_id)
        )

        return str(job.id)

    def _get_module_git_commit(self, method, service_ver=None) -> AnyStr:
        module_name = method.split(".")[0]

        if not service_ver:
            service_ver = "release"

        module_version = self.sdkmr.catalog_utils.catalog.get_module_version(
            {"module_name": module_name, "version": service_ver}
        )

        git_commit_hash = module_version.get("git_commit_hash")

        return git_commit_hash

    def _check_ws_objects(self, source_objects) -> None:
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.sdkmr.get_workspace().get_object_info3(
                {"objects": objects, "ignoreErrors": 1}
            )
            paths = info.get("paths")

            if None in paths:
                raise ValueError("Some workspace object is inaccessible")

    def run(self, params=None, as_admin=None) -> AnyStr:
        """
        :param params: RunJobParams object (See spec file)
        :return: The condor job id
        """
        wsid = params.get("wsid")
        ws_auth = self.sdkmr.get_workspace_auth()
        if wsid and not ws_auth.can_write(wsid):
            self.sdkmr.logger.debug(
                f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
            )
            raise PermissionError(
                f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
            )

        method = params.get("method")
        self.sdkmr.logger.info(
            f"User {self.sdkmr.user_id} attempting to run job {method}"
        )

        # Normalize multiple formats into one format (csv vs json)
        app_settings = self.sdkmr.catalog_utils.get_client_groups(method)

        # These are for saving into job inputs. Maybe its best to pass this into condor as well?
        extracted_resources = self.sdkmr.get_condor().extract_resources(
            cgrr=app_settings
        )
        # TODO Validate MB/GB from both config and catalog.

        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(self.sdkmr.user_id, params, extracted_resources)

        self.sdkmr.logger.debug("About to run job with")
        self.sdkmr.logger.debug(app_settings)

        params["job_id"] = job_id
        params["user_id"] = self.sdkmr.user_id
        params["token"] = self.sdkmr.token
        params["cg_resources_requirements"] = app_settings
        try:
            submission_info = self.sdkmr.get_condor().run_job(params)
            condor_job_id = submission_info.clusterid
            self.sdkmr.logger.debug(f"Submitted job id and got '{condor_job_id}'")
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

        self.sdkmr.logger.debug("Submission info is")
        self.sdkmr.logger.debug(submission_info)
        self.sdkmr.logger.debug(condor_job_id)
        self.sdkmr.logger.debug(type(condor_job_id))

        logging.info(f"Attempting to update job to queued  {job_id} {condor_job_id}")
        self.update_job_to_queued(job_id=job_id, scheduler_id=condor_job_id)
        self.sdkmr.slack_client.run_job_message(
            job_id=job_id, scheduler_id=condor_job_id, username=self.sdkmr.user_id
        )

        return job_id

    def update_job_to_queued(self, job_id, scheduler_id):
        # TODO RETRY FOR RACE CONDITION OF RUN/CANCEL
        # TODO PASS QUEUE TIME IN FROM SCHEDULER ITSELF?
        # TODO PASS IN SCHEDULER TYPE?
        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            j = self.sdkmr.get_mongo_util().get_job(job_id=job_id)
            previous_status = j.status
            j.status = Status.queued.value
            j.queued = time.time()
            j.scheduler_id = scheduler_id
            j.scheduler_type = "condor"
            j.save()

            self.sdkmr.kafka_client.send_kafka_message(
                message=KafkaQueueChange(
                    job_id=str(j.id),
                    new_status=j.status,
                    previous_status=previous_status,
                    scheduler_id=scheduler_id,
                )
            )

    def get_job_params(self, job_id, as_admin=False):
        """
        get_job_params: fetch SDK method params passed to job runner
        Parameters:
        job_id: id of job
        Returns:
        job_params:
        """
        job_params = dict()

        job = self.sdkmr._get_job_with_permission(job_id, JobPermissions.READ)

        job_input = job.job_input

        job_params["method"] = job_input.method
        job_params["params"] = job_input.params
        job_params["service_ver"] = job_input.service_ver
        job_params["app_id"] = job_input.app_id
        job_params["wsid"] = job_input.wsid
        job_params["parent_job_id"] = job_input.parent_job_id
        job_params["source_ws_objects"] = job_input.source_ws_objects

        return job_params
