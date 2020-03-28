"""
Authors @bsadkhin, @tgu
All functions related to running a job, and starting a job, including the initial state change and
the logic to retrieve info needed by the runnner to start the job

"""
import logging
import time
from enum import Enum
from typing import Optional

from execution_engine2.db.models.models import (
    Job,
    JobInput,
    Meta,
    JobRequirements,
    Status,
)
from execution_engine2.utils.Condor import CondorResources
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

    def extract_special_params(self):
        """
        int request_cpu;
        int request_memory_mb;
        int request_disk_mb;
        string request_clientgroup;
        list<string> request_staging_volume_mounts;
        list<string> request_refdata_volume_mounts;
        :return:
        """
        pass

    def _create_job_requirements(self, resources, condor_resources):
        """ #TODO, maybe require specification of ALL resources, or else throw an exception
            #TODO, ADD more variables to condor ENVIRONMENT
            #TODO, move environment variables to a file
            #TODO, ADD VOLUME MOUNTS TO JOB INPUT MODEL
        """
        pass

    def _init_job_rec(
        self, user_id, params, resources: CondorResources = None
    ) -> Optional[str]:
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

        # special_resources = self.extract_special_params(params)

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

    def _get_module_git_commit(self, method, service_ver=None) -> Optional[str]:
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

    def _check_workspace_permissions(self, wsid):
        if wsid:
            if not self.sdkmr.get_workspace_auth().can_write(wsid):
                self.sdkmr.logger.debug(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )
                raise PermissionError(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )

    def _run(self, params):
        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"))
        method = params.get("method")
        # Normalize multiple formats into one format (csv vs json)
        app_settings = self.sdkmr.catalog_utils.get_client_groups(method)
        # These are for saving into job inputs. Maybe its best to pass this into condor as well?
        # TODO Validate MB/GB from both config and catalog.
        extracted_resources = self.sdkmr.get_condor().extract_resources(
            cgrr=app_settings
        )  # type: CondorResources

        # insert initial job document into db
        job_id = self._init_job_rec(self.sdkmr.user_id, params, extracted_resources)
        params["service_ver"] = self._get_module_git_commit(
            method, params.get("service_ver")
        )
        params["job_id"] = job_id
        params["user_id"] = self.sdkmr.user_id
        params["token"] = self.sdkmr.token
        params["cg_resources_requirements"] = app_settings

        self.sdkmr.logger.info(
            f"User {self.sdkmr.user_id} attempting to run job {method} {params}"
        )
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

    def run_concierge(self, params=None):
        self._check_workspace_permissions(params.get("wsid"))
        return self.run(params)

    def run(self, params=None, as_admin=False) -> Optional[str]:
        """
        :param params: SpecialRunJobParamsParams object (See spec file)
        :param params: RunJobParams object (See spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :return: The condor job id
        """
        if as_admin:
            self.sdkmr.check_as_admin(requested_perm=JobPermissions.WRITE)
        else:
            self._check_workspace_permissions(params.get("wsid"))
        return self.run(params)

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
        job = self.sdkmr.get_job_with_permission(
            job_id, JobPermissions.READ, as_admin=as_admin
        )

        job_input = job.job_input

        job_params["method"] = job_input.method
        job_params["params"] = job_input.params
        job_params["service_ver"] = job_input.service_ver
        job_params["app_id"] = job_input.app_id
        job_params["wsid"] = job_input.wsid
        job_params["parent_job_id"] = job_input.parent_job_id
        job_params["source_ws_objects"] = job_input.source_ws_objects

        return job_params
