"""
Authors @bsadkhin, @tgu
All functions related to running a job, and starting a job, including the initial state change and
the logic to retrieve info needed by the runnner to start the job

"""
import os
import time
from enum import Enum
from typing import Optional, Dict, NamedTuple, Union, List

from lib.execution_engine2.db.models.models import (
    Job,
    JobInput,
    Meta,
    JobRequirements,
    Status,
    ErrorCode,
    TerminatedCode,
)
from lib.execution_engine2.exceptions import MultipleParentJobsException
from lib.execution_engine2.sdk.EE2Constants import ConciergeParams
from lib.execution_engine2.utils.CondorTuples import CondorResources
from lib.execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class PreparedJobParams(NamedTuple):
    params: dict
    job_id: str


class JobResourceMappings(NamedTuple):
    condor_resources: dict
    method_service_versions: dict


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
    from lib.execution_engine2.utils.CatalogUtils import CatalogUtils
    from lib.execution_engine2.utils import Condor


class EE2RunJob:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr  # type: SDKMethodRunner
        self.override_clientgroup = os.environ.get("OVERRIDE_CLIENT_GROUP", None)
        self.logger = sdkmr.logger
        self.catalog_utils = sdkmr.catalog_utils  # type: CatalogUtils
        self.condor = self.sdkmr.get_condor()  # type: Condor

    def _init_job_rec(
        self,
        user_id: str,
        params: Dict,
        resources: CondorResources = None,
        concierge_params: ConciergeParams = None,
    ) -> str:
        job = Job()
        inputs = JobInput()
        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get("wsid")
        job.status = "created"
        # Inputs
        inputs.wsid = job.wsid
        inputs.method = params.get("method")
        inputs.params = params.get("params")

        params["service_ver"] = self.catalog_utils.get_git_commit_version(
            job_params=params
        )

        inputs.service_ver = params.get("service_ver")

        inputs.app_id = params.get("app_id")
        inputs.source_ws_objects = params.get("source_ws_objects")
        inputs.parent_job_id = str(params.get("parent_job_id"))
        inputs.narrative_cell_info = Meta()
        meta = params.get("meta")

        if meta:
            for meta_attr in ["run_id", "token_id", "tag", "cell_id", "status"]:
                inputs.narrative_cell_info[meta_attr] = meta.get(meta_attr)

        if resources:
            # TODO Should probably do some type checking on these before its passed in
            jr = JobRequirements()
            if concierge_params:
                jr.cpu = concierge_params.request_cpus
                jr.memory = concierge_params.request_memory
                jr.disk = concierge_params.request_disk
                jr.clientgroup = concierge_params.client_group
            else:
                jr.clientgroup = resources.client_group
                if self.override_clientgroup:
                    jr.clientgroup = self.override_clientgroup
                jr.cpu = resources.request_cpus
                jr.memory = resources.request_memory[:-1]  # Memory always in mb
                jr.disk = resources.request_disk[:-2]  # Space always in gb

            inputs.requirements = jr

        job.job_input = inputs
        self.logger.debug(job.job_input.to_mongo().to_dict())

        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            self.logger.debug(job.to_mongo().to_dict())
            job.save()

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCreateJob(job_id=str(job.id), user=user_id)
        )

        return str(job.id)

    def _get_module_git_commit(self, method, service_ver=None) -> Optional[str]:
        return self.catalog_utils.get_git_commit_version()

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
                self.logger.debug(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )
                raise PermissionError(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )

    def _check_workspace_permissions_list(self, wsids):
        perms = self.sdkmr.get_workspace_auth().can_write_list(wsids)
        bad_ws = [key for key in perms.keys() if perms[key] is False]
        if bad_ws:
            self.logger.debug(
                f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
            )
            raise PermissionError(
                f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
            )

    def _finish_created_job(
        self, job_id, exception, error_code=None, error_message=None
    ):
        """
        :param job_id:
        :param exception:
        :param error_code:
        :param error_message:
        :return:
        """
        if error_message is None:
            error_message = f"Cannot submit to condor {exception}"
        if error_code is None:
            error_code = ErrorCode.job_crashed.value

        # TODO Better Error Parameter? A Dictionary?
        self.sdkmr.finish_job(
            job_id=job_id,
            error_message=error_message,
            error_code=error_code,
            error=f"{exception}",
        )

    def _prepare_to_run(self, params, concierge_params=None) -> PreparedJobParams:
        """
        Creates a job record, grabs info about the objects,
        checks the catalog resource requirements, and submits to condor
        """
        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"))
        method = params.get("method")
        # Normalize multiple formats into one format (csv vs json)
        normalized_resources = self.sdkmr.catalog_utils.get_normalized_resources(method)
        # These are for saving into job inputs. Maybe its best to pass this into condor as well?
        extracted_resources = self.sdkmr.get_condor().extract_resources(
            cgrr=normalized_resources
        )  # type: CondorResources
        # insert initial job document into db

        job_id = self._init_job_rec(
            self.sdkmr.user_id, params, extracted_resources, concierge_params
        )

        params["job_id"] = job_id
        params["user_id"] = self.sdkmr.user_id
        params["token"] = self.sdkmr.token
        params["cg_resources_requirements"] = normalized_resources

        self.logger.debug(
            f"User {self.sdkmr.user_id} attempting to run job {method} {params}"
        )

        return PreparedJobParams(params=params, job_id=job_id)

    def _submit(self, params, concierge_params, job_id):
        try:
            submission_info = self.sdkmr.get_condor().run_job(
                params=params, concierge_params=concierge_params
            )
            condor_job_id = submission_info.clusterid
            self.logger.debug(f"Submitted job id and got '{condor_job_id}'")
        except Exception as e:
            self.logger.error(e)
            self._finish_created_job(job_id=job_id, exception=e)
            raise e

        if submission_info.error is not None and isinstance(
            submission_info.error, Exception
        ):
            self._finish_created_job(exception=submission_info.error, job_id=job_id)
            raise submission_info.error
        if condor_job_id is None:
            error_msg = "Condor job not ran, and error not found. Something went wrong"
            self._finish_created_job(job_id=job_id, exception=RuntimeError(error_msg))
            raise RuntimeError(error_msg)

        # Submission info available in {submission_info} but contains sensitive info
        self.logger.debug(
            f"Attempting to update job to queued  {job_id} {condor_job_id}"
        )

    # def _cache_catalog_resources(self, param_set):
    #     # Get a list of all of the methods
    #     # Get their clientgroups and resources from the catalog
    #     # Cache method resources and versions
    #     mr_cache = self.catalog_utils.get_mass_resources()
    #     mv_cache = dict()
    #     for param in param_set:
    #         method = param['method']
    #         service_ver = param['service_ver']
    #         # Get resources for method
    #         if method not in mr_cache:
    #             self.sdkmr.get_condor().extract_resources()
    #             mr_cache[method] = self.catalog_utils.get_normalized_resources(method=method)
    #         # Get Method Version via Git Commit
    #         if method not in mv_cache and service_ver not in mv_cache[method]:
    #             mv_cache[method][service_ver]= self._get_module_git_commit(method=method,
    #                                                                        service_ver=service_ver)
    #
    #     return CatalogUtils

    # TODO delete this after checking it in
    # def _get_cached_catalog_resources(
    #     self, job_param_set: List[dict]
    # ) -> JobResourceMappings:
    #     # Check workspace objects for each of the jobs
    #     workspace_check_time = time.time()
    #     [
    #         self._check_ws_objects(param.get("source_ws_objects"))
    #         for param in job_param_set
    #     ]
    #     self.logger.debug(
    #         f"Took {time.time() - workspace_check_time} to check all workspace objects"
    #     )
    #     #Get a mapping of condor resources
    #     catalog_check_time = time.time()
    #     condor_resources_mapping = self.catalog_utils.get_mass_resources(
    #         job_param_set=job_param_set, condor=self.condor
    #     )
    #     self.logger.debug(
    #         f"Took {time.time() - catalog_check_time} to get all condor resources"
    #     )
    #     # Get a mapping of method git versions. If one is not valid, throw the catalog error #TODO Test this
    #     catalog_check_time_git = time.time()
    #
    #
    #     return JobResourceMappings(
    #         condor_resources=condor_resources_mapping,
    #         method_service_versions=service_versions_mapping,
    #     )

    def _evaluate_job_params_set(self, job_param_set, parent_job):
        # Fail early before job submission if possible
        # Make sure clientgroups and condor resources are set correctly for each job,
        # or raise an exception if something goes wrong
        for job_params in job_param_set:
            self.catalog_utils.get_git_commit_version(job_params=job_params)
            self.catalog_utils.get_condor_resources(
                job_params=job_params, condor=self.condor
            )
            # Don't allow differing parent jobs, and ensure job always runs with the parent
            if (
                "parent_job_id" in job_params
                and job_params["parent_job_id"] != parent_job.id
            ):
                raise MultipleParentJobsException(
                    "Launching child jobs with differing parents is not allowed"
                )

    def _run_batch(self, parent_job: Job, job_param_set: List[Dict]) -> List:
        # Prepare jobs, fail early if possible
        self._evaluate_job_params_set(job_param_set, parent_job)

        child_job_ids = []
        # Initiatialize Job Records
        for job_params in job_param_set:
            job_params["parent_job_id"]: parent_job.id
            resources = self.catalog_utils.get_condor_resources(
                job_params=job_params, condor=self.condor
            )
            try:
                child_job_id = self._init_job_rec(
                    user_id=self.sdkmr.user_id,
                    params=job_params,
                    resources=resources,
                )
                child_job_ids.append(child_job_id)
            except Exception as e:
                self.logger.debug(
                    msg=f"Failed to submit child job. Aborting entire batch job {e}"
                )
                self._abort_child_jobs(child_job_ids)
                raise e
        # Save record of child jobs
        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            parent_job.child_jobs = child_job_ids
            parent_job.save()

        # TODO Launch Job Submission Thread
        return child_job_ids

    def _run(self, params, concierge_params=None):
        prepared = self._prepare_to_run(
            params=params, concierge_params=concierge_params
        )
        params = prepared.params
        job_id = prepared.job_id
        condor_job_id = self._submit(
            params=params, concierge_params=concierge_params, job_id=job_id
        )

        self.update_job_to_queued(job_id=job_id, scheduler_id=condor_job_id)
        self.sdkmr.slack_client.run_job_message(
            job_id=job_id, scheduler_id=condor_job_id, username=self.sdkmr.user_id
        )

        return job_id

    def _abort_child_jobs(self, child_job_ids):
        """
        Cancel a list of child jobs, and their child jobs
        """
        for child_job_id in child_job_ids:
            try:
                self.sdkmr.cancel_job(
                    job_id=child_job_id,
                    terminated_code=TerminatedCode.terminated_by_batch_abort.value,
                )
            except Exception as e:
                # TODO Maybe add a retry here?
                self.logger.error(f"Couldn't cancel child job {e}")

    def _create_parent_job(self, wsid, meta):
        """
        This creates the parent job for all children to mark as their ancestor
        :param params:
        :return:
        """
        job_input = JobInput()
        job_input.service_ver = "batch"
        job_input.app_id = "batch"
        job_input.method = "batch"
        job_input.narrative_cell_info = Meta()

        if meta:
            job_input.narrative_cell_info.run_id = meta.get("run_id")
            job_input.narrative_cell_info.token_id = meta.get("token_id")
            job_input.narrative_cell_info.tag = meta.get("tag")
            job_input.narrative_cell_info.cell_id = meta.get("cell_id")
            job_input.narrative_cell_info.status = meta.get("status")

        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            j = Job(
                job_input=job_input,
                batch_job=True,
                status=Status.created.value,
                wsid=wsid,
                user=self.sdkmr.user_id,
            )
            j.save()

        # TODO Do we need a new kafka call?
        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCreateJob(job_id=str(j.id), user=j.user)
        )
        return j

    def run_batch(
        self, params, batch_params, as_admin=False
    ) -> Dict[str, Union[Job, List[str]]]:
        """
        :param params: List of RunJobParams (See Spec File)
        :param batch_params: List of Batch Params, such as wsid (See Spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :return: A list of condor job ids or a failure notification
        """
        # TODO: Do we need a kafka event for the parent job?
        wsid = batch_params.get("wsid")
        meta = batch_params.get("meta")
        workspace_permissions_time = time.time()

        if as_admin:
            self.sdkmr.check_as_admin(requested_perm=JobPermissions.WRITE)
        else:
            # Make sure you aren't running a job in someone elses workspace
            self._check_workspace_permissions(wsid)
            wsids = [job_input.get("wsid", wsid) for job_input in params]
            self._check_workspace_permissions_list(wsids)

        self.logger.debug(
            f"Time spent looking up workspace permissions {time.time() - workspace_permissions_time} "
        )

        parent_job = self._create_parent_job(wsid=wsid, meta=meta)
        child_job_ids = self._run_batch(parent_job=parent_job, job_param_set=params)
        return {"parent_job_id": str(parent_job.id), "child_job_ids": child_job_ids}

    def run(
        self, params=None, as_admin=False, concierge_params: Dict = None
    ) -> Optional[str]:
        """
        :param params: SpecialRunJobParamsParams object (See spec file)
        :param params: RunJobParams object (See spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :param concierge_params: Allows you to specify request_cpu, request_memory, request_disk, clientgroup
        :return: The condor job id
        """
        if as_admin:
            self.sdkmr.check_as_admin(requested_perm=JobPermissions.WRITE)
        else:
            self._check_workspace_permissions(params.get("wsid"))

        if concierge_params:
            cp = ConciergeParams(**concierge_params)
            self.sdkmr.check_as_concierge()
        else:
            cp = None

        return self._run(params=params, concierge_params=cp)

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
