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

from lib.execution_engine2.exceptions import (
    MultipleParentJobsException,
    CondorFailedJobSubmit,
    ExecutionEngineException,
)

from lib.execution_engine2.sdk.EE2Constants import ConciergeParams
from lib.execution_engine2.utils.CondorTuples import CondorResources, SubmissionInfo
from lib.execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class PreparedJobParams(NamedTuple):
    params: dict
    job_id: str


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
    from lib.execution_engine2.utils.CatalogUtils import CatalogUtils

class EE2RunJob:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr  # type: SDKMethodRunner
        self.override_clientgroup = os.environ.get("OVERRIDE_CLIENT_GROUP", None)
        self.logger = sdkmr.logger
        self.catalog_utils = sdkmr.catalog_utils  # type: CatalogUtils
        self.user_id = sdkmr.user_id


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
        self.logger.debug(job.to_mongo().to_dict())
        job.save()

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCreateJob(job_id=str(job.id), user=user_id)
        )

        return str(job.id)

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
                    f"User {self.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )
                raise PermissionError(
                    f"User {self.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )

    def _check_workspace_permissions_list(self, wsids):
        perms = self.sdkmr.get_workspace_auth().can_write_list(wsids)
        bad_ws = [key for key in perms.keys() if perms[key] is False]
        if bad_ws:
            self.logger.debug(
                f"User {self.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
            )
            raise PermissionError(
                f"User {self.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
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

    def _add_essential_job_info(
        self, job_params: Dict, job_id: str, normalized_resources
    ) -> Dict:
        """
        Mutate job params to include job_id, user, token, and resources
        :param job_params: Params to mutate
        :param job_id: Add EE2 Job ID
        :param normalized_resources: ClientGroups and Resource Requirements
        :return: job_params
        """

        job_params["job_id"] = job_id
        job_params["user_id"] = self.user_id
        job_params["token"] = self.sdkmr.token
        job_params["cg_resources_requirements"] = normalized_resources
        return job_params

    def _async__submit_child_batch_jobs(self, child_job_params_set, child_job_ids):
        child_job_params = []
        start_catalog_lookup = time.time()
        for i, child_job_param in enumerate(child_job_params_set):
            method = child_job_param.get("method")
            normalized_resources = self.sdkmr.catalog_utils.get_normalized_resources(
                method
            )
            job_id = child_job_ids[i]
            child_job_params.append(
                self._add_essential_job_info(
                    job_params=child_job_param,
                    job_id=job_id,
                    normalized_resources=normalized_resources,
                )
            )
        self.logger.debug(
            f"3. Time spent start_catalog_lookup {time.time() - start_catalog_lookup} "
        )

        start_batch_submit = time.time()

        batch_submission_info = self._submit_batch(
            child_job_params=child_job_params
        )  # type: List[SubmissionInfo]
        child_job_ids = []

        self.logger.debug(
            f"4. Time spent actually submitting batch job {time.time() - start_batch_submit} "
        )

        time_post_submitting = time.time()
        for i, child_job_param in enumerate(child_job_params_set):
            child_job_id = child_job_param["job_id"]
            child_sub_info = batch_submission_info[i]  # type: SubmissionInfo
            child_scheduler_id = child_sub_info.clusterid
            child_job_ids.append(child_scheduler_id)
            self.update_job_to_queued(
                job_id=child_job_id, scheduler_id=child_scheduler_id
            )

        self.logger.debug(
            f"5. Time spent updating jobs to queued {time.time() - time_post_submitting} "
        )
        return child_job_ids

    def _submit_child_batch_jobs(self, child_job_params_set, child_job_ids):
        """
        * The workspace objects have already been checked, along with resources/clientgroups for the jobs
        in _initialize_child_batch_job_records
        * The ee2 job ids already exist
        * Now to prepare and actually submit the jobs to condor, or fail if the submission is malformed
        """
        condor_submit_time = time.time()
        child_job_scheduler_ids = self._async__submit_child_batch_jobs(
            child_job_params_set, child_job_ids
        )
        self.logger.debug(
            f"2. Time spent submitting child batch jobs {time.time() - condor_submit_time} "
        )

        # TODO Print em
        return child_job_scheduler_ids

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
            self.user_id, params, extracted_resources, concierge_params
        )
        params = self._add_essential_job_info(
            job_params=params, job_id=job_id, normalized_resources=normalized_resources
        )

        self.logger.debug(
            f"User {self.user_id} attempting to run job {method} {params}"
        )

        return PreparedJobParams(params=params, job_id=job_id)


    def _submit_batch(self, child_job_params: List[Dict]) -> List[SubmissionInfo]:
        try:
            submission_info_set = self.sdkmr.get_condor().run_job_batch(
                batch_job_params=child_job_params
            )
        except CondorFailedJobSubmit as cfjs:
            self.logger.error(cfjs)
            try:
                for child_job_param in child_job_params:
                    child_job_id = child_job_param["job_id"]
                    self._finish_created_job(job_id=child_job_id, exception=cfjs)
            except ExecutionEngineException as fj:
                self.logger.error(fj)
            raise cfjs
        return submission_info_set

        # TODO This part about inspecting the failure and cancelling them if no info was returned from condor
        # due to some other issue

        # if submission_info.error is not None and isinstance(
        #     submission_info.error, Exception
        # ):
        #     self._finish_created_job(exception=submission_info.error, job_id=job_id)
        #     raise submission_info.error
        # if condor_job_id is None:
        #     error_msg = "Condor job not ran, and error not found. Something went wrong"
        #     self._finish_created_job(job_id=job_id, exception=RuntimeError(error_msg))
        #     raise RuntimeError(error_msg)
        #
        # # Submission info available in {submission_info} but contains sensitive info
        # self.logger.debug(
        #     f"Attempting to update job to queued  {job_id} {condor_job_id}"
        # )

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

    def _evaluate_job_params_set(self, job_param_set, parent_job):
        # Fail early before job submission if possible if job params are incorrect
        # Make sure clientgroups and condor resources are set correctly for each job,
        # or raise an exception if something goes wrong
        for job_params in job_param_set:
            self.catalog_utils.get_git_commit_version(job_params=job_params)
            self.catalog_utils.get_condor_resources(
                job_params=job_params, condor=self.sdkmr.get_condor()
            )
            # Don't allow differing parent jobs, and ensure job always runs with the parent
            if (
                "parent_job_id" in job_params
                and job_params["parent_job_id"] != parent_job.id
            ):
                raise MultipleParentJobsException(
                    "Launching child jobs with differing parents is not allowed"
                    + f"parent_job.wsid={parent_job.id} "
                    + f"batch_job_params.wsid={job_params['parent_job_id']} "
                )

    def _run_batch(self, parent_job: Job, job_param_set: List[Dict]) -> List:
        # Prepare jobs, fail early if possible
        self._evaluate_job_params_set(job_param_set, parent_job)

        child_job_ids = []
        # Initialize Job Records
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
            job_id=job_id, scheduler_id=condor_job_id, username=self.user_id
        )

        return job_id

    def _abort_child_jobs(self, child_job_ids):
        """
        Cancel a list of child jobs, and their child jobs
        :param child_job_ids: List of job ids to abort
        :return:
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

    def _intialize_child_job_records(self, child_job_params_set, parent_job_id):
        child_job_ids = []
        # Initialize Job Records
        for job_params in child_job_params_set:
            job_params["parent_job_id"] = parent_job_id
            resources = self.catalog_utils.get_condor_resources(
                job_params=job_params, condor=self.sdkmr.get_condor()
            )
            try:

                child_job_id = self._init_job_rec(
                    user_id=self.user_id,
                    params=job_params,
                    resources=resources,
                )
                child_job_ids.append(child_job_id)
                self.sdkmr.kafka_client.send_kafka_message(
                    message=KafkaCreateJob(job_id=child_job_id, user=self.user_id)
                )
            except Exception as e:
                self.logger.debug(
                    msg=f"Failed to submit child job. Aborting entire batch job {e}"
                )
                self._abort_child_jobs(child_job_ids)
                raise e

        return child_job_ids

    def _initialize_child_batch_job_records(
        self, parent_job: Job, child_job_params_set: List[Dict]
    ) -> List:
        """
        * Checks to see if params are valid or not

        :param parent_job: The parent job record
        :param child_job_params_set: The set of params for child jobs to be run in batch
        :return: A list of child job ids for saved entries in ee2, in "Created" status
        """
        intialize_child_batch_time = time.time()
        # Prepare jobs, fail early if possible
        self._evaluate_job_params_set(child_job_params_set, parent_job)
        # Prepare child jobs
        child_job_ids = self._intialize_child_job_records(
            child_job_params_set=child_job_params_set, parent_job_id=parent_job.id
        )
        # Save record of child jobs in parent
        parent_job.child_jobs = child_job_ids
        parent_job.save()

        self.logger.debug(
            f"Time spent _initialize_child_batch_job_records  {time.time() - intialize_child_batch_time} "
        )
        return child_job_ids

    def _create_parent_job(self, wsid, meta):
        """
        :param wsid: The workspace ID
        :param meta: Cell Information
        :return: Job Record of Parent for Batch Job
        """
        parent_start_time = time.time()
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

        j = Job(
            job_input=job_input,
            batch_job=True,
            status=Status.created.value,
            wsid=wsid,
            user=self.user_id,
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
        Check permissions, setup parent job, and child jobs.
        :param params: List of RunJobParams (See Spec File)
        :param batch_params: List of Batch Params, such as wsid (See Spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :return: A list of condor job ids or a failure notification
        """
        # TODO: Do we need a kafka event for the parent job?
        # TODO: Do we need a nice Exception for failing a batch submission?
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
            f"1. Time spent looking up workspace permissions {time.time() - workspace_permissions_time} "
        )
        parent_job = self._create_parent_job(wsid=wsid, meta=meta)
        child_job_ids = self._initialize_child_batch_job_records(
            parent_job=parent_job, child_job_params_set=params
        )

        # Submit jobs to the scheduler
        self._submit_child_batch_jobs(
            child_job_params_set=params, child_job_ids=child_job_ids
        )

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
        """
        Update a "Created" (Or any other) job to "Queued"
        :param job_id: The job to transition to queued
        :param scheduler_id: Add the condor_id upon successful queue
        :return:
        """
        # TODO RETRY FOR RACE CONDITION OF RUN/CANCEL
        # TODO PASS QUEUE TIME IN FROM SCHEDULER ITSELF?
        # TODO PASS IN SCHEDULER TYPE?

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
