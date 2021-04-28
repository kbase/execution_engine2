"""
Authors @bsadkhin, @tgu
All functions related to running a job, and starting a job, including the initial state change and
the logic to retrieve info needed by the runnner to start the job

"""
import os
import time
from enum import Enum
from typing import Optional, Dict, NamedTuple, Union, List, Any

from execution_engine2.db.models.models import (
    Job,
    JobInput,
    Meta,
    JobRequirements,
    Status,
    ErrorCode,
    TerminatedCode,
)
from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements as ResolvedRequirements,
    AppInfo,
    UserCreds,
)
from execution_engine2.sdk.EE2Constants import CONCIERGE_CLIENTGROUP
from execution_engine2.utils.job_requirements_resolver import (
    REQUEST_CPUS,
    REQUEST_DISK,
    REQUEST_MEMORY,
    CLIENT_GROUP,
    CLIENT_GROUP_REGEX,
    DEBUG_MODE,
)
from execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange
from execution_engine2.exceptions import IncorrectParamsException


_JOB_REQUIREMENTS = "job_reqs"
_REQUIREMENTS_LIST = "requirements_list"
_METHOD = "method"
_APP_ID = "app_id"
_PARENT_JOB_ID = "parent_job_id"
_WORKSPACE_ID = "wsid"
_SOURCE_WS_OBJECTS = "source_ws_objects"


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


class EE2RunJob:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr  # type: SDKMethodRunner
        self.override_clientgroup = os.environ.get("OVERRIDE_CLIENT_GROUP", None)
        self.logger = self.sdkmr.get_logger()

    def _init_job_rec(
        self,
        user_id: str,
        params: Dict,
    ) -> str:
        job = Job()
        inputs = JobInput()
        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get(_WORKSPACE_ID)
        job.status = "created"
        # Inputs
        inputs.wsid = job.wsid
        inputs.method = params.get(_METHOD)
        inputs.params = params.get("params")

        params["service_ver"] = self._get_module_git_commit(
            params.get(_METHOD), params.get("service_ver")
        )
        inputs.service_ver = params.get("service_ver")

        inputs.app_id = params.get(_APP_ID)
        inputs.source_ws_objects = params.get(_SOURCE_WS_OBJECTS)
        inputs.parent_job_id = str(params.get(_PARENT_JOB_ID))
        inputs.narrative_cell_info = Meta()
        meta = params.get("meta")

        if meta:
            for meta_attr in ["run_id", "token_id", "tag", "cell_id"]:
                inputs.narrative_cell_info[meta_attr] = meta.get(meta_attr)

        jr = JobRequirements()
        jr.cpu = params[_JOB_REQUIREMENTS].cpus
        jr.memory = params[_JOB_REQUIREMENTS].memory_MB
        jr.disk = params[_JOB_REQUIREMENTS].disk_GB
        jr.clientgroup = params[_JOB_REQUIREMENTS].client_group
        inputs.requirements = jr

        job.job_input = inputs
        job_id = self.sdkmr.save_job(job)

        self.sdkmr.get_kafka_client().send_kafka_message(
            message=KafkaCreateJob(job_id=job_id, user=user_id)
        )

        return job_id

    def _get_module_git_commit(self, method, service_ver=None) -> Optional[str]:
        module_name = method.split(".")[0]

        if not service_ver:
            service_ver = "release"

        self.logger.debug(f"Getting commit for {module_name} {service_ver}")

        module_version = self.sdkmr.get_catalog().get_module_version(
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

    def _prepare_to_run(self, params, concierge_params=None) -> JobSubmissionParameters:
        """
        Creates a job record and creates the job submission params
        """

        job_id = self._init_job_rec(self.sdkmr.get_user_id(), params)

        self.logger.debug(
            f"User {self.sdkmr.get_user_id()} attempting to run job {params[_METHOD]} {params}"
        )

        return JobSubmissionParameters(
            job_id,
            AppInfo(params[_METHOD], params[_APP_ID]),
            params[_JOB_REQUIREMENTS],
            UserCreds(self.sdkmr.get_user_id(), self.sdkmr.get_token()),
            parent_job_id=params.get(_PARENT_JOB_ID),
            wsid=params.get(_WORKSPACE_ID),
            source_ws_objects=params.get(_SOURCE_WS_OBJECTS),
        )

    def _run(self, params):
        job_params = self._prepare_to_run(params=params)
        job_id = job_params.job_id

        try:
            submission_info = self.sdkmr.get_condor().run_job(params=job_params)
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

        self.update_job_to_queued(job_id=job_id, scheduler_id=condor_job_id)
        self.sdkmr.get_slack_client().run_job_message(
            job_id=job_id, scheduler_id=condor_job_id, username=self.sdkmr.get_user_id()
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

        j = Job(
            job_input=job_input,
            batch_job=True,
            status=Status.created.value,
            wsid=wsid,
            user=self.sdkmr.get_user_id(),
        )
        j = self.sdkmr.save_and_return_job(j)

        # TODO Do we need a new kafka call?
        self.sdkmr.get_kafka_client().send_kafka_message(
            message=KafkaCreateJob(job_id=str(j.id), user=j.user)
        )
        return j

    def _run_batch(self, parent_job: Job, params):
        child_jobs = []
        for job_param in params:
            job_param[_PARENT_JOB_ID] = str(parent_job.id)
            try:
                child_jobs.append(str(self._run(params=job_param)))
            except Exception as e:
                self.logger.debug(
                    msg=f"Failed to submit child job. Aborting entire batch job {e}"
                )
                self._abort_child_jobs(child_jobs)
                raise e

        parent_job.child_jobs = child_jobs
        self.sdkmr.save_job(parent_job)

        return child_jobs

    def run_batch(
        self, params, batch_params, as_admin=False
    ) -> Dict[str, Union[Job, List[str]]]:
        """
        :param params: List of RunJobParams (See Spec File)
        :param batch_params: List of Batch Params, such as wsid (See Spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :return: A list of condor job ids or a failure notification
        """
        if type(params) != list:
            raise IncorrectParamsException("params must be a list")
        wsid = batch_params.get(_WORKSPACE_ID)
        meta = batch_params.get("meta")
        if as_admin:
            self.sdkmr.check_as_admin(requested_perm=JobPermissions.WRITE)
        else:
            # Make sure you aren't running a job in someone elses workspace
            self._check_workspace_permissions(wsid)
            # this is very odd. Why check the parent wsid again if there's no wsid in the job?
            # also, what if the parent wsid is None?
            # also also, why not just put all the wsids in one list and make one ws call?
            wsids = [job_input.get(_WORKSPACE_ID, wsid) for job_input in params]
            self._check_workspace_permissions_list(wsids)

        self._add_job_requirements(params)
        self._check_job_arguments(params, has_parent_job=True)

        parent_job = self._create_parent_job(wsid=wsid, meta=meta)
        children_jobs = self._run_batch(parent_job=parent_job, params=params)
        return {_PARENT_JOB_ID: str(parent_job.id), "child_job_ids": children_jobs}

    # modifies the jobs in place
    def _add_job_requirements(self, jobs: List[Dict[str, Any]]):
        f"""
        Adds the job requirements, generated from the job requirements resolver,
        to the provided RunJobParams dicts. Expects the required field {_METHOD} in the param
        dicts. Adds the {_JOB_REQUIREMENTS} field to the param dicts, which holds the value of the
        job requirements object.
        """
        # could add a cache in the job requirements resolver to avoid making the same
        # catalog call over and over if all the jobs have the same method
        jrr = self.sdkmr.get_job_requirements_resolver()
        for j in jobs:
            # TODO JRR check if requesting any job requirements & if is admin
            # TODO JRR actually process the requirements once added to the spec
            j[_JOB_REQUIREMENTS] = jrr.resolve_requirements(j.get(_METHOD))

    def _check_job_arguments(self, jobs, has_parent_job=False):
        # perform sanity checks before creating any jobs, including the parent job for batch jobs
        for i, job in enumerate(jobs):
            # Could make an argument checker method, or a class that doesn't require a job id.
            # Seems like more code & work for no real benefit though.
            # Just create the class for checks, don't use yet
            JobSubmissionParameters(
                "fakejobid",
                AppInfo(job.get(_METHOD), job.get(_APP_ID)),
                job[_JOB_REQUIREMENTS],
                UserCreds(self.sdkmr.get_user_id(), self.sdkmr.get_token()),
                wsid=job.get(_WORKSPACE_ID),
                source_ws_objects=job.get(_SOURCE_WS_OBJECTS),
            )
            if has_parent_job and job.get(_PARENT_JOB_ID):
                pre = f"Job #{i + 1}: b" if len(jobs) > 1 else "B"
                raise IncorrectParamsException(
                    f"{pre}atch jobs may not specify a parent job ID"
                )
            # This is also an opportunity for caching
            # although most likely jobs aren't operating on the same object
            self._check_ws_objects(source_objects=job.get(_SOURCE_WS_OBJECTS))

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
            self._check_workspace_permissions(params.get(_WORKSPACE_ID))

        if concierge_params:
            self.sdkmr.check_as_concierge()
            # we don't check requirements type because the concierge can do what they like
            params[_JOB_REQUIREMENTS] = self._get_job_reqs_from_concierge_params(
                params.get(_METHOD), concierge_params
            )
        else:
            self._add_job_requirements([params])
        self._check_job_arguments([params])

        return self._run(params=params)

    def _get_job_reqs_from_concierge_params(
        self, method: str, concierge_params: Dict[str, Any]
    ) -> ResolvedRequirements:
        jrr = self.sdkmr.get_job_requirements_resolver()
        norm = jrr.normalize_job_reqs(concierge_params, "concierge parameters")
        rl = concierge_params.get(_REQUIREMENTS_LIST)
        schd_reqs = {}
        if rl:
            if type(rl) != list:
                raise IncorrectParamsException(f"{_REQUIREMENTS_LIST} must be a list")
            for s in rl:
                if type(s) != str or "=" not in s:
                    raise IncorrectParamsException(
                        f"Found illegal requirement in {_REQUIREMENTS_LIST}: {s}"
                    )
                key, val = s.split("=")
                schd_reqs[key.strip()] = val.strip()

        return jrr.resolve_requirements(
            method,
            cpus=norm.get(REQUEST_CPUS),
            memory_MB=norm.get(REQUEST_MEMORY),
            disk_GB=norm.get(REQUEST_DISK),
            client_group=norm.get(CLIENT_GROUP) or CONCIERGE_CLIENTGROUP,
            client_group_regex=norm.get(CLIENT_GROUP_REGEX),
            # error messaging here is for 'bill_to_user' vs 'account_group' but almost impossible
            # to screw up so YAGNI
            # Note that this is never confirmed to be a real user. May want to fix that, but
            # since it's admin only... YAGNI
            bill_to_user=concierge_params.get("account_group"),
            # default is to ignore concurrency limits for concierge
            ignore_concurrency_limits=bool(
                concierge_params.get("ignore_concurrency_limits", 1)
            ),
            scheduler_requirements=schd_reqs,
            debug_mode=norm.get(DEBUG_MODE),
        )

    def update_job_to_queued(self, job_id, scheduler_id):
        # TODO RETRY FOR RACE CONDITION OF RUN/CANCEL
        # TODO PASS QUEUE TIME IN FROM SCHEDULER ITSELF?
        # TODO PASS IN SCHEDULER TYPE?
        j = self.sdkmr.get_mongo_util().get_job(job_id=job_id)
        previous_status = j.status
        j.status = Status.queued.value
        j.queued = time.time()
        j.scheduler_id = scheduler_id
        j.scheduler_type = "condor"
        self.sdkmr.save_job(j)

        self.sdkmr.get_kafka_client().send_kafka_message(
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

        job_params[_METHOD] = job_input.method
        job_params["params"] = job_input.params
        job_params["service_ver"] = job_input.service_ver
        job_params[_APP_ID] = job_input.app_id
        job_params[_WORKSPACE_ID] = job_input.wsid
        job_params[_PARENT_JOB_ID] = job_input.parent_job_id
        job_params[_SOURCE_WS_OBJECTS] = job_input.source_ws_objects

        return job_params
