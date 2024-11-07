# -*- coding: utf-8 -*-
# BEGIN_HEADER
import os
import time

from cachetools import TTLCache

from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.APIHelpers import GenerateFromConfig
from execution_engine2.utils.clients import get_client_set

_AS_ADMIN = "as_admin"
# END_HEADER


class execution_engine2:
    '''
    Module Name:
    execution_engine2

    Module Description:
    
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa

    VERSION = "0.0.15"
    GIT_URL = "git@github.com:kbase/execution_engine2.git"
    GIT_COMMIT_HASH = "d82ec9b1c5b8fc0fc67e0ebc4b52f29d0c817835"

    # BEGIN_CLASS_HEADER
    MONGO_COLLECTION = "jobs"
    MONGO_AUTHMECHANISM = "DEFAULT"

    SERVICE_NAME = "KBase Execution Engine"

    JOB_PERMISSION_CACHE_SIZE = 500
    JOB_PERMISSION_CACHE_EXPIRE_TIME = 300  # seconds

    ADMIN_ROLES_CACHE_SIZE = 500
    ADMIN_ROLES_CACHE_EXPIRE_TIME = 300  # seconds
    # END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        # BEGIN_CONSTRUCTOR
        self.config = config
        self.config["mongo-collection"] = self.MONGO_COLLECTION
        self.config.setdefault("mongo-authmechanism", self.MONGO_AUTHMECHANISM)

        self.job_permission_cache = TTLCache(
            maxsize=self.JOB_PERMISSION_CACHE_SIZE,
            ttl=self.JOB_PERMISSION_CACHE_EXPIRE_TIME,
        )
        self.admin_permissions_cache = TTLCache(
            maxsize=self.ADMIN_ROLES_CACHE_SIZE, ttl=self.ADMIN_ROLES_CACHE_EXPIRE_TIME
        )
        self.gen_cfg = GenerateFromConfig(config)
        # move these into GFC? Since they're only generated once it doesn't seem necessary
        configpath = os.environ["KB_DEPLOYMENT_CONFIG"]
        override = os.environ.get("OVERRIDE_CLIENT_GROUP")
        with open(configpath) as cf:
            self.clients = get_client_set(config, cf, override)
        # END_CONSTRUCTOR
        pass


    def list_config(self, ctx):
        """
        Returns the service configuration, including URL endpoints and timeouts.
        The returned values are:
        external-url - string - url of this service
        kbase-endpoint - string - url of the services endpoint for the KBase environment
        workspace-url - string - Workspace service url
        catalog-url - string - catalog service url
        shock-url - string - shock service url
        handle-url - string - handle service url
        auth-service-url - string - legacy auth service url
        auth-service-url-v2 - string - current auth service url
        auth-service-url-allow-insecure - boolean string (true or false) - whether to allow insecure requests
        scratch - string - local path to scratch directory
        executable - string - name of Job Runner executable
        docker_timeout - int - time in seconds before a job will be timed out and terminated
        initial_dir - string - initial dir for HTCondor to search for passed input/output files
        transfer_input_files - initial list of files to transfer to HTCondor for job running
        :returns: instance of mapping from String to String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN list_config
        public_keys = [
            "external-url",
            "kbase-endpoint",
            "workspace-url",
            "catalog-url",
            "shock-url",
            "handle-url",
            "srv-wiz-url",
            "auth-service-url",
            "auth-service-url-v2",
            "auth-service-url-allow-insecure",
            "scratch",
            "executable",
            "docker_timeout",
            "initialdir",
            "ref_data_base"
        ]

        returnVal = {key: self.config.get(key) for key in public_keys}

        # END list_config

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method list_config ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def ver(self, ctx):
        """
        Returns the current running version of the execution_engine2 servicve as a semantic version string.
        :returns: instance of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN ver
        returnVal = self.VERSION
        # END ver

        # At some point might do deeper type checking...
        if not isinstance(returnVal, str):
            raise ValueError('Method ver ' +
                             'return value returnVal ' +
                             'is not type str as required.')
        # return the results
        return [returnVal]

    def status(self, ctx):
        """
        Simply check the status of this service to see queue details
        :returns: instance of type "Status" (A structure representing the
           Execution Engine status git_commit - the Git hash of the version
           of the module. version - the semantic version for the module.
           service - the name of the service. server_time - the current
           server timestamp since epoch # TODO - add some or all of the
           following reboot_mode - if 1, then in the process of rebooting
           stopping_mode - if 1, then in the process of stopping
           running_tasks_total - number of total running jobs
           running_tasks_per_user - mapping from user id to number of running
           jobs for that user tasks_in_queue - number of jobs in the queue
           that are not running) -> structure: parameter "git_commit" of
           String, parameter "version" of String, parameter "service" of
           String, parameter "server_time" of Double
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN status
        returnVal = {
            "server_time": time.time(),
            "git_commit": self.GIT_COMMIT_HASH,
            "version": self.VERSION,
            "service": self.SERVICE_NAME,
        }

        # END status

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method status ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def run_job(self, ctx, params):
        """
        Start a new job.
        :param params: instance of type "RunJobParams" (method - the SDK
           method to run in module.method format, e.g.
           'KBaseTrees.construct_species_tree' params - the parameters to
           pass to the method. Optional parameters: app_id - the id of the
           Narrative application (UI) running this job (e.g. repo/name)
           service_ver - specific version of deployed service, last version
           is used if this parameter is not defined source_ws_objects -
           denotes the workspace objects that will serve as a source of data
           when running the SDK method. These references will be added to the
           autogenerated provenance. Must be in UPA format (e.g. 6/90/4).
           meta - Narrative metadata to associate with the job. wsid - an
           optional workspace id to associate with the job. This is passed to
           the workspace service, which will share the job based on the
           permissions of the workspace rather than owner of the job
           parent_job_id - EE2 job id for the parent of the current job. For
           run_job and run_job_concierge, this value can be specified to
           denote the parent job of the job being created. Warning: No
           checking is done on the validity of the job ID, and the parent job
           record is not altered. Submitting a job with a parent ID to
           run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1])
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        # BEGIN run_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients = self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        job_id = mr.run_job(params, as_admin=bool(params.get(_AS_ADMIN)))
        # END run_job

        # At some point might do deeper type checking...
        if not isinstance(job_id, str):
            raise ValueError('Method run_job ' +
                             'return value job_id ' +
                             'is not type str as required.')
        # return the results
        return [job_id]

    def run_job_batch(self, ctx, params, batch_params):
        """
        Run a batch job, consisting of a parent job and one or more child jobs.
        Note that the as_admin parameters in the list of child jobs are ignored -
        only the as_admin parameter in the batch_params is considered.
        :param params: instance of list of type "RunJobParams" (method - the
           SDK method to run in module.method format, e.g.
           'KBaseTrees.construct_species_tree' params - the parameters to
           pass to the method. Optional parameters: app_id - the id of the
           Narrative application (UI) running this job (e.g. repo/name)
           service_ver - specific version of deployed service, last version
           is used if this parameter is not defined source_ws_objects -
           denotes the workspace objects that will serve as a source of data
           when running the SDK method. These references will be added to the
           autogenerated provenance. Must be in UPA format (e.g. 6/90/4).
           meta - Narrative metadata to associate with the job. wsid - an
           optional workspace id to associate with the job. This is passed to
           the workspace service, which will share the job based on the
           permissions of the workspace rather than owner of the job
           parent_job_id - EE2 job id for the parent of the current job. For
           run_job and run_job_concierge, this value can be specified to
           denote the parent job of the job being created. Warning: No
           checking is done on the validity of the job ID, and the parent job
           record is not altered. Submitting a job with a parent ID to
           run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1])
        :param batch_params: instance of type "BatchParams" (Additional
           parameters for a batch job. wsid: the workspace with which to
           associate the parent job. as_admin: run the job with full EE2
           permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights.) -> structure:
           parameter "wsid" of Long, parameter "as_admin" of type "boolean"
           (@range [0,1])
        :returns: instance of type "BatchSubmission" -> structure: parameter
           "batch_id" of type "job_id" (A job id.), parameter "child_job_ids"
           of list of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_ids
        # BEGIN run_job_batch
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients = self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache
        )
        job_ids = mr.run_job_batch(
            params, batch_params, as_admin=bool(batch_params.get(_AS_ADMIN)))
        # END run_job_batch

        # At some point might do deeper type checking...
        if not isinstance(job_ids, dict):
            raise ValueError('Method run_job_batch ' +
                             'return value job_ids ' +
                             'is not type dict as required.')
        # return the results
        return [job_ids]

    def retry_job(self, ctx, params):
        """
        #TODO write retry parent tests to ensure BOTH the parent_job_id is present, and retry_job_id is present
        #TODO Add retry child that checks the status of the child? to prevent multiple retries
        Allowed Jobs
         Regular Job with no children
         Regular job with/without parent_id that runs a kbparallel call or a run_job_batch call
        Not Allowed
         Regular Job with children (Should not be possible to create yet)
         Batch Job Parent Container (Not a job, it won't do anything, except cancel it's child jobs)
        :param params: instance of type "RetryParams" (job_id of job to retry
           as_admin: retry someone elses job in your namespace #TODO Possibly
           Add JobRequirements job_requirements;) -> structure: parameter
           "job_id" of type "job_id" (A job id.), parameter "as_admin" of
           type "boolean" (@range [0,1])
        :returns: instance of type "RetryResult" (job_id of retried job
           retry_id: job_id of the job that was launched str error: reason as
           to why that particular retry failed (available for bulk retry
           only)) -> structure: parameter "job_id" of type "job_id" (A job
           id.), parameter "retry_id" of type "job_id" (A job id.), parameter
           "error" of String
        """
        # ctx is the context object
        # return variables are: retry_result
        # BEGIN retry_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients = self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache
        )
        retry_result = mr.retry(job_id=params.get('job_id'), as_admin=params.get('as_admin'))
        # END retry_job

        # At some point might do deeper type checking...
        if not isinstance(retry_result, dict):
            raise ValueError('Method retry_job ' +
                             'return value retry_result ' +
                             'is not type dict as required.')
        # return the results
        return [retry_result]

    def retry_jobs(self, ctx, params):
        """
        Same as retry_job, but accepts multiple jobs
        :param params: instance of type "BulkRetryParams" (job_ids of job to
           retry as_admin: retry someone else's job in your namespace #TODO:
           Possibly Add list<JobRequirements> job_requirements;) ->
           structure: parameter "job_ids" of list of type "job_id" (A job
           id.), parameter "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of list of type "RetryResult" (job_id of retried
           job retry_id: job_id of the job that was launched str error:
           reason as to why that particular retry failed (available for bulk
           retry only)) -> structure: parameter "job_id" of type "job_id" (A
           job id.), parameter "retry_id" of type "job_id" (A job id.),
           parameter "error" of String
        """
        # ctx is the context object
        # return variables are: retry_result
        # BEGIN retry_jobs
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients = self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache
        )
        retry_result = mr.retry_multiple(job_ids=params.get('job_ids'), as_admin=params.get('as_admin'))
        # END retry_jobs

        # At some point might do deeper type checking...
        if not isinstance(retry_result, list):
            raise ValueError('Method retry_jobs ' +
                             'return value retry_result ' +
                             'is not type list as required.')
        # return the results
        return [retry_result]

    def abandon_children(self, ctx, params):
        """
        :param params: instance of type "AbandonChildren" -> structure:
           parameter "batch_id" of type "job_id" (A job id.), parameter
           "child_job_ids" of list of type "job_id" (A job id.), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "BatchSubmission" -> structure: parameter
           "batch_id" of type "job_id" (A job id.), parameter "child_job_ids"
           of list of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: parent_and_child_ids
        # BEGIN abandon_children
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        parent_and_child_ids = mr.abandon_children(batch_id=params['batch_id'],
                                                   child_job_ids=params['child_job_ids'],
                                                   as_admin=params.get('as_admin'))
        # END abandon_children

        # At some point might do deeper type checking...
        if not isinstance(parent_and_child_ids, dict):
            raise ValueError('Method abandon_children ' +
                             'return value parent_and_child_ids ' +
                             'is not type dict as required.')
        # return the results
        return [parent_and_child_ids]

    def run_job_concierge(self, ctx, params, concierge_params):
        """
        :param params: instance of type "RunJobParams" (method - the SDK
           method to run in module.method format, e.g.
           'KBaseTrees.construct_species_tree' params - the parameters to
           pass to the method. Optional parameters: app_id - the id of the
           Narrative application (UI) running this job (e.g. repo/name)
           service_ver - specific version of deployed service, last version
           is used if this parameter is not defined source_ws_objects -
           denotes the workspace objects that will serve as a source of data
           when running the SDK method. These references will be added to the
           autogenerated provenance. Must be in UPA format (e.g. 6/90/4).
           meta - Narrative metadata to associate with the job. wsid - an
           optional workspace id to associate with the job. This is passed to
           the workspace service, which will share the job based on the
           permissions of the workspace rather than owner of the job
           parent_job_id - EE2 job id for the parent of the current job. For
           run_job and run_job_concierge, this value can be specified to
           denote the parent job of the job being created. Warning: No
           checking is done on the validity of the job ID, and the parent job
           record is not altered. Submitting a job with a parent ID to
           run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1])
        :param concierge_params: instance of type "ConciergeParams"
           (EE2Constants Concierge Params are request_cpus: int
           request_memory: int in MB request_disk: int in GB job_priority:
           int = None  range from -20 to +20, with higher values meaning
           better priority. Note: job_priority is currently not implemented.
           account_group: str = None # Someone elses account
           ignore_concurrency_limits: ignore any limits on simultaneous job
           runs. Default 1 (True). requirements_list: list = None
           ['machine=worker102','color=red'] client_group: Optional[str] =
           CONCIERGE_CLIENTGROUP # You can leave default or specify a
           clientgroup client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. debug_mode: Whether to run the job in debug mode. Default
           0 (False).) -> structure: parameter "request_cpu" of Long,
           parameter "request_memory" of Long, parameter "request_disk" of
           Long, parameter "job_priority" of Long, parameter "account_group"
           of String, parameter "ignore_concurrency_limits" of type "boolean"
           (@range [0,1]), parameter "requirements_list" of list of String,
           parameter "client_group" of String, parameter "client_group_regex"
           of type "boolean" (@range [0,1]), parameter "debug_mode" of type
           "boolean" (@range [0,1])
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        # BEGIN run_job_concierge
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        job_id = mr.run_job_concierge(params=params,concierge_params=concierge_params)
        # END run_job_concierge

        # At some point might do deeper type checking...
        if not isinstance(job_id, str):
            raise ValueError('Method run_job_concierge ' +
                             'return value job_id ' +
                             'is not type str as required.')
        # return the results
        return [job_id]

    def get_job_params(self, ctx, params):
        """
        :param params: instance of type "GetJobParams" (Get job params
           necessary for job execution @optional as_admin) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "RunJobParams" (method - the SDK method to
           run in module.method format, e.g.
           'KBaseTrees.construct_species_tree' params - the parameters to
           pass to the method. Optional parameters: app_id - the id of the
           Narrative application (UI) running this job (e.g. repo/name)
           service_ver - specific version of deployed service, last version
           is used if this parameter is not defined source_ws_objects -
           denotes the workspace objects that will serve as a source of data
           when running the SDK method. These references will be added to the
           autogenerated provenance. Must be in UPA format (e.g. 6/90/4).
           meta - Narrative metadata to associate with the job. wsid - an
           optional workspace id to associate with the job. This is passed to
           the workspace service, which will share the job based on the
           permissions of the workspace rather than owner of the job
           parent_job_id - EE2 job id for the parent of the current job. For
           run_job and run_job_concierge, this value can be specified to
           denote the parent job of the job being created. Warning: No
           checking is done on the validity of the job ID, and the parent job
           record is not altered. Submitting a job with a parent ID to
           run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1])
        """
        # ctx is the context object
        # return variables are: params
        # BEGIN get_job_params
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        params = mr.get_job_params(job_id=params['job_id'], as_admin=params.get('as_admin'))
        # END get_job_params

        # At some point might do deeper type checking...
        if not isinstance(params, dict):
            raise ValueError('Method get_job_params ' +
                             'return value params ' +
                             'is not type dict as required.')
        # return the results
        return [params]

    def update_job_status(self, ctx, params):
        """
        :param params: instance of type "UpdateJobStatusParams" (job_id - a
           job id status - the new status to set for the job.) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "status" of String, parameter "as_admin" of type "boolean" (@range
           [0,1])
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        # BEGIN update_job_status
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,

        )
        job_id = mr.update_job_status(job_id=params['job_id'], status=params['status'],
                                      as_admin=params.get('as_admin'))
        # END update_job_status

        # At some point might do deeper type checking...
        if not isinstance(job_id, str):
            raise ValueError('Method update_job_status ' +
                             'return value job_id ' +
                             'is not type str as required.')
        # return the results
        return [job_id]

    def add_job_logs(self, ctx, params, lines):
        """
        :param params: instance of type "AddJobLogsParams" -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :param lines: instance of list of type "LogLine" (line - string - a
           string to set for the log line. is_error - int - if 1, then this
           line should be treated as an error, default 0 ts - int - a
           timestamp since epoch in milliseconds for the log line (optional)
           @optional ts) -> structure: parameter "line" of String, parameter
           "is_error" of type "boolean" (@range [0,1]), parameter "ts" of Long
        :returns: instance of type "AddJobLogsResults" (@success Whether or
           not the add operation was successful @line_number the line number
           of the last added log) -> structure: parameter "success" of type
           "boolean" (@range [0,1]), parameter "line_number" of Long
        """
        # ctx is the context object
        # return variables are: results
        # BEGIN add_job_logs
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        add_job_logs = mr.add_job_logs(job_id=params['job_id'], log_lines=lines,
                                       as_admin=params.get('as_admin'))

        results = {'success': add_job_logs.success,
                   'line_number': add_job_logs.stored_line_count}

        # END add_job_logs

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method add_job_logs ' +
                             'return value results ' +
                             'is not type dict as required.')
        # return the results
        return [results]

    def get_job_logs(self, ctx, params):
        """
        :param params: instance of type "GetJobLogsParams" (job id - the job
           id optional skip_lines Legacy Parameter for Offset optional offset
           Number of lines to skip (in case they were already loaded before).
           optional limit  optional parameter, maximum number of lines
           returned optional as_admin  request read access to record normally
           not allowed..) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "skip_lines" of Long, parameter "offset" of
           Long, parameter "limit" of Long, parameter "as_admin" of type
           "boolean" (@range [0,1])
        :returns: instance of type "GetJobLogsResults" (last_line_number -
           common number of lines (including those in skip_lines parameter),
           this number can be used as next skip_lines value to skip already
           loaded lines next time.) -> structure: parameter "lines" of list
           of type "LogLine" (line - string - a string to set for the log
           line. is_error - int - if 1, then this line should be treated as
           an error, default 0 ts - int - a timestamp since epoch in
           milliseconds for the log line (optional) @optional ts) ->
           structure: parameter "line" of String, parameter "is_error" of
           type "boolean" (@range [0,1]), parameter "ts" of Long, parameter
           "last_line_number" of Long, parameter "count" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN get_job_logs
        if params.get("skip_lines") and params.get("offset"):
            raise ValueError("Please provide only one of skip_lines or offset")

        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        returnVal = mr.view_job_logs(
            job_id=params["job_id"],
            skip_lines=params.get("skip_lines", params.get("offset", None)),
            limit=params.get("limit", None),
            as_admin=params.get('as_admin')
        )
        # END get_job_logs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method get_job_logs ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def finish_job(self, ctx, params):
        """
        Register results of already started job
        :param params: instance of type "FinishJobParams" (job_id - string -
           the id of the job to mark completed or finished with an error
           error_message - string - optional unless job is finished with an
           error error_code - int - optional unless job finished with an
           error error - JsonRpcError - optional output from SDK Job
           Containers job_output - job output if job completed successfully)
           -> structure: parameter "job_id" of type "job_id" (A job id.),
           parameter "error_message" of String, parameter "error_code" of
           Long, parameter "error" of type "JsonRpcError" (Error block of
           JSON RPC response) -> structure: parameter "name" of String,
           parameter "code" of Long, parameter "message" of String, parameter
           "error" of String, parameter "job_output" of unspecified object,
           parameter "as_admin" of type "boolean" (@range [0,1])
        """
        # ctx is the context object
        # BEGIN finish_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        mr.finish_job(
            job_id=params["job_id"],
            error_message=params.get("error_message"),
            error_code=params.get("error_code"),
            error=params.get("error"),
            job_output=params.get("job_output"),
            as_admin=params.get('as_admin')
        )

        # END finish_job
        pass

    def start_job(self, ctx, params):
        """
        :param params: instance of type "StartJobParams" (skip_estimation:
           default true. If set true, job will set to running status skipping
           estimation step) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "skip_estimation" of type "boolean" (@range
           [0,1]), parameter "as_admin" of type "boolean" (@range [0,1])
        """
        # ctx is the context object
        # BEGIN start_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        mr.start_job(
            params["job_id"], skip_estimation=params.get("skip_estimation", True),
            as_admin=params.get('as_admin')
        )
        # END start_job
        pass

    def check_job(self, ctx, params):
        """
        get current status of a job
        :param params: instance of type "CheckJobParams" (exclude_fields:
           exclude certain fields to return. default None. exclude_fields
           strings can be one of fields defined in
           execution_engine2.db.models.models.Job) -> structure: parameter
           "job_id" of type "job_id" (A job id.), parameter "exclude_fields"
           of list of String, parameter "as_admin" of type "boolean" (@range
           [0,1])
        :returns: instance of type "JobState" (job_id - string - id of the
           job user - string - user who started the job wsid - int - optional
           id of the workspace where the job is bound authstrat - string -
           what strategy used to authenticate the job job_input - object -
           inputs to the job (from the run_job call)  ## TODO - verify
           job_output - object - outputs from the job (from the run_job call)
           ## TODO - verify updated - int - timestamp since epoch in
           milliseconds of the last time the status was updated running - int
           - timestamp since epoch in milliseconds of when it entered the
           running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String
        """
        # ctx is the context object
        # return variables are: job_state
        # BEGIN check_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        job_state = mr.check_job(
            params["job_id"], exclude_fields=params.get("exclude_fields", None),
            as_admin=params.get('as_admin')
        )
        # END check_job

        # At some point might do deeper type checking...
        if not isinstance(job_state, dict):
            raise ValueError('Method check_job ' +
                             'return value job_state ' +
                             'is not type dict as required.')
        # return the results
        return [job_state]

    def check_job_batch(self, ctx, params):
        """
        get current status of a parent job, and it's children, if it has any.
        :param params: instance of type "CheckJobParams" (exclude_fields:
           exclude certain fields to return. default None. exclude_fields
           strings can be one of fields defined in
           execution_engine2.db.models.models.Job) -> structure: parameter
           "job_id" of type "job_id" (A job id.), parameter "exclude_fields"
           of list of String, parameter "as_admin" of type "boolean" (@range
           [0,1])
        :returns: instance of type "CheckJobBatchResults" (batch_jobstate -
           state of the coordinating job for the batch child_jobstates -
           states of child jobs IDEA: ADD aggregate_states - count of all
           available child job states, even if they are zero) -> structure:
           parameter "batch_jobstate" of type "JobState" (job_id - string -
           id of the job user - string - user who started the job wsid - int
           - optional id of the workspace where the job is bound authstrat -
           string - what strategy used to authenticate the job job_input -
           object - inputs to the job (from the run_job call)  ## TODO -
           verify job_output - object - outputs from the job (from the
           run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String, parameter "child_jobstates" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - optional id of the workspace where the job is
           bound authstrat - string - what strategy used to authenticate the
           job job_input - object - inputs to the job (from the run_job call)
           ## TODO - verify job_output - object - outputs from the job (from
           the run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN check_job_batch
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_job_batch(
            batch_id=params["job_id"], exclude_fields=params.get("exclude_fields", None),
            as_admin=params.get('as_admin')
        )
        # END check_job_batch

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_job_batch ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def check_jobs(self, ctx, params):
        """
        :param params: instance of type "CheckJobsParams" (As in check_job,
           exclude_fields strings can be used to exclude fields. see
           CheckJobParams for allowed strings. return_list - optional, return
           list of job state if set to 1. Otherwise return a dict. Default
           1.) -> structure: parameter "job_ids" of list of type "job_id" (A
           job id.), parameter "exclude_fields" of list of String, parameter
           "return_list" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - optional id of the workspace where the job is
           bound authstrat - string - what strategy used to authenticate the
           job job_input - object - inputs to the job (from the run_job call)
           ## TODO - verify job_output - object - outputs from the job (from
           the run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN check_jobs
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_jobs(
            params.get("job_ids"),
            exclude_fields=params.get("exclude_fields", None),
            return_list=params.get("return_list", 1),
            as_admin=params.get('as_admin')
        )
        # END check_jobs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def check_workspace_jobs(self, ctx, params):
        """
        :param params: instance of type "CheckWorkspaceJobsParams" (Check
           status of all jobs in a given workspace. Only checks jobs that
           have been associated with a workspace at their creation.
           return_list - optional, return list of job state if set to 1.
           Otherwise return a dict. Default 0.) -> structure: parameter
           "workspace_id" of String, parameter "exclude_fields" of list of
           String, parameter "return_list" of type "boolean" (@range [0,1]),
           parameter "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - optional id of the workspace where the job is
           bound authstrat - string - what strategy used to authenticate the
           job job_input - object - inputs to the job (from the run_job call)
           ## TODO - verify job_output - object - outputs from the job (from
           the run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN check_workspace_jobs
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_workspace_jobs(
            params.get("workspace_id"),
            exclude_fields=params.get("exclude_fields", None),
            return_list=params.get("return_list", 1),
            as_admin=params.get('as_admin')
        )
        # END check_workspace_jobs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_workspace_jobs ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def cancel_job(self, ctx, params):
        """
        Cancels a job. This results in the status becoming "terminated" with termination_code 0.
        :param params: instance of type "CancelJobParams" (cancel_and_sigterm
           "" Reasons for why the job was cancelled Current Default is
           `terminated_by_user 0` so as to not update narrative client
           terminated_by_user = 0 terminated_by_admin = 1
           terminated_by_automation = 2 "" job_id job_id @optional
           terminated_code) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "terminated_code" of Long, parameter
           "as_admin" of type "boolean" (@range [0,1])
        """
        # ctx is the context object
        # BEGIN cancel_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )

        mr.cancel_job(
            job_id=params["job_id"], terminated_code=params.get("terminated_code"),
            as_admin=params.get('as_admin')
        )
        # END cancel_job
        pass

    def check_job_canceled(self, ctx, params):
        """
        Check whether a job has been canceled. This method is lightweight compared to check_job.
        :param params: instance of type "CancelJobParams" (cancel_and_sigterm
           "" Reasons for why the job was cancelled Current Default is
           `terminated_by_user 0` so as to not update narrative client
           terminated_by_user = 0 terminated_by_admin = 1
           terminated_by_automation = 2 "" job_id job_id @optional
           terminated_code) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "terminated_code" of Long, parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobCanceledResult" (job_id - id of
           job running method finished - indicates whether job is done
           (including error/cancel cases) or not canceled - whether the job
           is canceled or not. ujs_url - url of UserAndJobState service used
           by job service) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "finished" of type "boolean" (@range
           [0,1]), parameter "canceled" of type "boolean" (@range [0,1]),
           parameter "ujs_url" of String, parameter "as_admin" of type
           "boolean" (@range [0,1])
        """
        # ctx is the context object
        # return variables are: result
        # BEGIN check_job_canceled
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        result = mr.check_job_canceled(job_id=params["job_id"], as_admin=params.get('as_admin'))
        # END check_job_canceled

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method check_job_canceled ' +
                             'return value result ' +
                             'is not type dict as required.')
        # return the results
        return [result]

    def get_job_status(self, ctx, params):
        """
        Just returns the status string for a job of a given id.
        :param params: instance of type "GetJobStatusParams" -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "GetJobStatusResult" -> structure:
           parameter "status" of String
        """
        # ctx is the context object
        # return variables are: result
        # BEGIN get_job_status
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
            job_permission_cache=self.job_permission_cache,
            admin_permissions_cache=self.admin_permissions_cache,
        )
        result = mr.get_job_status_field(job_id=params['job_id'],       as_admin=params.get('as_admin'))
        # END get_job_status

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method get_job_status ' +
                             'return value result ' +
                             'is not type dict as required.')
        # return the results
        return [result]

    def check_jobs_date_range_for_user(self, ctx, params):
        """
        :param params: instance of type "CheckJobsDateRangeParams" (Check job
           for all jobs in a given date/time range for all users (Admin
           function) Notes on start_time and end_time: These fields are
           designated as floats but floats, ints, and strings are all
           accepted. Times are determined as follows: - if the field is a
           float or a string that contains a float and only a float, the
           field value is treated as seconds since the epoch. - if the field
           is an int or a string that contains an int and only an int, the
           field value is treated as milliseconds since the epoch. - if the
           field is a string not matching the criteria above, it is treated
           as a date and time. Nearly any unambigous format can be parsed.
           float start_time - Filter based on job creation timestamp since
           epoch float end_time - Filter based on job creation timestamp
           since epoch list<string> projection - A list of fields to include
           in the projection, default ALL See "Projection Fields" above
           list<string> filter - DEPRECATED: this field may change or be
           removed in the future. A list of simple filters to "AND" together,
           such as error_code=1, wsid=1234, terminated_code = 1 int limit -
           The maximum number of records to return string user - The user
           whose job records will be returned. Optional. Default is the
           current user. int offset - the number of jobs to skip before
           returning records. boolean ascending - true to sort by job ID
           ascending, false descending. boolean as_admin - true to run the
           query as an admin; user must have admin EE2 permissions. Required
           if setting `user` to something other than your own. TODO: this
           seems to have no effect @optional projection @optional filter
           @optional limit @optional user @optional offset @optional
           ascending) -> structure: parameter "start_time" of Double,
           parameter "end_time" of Double, parameter "projection" of list of
           String, parameter "filter" of list of String, parameter "limit" of
           Long, parameter "user" of String, parameter "offset" of Long,
           parameter "ascending" of type "boolean" (@range [0,1]), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsDateRangeResults" (Projection
           Fields user = StringField(required=True) authstrat = StringField(
           required=True, default="kbaseworkspace",
           validation=valid_authstrat ) wsid = IntField(required=False)
           status = StringField(required=True, validation=valid_status)
           updated = DateTimeField(default=datetime.datetime.utcnow,
           autonow=True) estimating = DateTimeField(default=None)  # Time
           when job began estimating running = DateTimeField(default=None)  #
           Time when job started # Time when job finished, errored out, or
           was terminated by the user/admin finished =
           DateTimeField(default=None) errormsg = StringField() msg =
           StringField() error = DynamicField() terminated_code =
           IntField(validation=valid_termination_code) error_code =
           IntField(validation=valid_errorcode) scheduler_type =
           StringField() scheduler_id = StringField() scheduler_estimator_id
           = StringField() job_input = EmbeddedDocumentField(JobInput,
           required=True) job_output = DynamicField() /* /* Results of
           check_jobs_date_range methods. jobs - the jobs matching the query,
           up to `limit` jobs. count - the number of jobs returned.
           query_count - the number of jobs that matched the filters. filter
           - DEPRECATED - this field may change in the future. The filters
           that were applied to the jobs. skip - the number of jobs that were
           skipped prior to beginning to return jobs. projection - the list
           of fields included in the returned job. By default all fields.
           limit - the maximum number of jobs returned. sort_order - the
           order in which the results were sorted by the job ID - + for
           ascending, - for descending. TODO: DOCUMENT THE RETURN OF STATS
           mapping) -> structure: parameter "jobs" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - optional id of the workspace where the job is
           bound authstrat - string - what strategy used to authenticate the
           job job_input - object - inputs to the job (from the run_job call)
           ## TODO - verify job_output - object - outputs from the job (from
           the run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String, parameter "count" of Long, parameter "query_count" of
           Long, parameter "filter" of mapping from String to String,
           parameter "skip" of Long, parameter "projection" of list of
           String, parameter "limit" of Long, parameter "sort_order" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN check_jobs_date_range_for_user
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_jobs_date_range_for_user(
            creation_start_time=params.get("start_time"),
            creation_end_time=params.get("end_time"),
            job_projection=params.get("projection"),
            job_filter=params.get("filter"),
            limit=params.get("limit"),
            user=params.get("user"),
            offset=params.get("offset"),
            ascending=params.get("ascending"),
            as_admin=params.get('as_admin')
        )
        # END check_jobs_date_range_for_user

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs_date_range_for_user ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def check_jobs_date_range_for_all(self, ctx, params):
        """
        :param params: instance of type "CheckJobsDateRangeParams" (Check job
           for all jobs in a given date/time range for all users (Admin
           function) Notes on start_time and end_time: These fields are
           designated as floats but floats, ints, and strings are all
           accepted. Times are determined as follows: - if the field is a
           float or a string that contains a float and only a float, the
           field value is treated as seconds since the epoch. - if the field
           is an int or a string that contains an int and only an int, the
           field value is treated as milliseconds since the epoch. - if the
           field is a string not matching the criteria above, it is treated
           as a date and time. Nearly any unambigous format can be parsed.
           float start_time - Filter based on job creation timestamp since
           epoch float end_time - Filter based on job creation timestamp
           since epoch list<string> projection - A list of fields to include
           in the projection, default ALL See "Projection Fields" above
           list<string> filter - DEPRECATED: this field may change or be
           removed in the future. A list of simple filters to "AND" together,
           such as error_code=1, wsid=1234, terminated_code = 1 int limit -
           The maximum number of records to return string user - The user
           whose job records will be returned. Optional. Default is the
           current user. int offset - the number of jobs to skip before
           returning records. boolean ascending - true to sort by job ID
           ascending, false descending. boolean as_admin - true to run the
           query as an admin; user must have admin EE2 permissions. Required
           if setting `user` to something other than your own. TODO: this
           seems to have no effect @optional projection @optional filter
           @optional limit @optional user @optional offset @optional
           ascending) -> structure: parameter "start_time" of Double,
           parameter "end_time" of Double, parameter "projection" of list of
           String, parameter "filter" of list of String, parameter "limit" of
           Long, parameter "user" of String, parameter "offset" of Long,
           parameter "ascending" of type "boolean" (@range [0,1]), parameter
           "as_admin" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsDateRangeResults" (Projection
           Fields user = StringField(required=True) authstrat = StringField(
           required=True, default="kbaseworkspace",
           validation=valid_authstrat ) wsid = IntField(required=False)
           status = StringField(required=True, validation=valid_status)
           updated = DateTimeField(default=datetime.datetime.utcnow,
           autonow=True) estimating = DateTimeField(default=None)  # Time
           when job began estimating running = DateTimeField(default=None)  #
           Time when job started # Time when job finished, errored out, or
           was terminated by the user/admin finished =
           DateTimeField(default=None) errormsg = StringField() msg =
           StringField() error = DynamicField() terminated_code =
           IntField(validation=valid_termination_code) error_code =
           IntField(validation=valid_errorcode) scheduler_type =
           StringField() scheduler_id = StringField() scheduler_estimator_id
           = StringField() job_input = EmbeddedDocumentField(JobInput,
           required=True) job_output = DynamicField() /* /* Results of
           check_jobs_date_range methods. jobs - the jobs matching the query,
           up to `limit` jobs. count - the number of jobs returned.
           query_count - the number of jobs that matched the filters. filter
           - DEPRECATED - this field may change in the future. The filters
           that were applied to the jobs. skip - the number of jobs that were
           skipped prior to beginning to return jobs. projection - the list
           of fields included in the returned job. By default all fields.
           limit - the maximum number of jobs returned. sort_order - the
           order in which the results were sorted by the job ID - + for
           ascending, - for descending. TODO: DOCUMENT THE RETURN OF STATS
           mapping) -> structure: parameter "jobs" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - optional id of the workspace where the job is
           bound authstrat - string - what strategy used to authenticate the
           job job_input - object - inputs to the job (from the run_job call)
           ## TODO - verify job_output - object - outputs from the job (from
           the run_job call) ## TODO - verify updated - int - timestamp since
           epoch in milliseconds of the last time the status was updated
           running - int - timestamp since epoch in milliseconds of when it
           entered the running state created - int - timestamp since epoch in
           milliseconds when the job was created finished - int - timestamp
           since epoch in milliseconds when the job was finished status -
           string - status of the job. one of the following: created - job
           has been created in the service estimating - an estimation job is
           running to estimate resources required for the main job, and which
           queue should be used queued - job is queued to be run running -
           job is running on a worker node completed - job was completed
           successfully error - job is no longer running, but failed with an
           error terminated - job is no longer running, terminated either due
           to user cancellation, admin cancellation, or some automated task
           error_code - int - internal reason why the job is an error. one of
           the following: 0 - unknown 1 - job crashed 2 - job terminated by
           automation 3 - job ran over time limit 4 - job was missing its
           automated output document 5 - job authentication token expired
           errormsg - string - message (e.g. stacktrace) accompanying an
           errored job error - object - the JSON-RPC error package that
           accompanies the error code and message #TODO, add these to the
           structure? condor_job_ads - dict - condor related job information
           retry_count - int - generated field based on length of retry_ids
           retry_ids - list - list of jobs that are retried based off of this
           job retry_parent - str - job_id of the parent this retry is based
           off of. Not available on a retry_parent itself batch_id - str -
           the coordinating job, if the job is a child job created via
           run_job_batch batch_job - bool - whether or not this is a batch
           parent container child_jobs - array - Only parent container should
           have child job ids scheduler_type - str - scheduler, such as awe
           or condor scheduler_id - str - scheduler generated id
           scheduler_estimator_id - str - id for the job spawned for
           estimation terminated_code - int - internal reason why a job was
           terminated, one of: 0 - user cancellation 1 - admin cancellation 2
           - terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - the SDK method to run in module.method
           format, e.g. 'KBaseTrees.construct_species_tree' params - the
           parameters to pass to the method. Optional parameters: app_id -
           the id of the Narrative application (UI) running this job (e.g.
           repo/name) service_ver - specific version of deployed service,
           last version is used if this parameter is not defined
           source_ws_objects - denotes the workspace objects that will serve
           as a source of data when running the SDK method. These references
           will be added to the autogenerated provenance. Must be in UPA
           format (e.g. 6/90/4). meta - Narrative metadata to associate with
           the job. wsid - an optional workspace id to associate with the
           job. This is passed to the workspace service, which will share the
           job based on the permissions of the workspace rather than owner of
           the job parent_job_id - EE2 job id for the parent of the current
           job. For run_job and run_job_concierge, this value can be
           specified to denote the parent job of the job being created.
           Warning: No checking is done on the validity of the job ID, and
           the parent job record is not altered. Submitting a job with a
           parent ID to run_job_batch will cause an error to be returned.
           job_requirements: the requirements for the job. The user must have
           full EE2 administration rights to use this parameter. Note that
           the job_requirements are not returned along with the rest of the
           job parameters when querying the EE2 API - they are only
           considered when submitting a job. as_admin: run the job with full
           EE2 permissions, meaning that any supplied workspace IDs are not
           checked for accessibility and job_requirements may be supplied.
           The user must have full EE2 administration rights. Note that this
           field is not included in returned data when querying EE2.) ->
           structure: parameter "method" of String, parameter "app_id" of
           String, parameter "params" of list of unspecified object,
           parameter "service_ver" of String, parameter "source_ws_objects"
           of list of type "wsref" (A workspace object reference of the form
           X/Y/Z, where X is the workspace id, Y is the object id, Z is the
           version.), parameter "meta" of type "Meta" (Narrative metadata for
           a job. All fields are optional. run_id - the Narrative-assigned ID
           of the job run. 1:1 with a job ID. token_id - the ID of the token
           used to run the method. tag - the release tag, e.g.
           dev/beta/release. cell_id - the ID of the narrative cell from
           which the job was run.) -> structure: parameter "run_id" of
           String, parameter "token_id" of String, parameter "tag" of String,
           parameter "cell_id" of String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "job_requirements" of type
           "JobRequirements" (Job requirements for a job. All fields are
           optional. To submit job requirements, the user must have full EE2
           admin permissions. Ignored for the run concierge endpoint.
           request_cpus: the number of CPUs to request for the job.
           request_memory: the amount of memory, in MB, to request for the
           job. request_disk: the amount of disk space, in GB, to request for
           the job. client_group: the name of the client group on which to
           run the job. client_group_regex: Whether to treat the client group
           string, whether provided here, from the catalog, or as a default,
           as a regular expression when matching clientgroups. Default True
           for HTC, but the default depends on the scheduler. Omit to use the
           default. bill_to_user: the job will be counted against the
           provided user's fair share quota. ignore_concurrency_limits:
           ignore any limits on simultaneous job runs. Default false.
           scheduler_requirements: arbitrary key-value pairs to be provided
           to the job scheduler. Requires knowledge of the scheduler
           interface. debug_mode: Whether to run the job in debug mode.
           Default false.) -> structure: parameter "request_cpus" of Long,
           parameter "requst_memory" of Long, parameter "request_disk" of
           Long, parameter "client_group" of String, parameter
           "client_group_regex" of type "boolean" (@range [0,1]), parameter
           "bill_to_user" of String, parameter "ignore_concurrency_limits" of
           type "boolean" (@range [0,1]), parameter "scheduler_requirements"
           of mapping from String to String, parameter "debug_mode" of type
           "boolean" (@range [0,1]), parameter "as_admin" of type "boolean"
           (@range [0,1]), parameter "created" of Long, parameter "queued" of
           Long, parameter "estimating" of Long, parameter "running" of Long,
           parameter "finished" of Long, parameter "updated" of Long,
           parameter "error" of type "JsonRpcError" (Error block of JSON RPC
           response) -> structure: parameter "name" of String, parameter
           "code" of Long, parameter "message" of String, parameter "error"
           of String, parameter "error_code" of Long, parameter "errormsg" of
           String, parameter "terminated_code" of Long, parameter "batch_id"
           of String, parameter "count" of Long, parameter "query_count" of
           Long, parameter "filter" of mapping from String to String,
           parameter "skip" of Long, parameter "projection" of list of
           String, parameter "limit" of Long, parameter "sort_order" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN check_jobs_date_range_for_all
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_jobs_date_range_for_user(
            creation_start_time=params.get("start_time"),
            creation_end_time=params.get("end_time"),
            job_projection=params.get("projection"),
            job_filter=params.get("filter"),
            limit=params.get("limit"),
            offset=params.get("offset"),
            ascending=params.get("ascending"),
            as_admin=params.get('as_admin'),
            user="ALL",
        )
        # END check_jobs_date_range_for_all

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs_date_range_for_all ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def handle_held_job(self, ctx, cluster_id):
        """
        Handle a held CONDOR job. You probably never want to run this, only the reaper should run it.
        :param cluster_id: instance of String
        :returns: instance of type "HeldJob" -> structure: parameter
           "held_job" of unspecified object
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN handle_held_job
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.handle_held_job(cluster_id=cluster_id)
        # END handle_held_job

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method handle_held_job ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def is_admin(self, ctx):
        """
        Check if current user has ee2 admin rights.
        :returns: instance of type "boolean" (@range [0,1])
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN is_admin
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.check_is_admin()
        # END is_admin

        # At some point might do deeper type checking...
        if not isinstance(returnVal, int):
            raise ValueError('Method is_admin ' +
                             'return value returnVal ' +
                             'is not type int as required.')
        # return the results
        return [returnVal]

    def get_admin_permission(self, ctx):
        """
        Check if current user has ee2 admin rights.
        If so, return the type of rights and their roles
        :returns: instance of type "AdminRolesResults" (str permission - One
           of 'r|w|x' (('read' | 'write' | 'none'))) -> structure: parameter
           "permission" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        # BEGIN get_admin_permission
        mr = SDKMethodRunner(
            user_clients=self.gen_cfg.get_user_clients(ctx),
            clients=self.clients,
        )
        returnVal = mr.get_admin_permission()
        # END get_admin_permission

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method get_admin_permission ' +
                             'return value returnVal ' +
                             'is not type dict as required.')
        # return the results
        return [returnVal]

    def get_client_groups(self, ctx):
        """
        Get a list of clientgroups manually extracted from the config file
        :returns: instance of list of String
        """
        # ctx is the context object
        # return variables are: client_groups
        # BEGIN get_client_groups
        # TODO I think this needs to be actually extracted from the config file
        client_groups = ['njs', 'bigmem', 'bigmemlong', 'extreme', 'concierge', 'hpc', 'kb_upload',
                         'terabyte', 'multi_tb', 'kb_upload_bulk']
        # END get_client_groups

        # At some point might do deeper type checking...
        if not isinstance(client_groups, list):
            raise ValueError('Method get_client_groups ' +
                             'return value client_groups ' +
                             'is not type list as required.')
        # return the results
        return [client_groups]
