# -*- coding: utf-8 -*-
#BEGIN_HEADER
from execution_engine2.SDKMethodRunner import SDKMethodRunner
import time
from cachetools import TTLCache
#END_HEADER


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
    VERSION = "0.0.1"
    GIT_URL = "https://bio-boris@github.com/kbase/execution_engine2"
    GIT_COMMIT_HASH = "b565e76aa81a3f9e42e55f52dd12a701c44af377"

    #BEGIN_CLASS_HEADER
    MONGO_COLLECTION = "jobs"
    MONGO_AUTHMECHANISM = "DEFAULT"

    SERVICE_NAME = "KBase Execution Engine"

    JOB_PERMISSION_CACHE_SIZE = 500
    JOB_PERMISSION_CACHE_EXPIRE_TIME = 300  # seconds

    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.config = config
        self.config["mongo-collection"] = self.MONGO_COLLECTION
        self.config.setdefault("mongo-authmechanism", self.MONGO_AUTHMECHANISM)
        self.job_permission_cache = TTLCache(maxsize=self.JOB_PERMISSION_CACHE_SIZE,
                                             ttl=self.JOB_PERMISSION_CACHE_EXPIRE_TIME)
        #END_CONSTRUCTOR
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
        #BEGIN list_config
        public_keys = ['external-url', 'kbase-endpoint', 'workspace-url', 'catalog-url',
                       'shock-url', 'handle-url', 'auth-service-url', 'auth-service-url-v2',
                       'auth-service-url-allow-insecure',
                       'scratch', 'executable', 'docker_timeout',
                       'initialdir', 'transfer_input_files']

        returnVal = {key: self.config.get(key) for key in public_keys}

        #END list_config

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method list_config return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def ver(self, ctx):
        """
        Returns the current running version of the execution_engine2 servicve as a semantic version string.
        :returns: instance of String
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN ver
        returnVal = self.VERSION
        #END ver

        # At some point might do deeper type checking...
        if not isinstance(returnVal, str):
            raise ValueError('Method ver return value ' +
                             'returnVal is not type str as required.')
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
        #BEGIN status
        returnVal = {
            "server_time": time.time(),
            "git_commit": self.GIT_COMMIT_HASH,
            "version": self.VERSION,
            "service": self.SERVICE_NAME
        }

        #END status

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method status return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def run_job(self, ctx, params):
        """
        Start a new job (long running method of service registered in ServiceRegistery).
        Such job runs Docker image for this service in script mode.
        :param params: instance of type "RunJobParams" (method - service
           defined in standard JSON RPC way, typically it's module name from
           spec-file followed by '.' and name of funcdef from spec-file
           corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        #BEGIN run_job
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        job_id = mr.run_job(params)
        #END run_job

        # At some point might do deeper type checking...
        if not isinstance(job_id, str):
            raise ValueError('Method run_job return value ' +
                             'job_id is not type str as required.')
        # return the results
        return [job_id]

    def get_job_params(self, ctx, job_id):
        """
        Get job params necessary for job execution
        :param job_id: instance of type "job_id" (A job id.)
        :returns: instance of type "RunJobParams" (method - service defined
           in standard JSON RPC way, typically it's module name from
           spec-file followed by '.' and name of funcdef from spec-file
           corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String
        """
        # ctx is the context object
        # return variables are: params
        #BEGIN get_job_params
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        params = mr.get_job_params(job_id)
        #END get_job_params

        # At some point might do deeper type checking...
        if not isinstance(params, dict):
            raise ValueError('Method get_job_params return value ' +
                             'params is not type dict as required.')
        # return the results
        return [params]

    def update_job_status(self, ctx, params):
        """
        :param params: instance of type "UpdateJobStatusParams" (job_id - a
           job id status - the new status to set for the job.) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "status" of String
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        #BEGIN update_job_status
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        job_id = mr.update_job_status(params.get("job_id"), params.get("status"))
        #END update_job_status

        # At some point might do deeper type checking...
        if not isinstance(job_id, str):
            raise ValueError('Method update_job_status return value ' +
                             'job_id is not type str as required.')
        # return the results
        return [job_id]

    def add_job_logs(self, ctx, job_id, lines):
        """
        :param job_id: instance of type "job_id" (A job id.)
        :param lines: instance of list of type "LogLine" (line - string - a
           string to set for the log line. is_error - int - if 1, then this
           line should be treated as an error, default 0 ts - int - a
           timestamp since epoch in milliseconds for the log line (optional)
           @optional ts) -> structure: parameter "line" of String, parameter
           "is_error" of type "boolean" (@range [0,1]), parameter "ts" of Long
        :returns: instance of Long
        """
        # ctx is the context object
        # return variables are: line_number
        #BEGIN add_job_logs

        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"),
                             job_permission_cache=self.job_permission_cache)
        line_number = mr.add_job_logs(job_id=job_id, log_lines=lines)
        #END add_job_logs

        # At some point might do deeper type checking...
        if not isinstance(line_number, int):
            raise ValueError('Method add_job_logs return value ' +
                             'line_number is not type int as required.')
        # return the results
        return [line_number]

    def get_job_logs(self, ctx, params):
        """
        :param params: instance of type "GetJobLogsParams" (skip_lines -
           optional parameter, number of lines to skip (in case they were
           already loaded before).) -> structure: parameter "job_id" of type
           "job_id" (A job id.), parameter "skip_lines" of Long
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
           "last_line_number" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN get_job_logs
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.view_job_logs(
            job_id=params["job_id"], skip_lines=params.get("skip_lines", None)
        )
        #END get_job_logs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method get_job_logs return value ' +
                             'returnVal is not type dict as required.')
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
           "error" of String, parameter "job_output" of unspecified object
        """
        # ctx is the context object
        #BEGIN finish_job
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        mr.finish_job(params.get('job_id'),
                      error_message=params.get('error_message'),
                      error_code=params.get('error_code'),
                      error=params.get('error'),
                      job_output=params.get('job_output'))

        #END finish_job
        pass

    def start_job(self, ctx, params):
        """
        :param params: instance of type "StartJobParams" (skip_estimation:
           default true. If set true, job will set to running status skipping
           estimation step) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "skip_estimation" of type "boolean" (@range
           [0,1])
        """
        # ctx is the context object
        #BEGIN start_job
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        mr.start_job(
            params.get("job_id"),
            skip_estimation=params.get("skip_estimation", True),
        )
        #END start_job
        pass

    def check_job(self, ctx, params):
        """
        get current status of a job
        :param params: instance of type "CheckJobParams" (projection: project
           certain fields to return. default None. projection strings can be
           one of: ...) -> structure: parameter "job_id" of type "job_id" (A
           job id.), parameter "projection" of list of String
        :returns: instance of type "JobState" (job_id - string - id of the
           job user - string - user who started the job wsid - int - id of
           the workspace where the job is bound authstrat - string - what
           strategy used to authenticate the job job_input - object - inputs
           to the job (from the run_job call)  ## TODO - verify updated - int
           - timestamp since epoch in milliseconds of the last time the
           status was updated running - int - timestamp since epoch in
           milliseconds of when it entered the running state created - int -
           timestamp since epoch in milliseconds when the job was created
           finished - int - timestamp since epoch in milliseconds when the
           job was finished status - string - status of the job. one of the
           following: created - job has been created in the service
           estimating - an estimation job is running to estimate resources
           required for the main job, and which queue should be used queued -
           job is queued to be run running - job is running on a worker node
           completed - job was completed successfully error - job is no
           longer running, but failed with an error terminated - job is no
           longer running, terminated either due to user cancellation, admin
           cancellation, or some automated task error_code - int - internal
           reason why the job is an error. one of the following: 0 - unknown
           1 - job crashed 2 - job terminated by automation 3 - job ran over
           time limit 4 - job was missing its automated output document 5 -
           job authentication token expired errormsg - string - message (e.g.
           stacktrace) accompanying an errored job error - object - the
           JSON-RPC error package that accompanies the error code and message
           terminated_code - int - internal reason why a job was terminated,
           one of: 0 - user cancellation 1 - admin cancellation 2 -
           terminated by some automatic process @optional error @optional
           error_code @optional errormsg @optional terminated_code @optional
           estimating @optional running @optional finished) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter "user"
           of String, parameter "authstrat" of String, parameter "wsid" of
           Long, parameter "status" of String, parameter "job_input" of type
           "RunJobParams" (method - service defined in standard JSON RPC way,
           typically it's module name from spec-file followed by '.' and name
           of funcdef from spec-file corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "created" of Long, parameter
           "queued" of Long, parameter "estimating" of Long, parameter
           "running" of Long, parameter "finished" of Long, parameter
           "updated" of Long, parameter "error" of type "JsonRpcError" (Error
           block of JSON RPC response) -> structure: parameter "name" of
           String, parameter "code" of Long, parameter "message" of String,
           parameter "error" of String, parameter "error_code" of Long,
           parameter "errormsg" of String, parameter "terminated_code" of Long
        """
        # ctx is the context object
        # return variables are: job_state
        #BEGIN check_job
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        job_state = mr.check_job(
            params.get("job_id"),
            projection=params.get("projection", ["job_output"]),
        )
        #END check_job

        # At some point might do deeper type checking...
        if not isinstance(job_state, dict):
            raise ValueError('Method check_job return value ' +
                             'job_state is not type dict as required.')
        # return the results
        return [job_state]

    def check_jobs(self, ctx, params):
        """
        :param params: instance of type "CheckJobsParams" (As in check_job,
           projection strings can be used to return only useful fields. see
           CheckJobParams for allowed strings. return_list - optional, return
           list of job state if set to 1. Otherwise return a dict. Default
           1.) -> structure: parameter "job_ids" of list of type "job_id" (A
           job id.), parameter "projection" of list of String, parameter
           "return_list" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - id of the workspace where the job is bound
           authstrat - string - what strategy used to authenticate the job
           job_input - object - inputs to the job (from the run_job call)  ##
           TODO - verify updated - int - timestamp since epoch in
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
           accompanies the error code and message terminated_code - int -
           internal reason why a job was terminated, one of: 0 - user
           cancellation 1 - admin cancellation 2 - terminated by some
           automatic process @optional error @optional error_code @optional
           errormsg @optional terminated_code @optional estimating @optional
           running @optional finished) -> structure: parameter "job_id" of
           type "job_id" (A job id.), parameter "user" of String, parameter
           "authstrat" of String, parameter "wsid" of Long, parameter
           "status" of String, parameter "job_input" of type "RunJobParams"
           (method - service defined in standard JSON RPC way, typically it's
           module name from spec-file followed by '.' and name of funcdef
           from spec-file corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "created" of Long, parameter
           "queued" of Long, parameter "estimating" of Long, parameter
           "running" of Long, parameter "finished" of Long, parameter
           "updated" of Long, parameter "error" of type "JsonRpcError" (Error
           block of JSON RPC response) -> structure: parameter "name" of
           String, parameter "code" of Long, parameter "message" of String,
           parameter "error" of String, parameter "error_code" of Long,
           parameter "errormsg" of String, parameter "terminated_code" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_jobs

        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.check_jobs(
            params.get("job_ids"),
            projection=params.get("projection", ["job_output"]),
            return_list=params.get("return_list", 1),
        )
        #END check_jobs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def check_workspace_jobs(self, ctx, params):
        """
        :param params: instance of type "CheckWorkspaceJobsParams" (Check
           status of all jobs in a given workspace. Only checks jobs that
           have been associated with a workspace at their creation.
           return_list - optional, return list of job state if set to 1.
           Otherwise return a dict. Default 0.) -> structure: parameter
           "workspace_id" of String, parameter "projection" of list of
           String, parameter "return_list" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - id of the workspace where the job is bound
           authstrat - string - what strategy used to authenticate the job
           job_input - object - inputs to the job (from the run_job call)  ##
           TODO - verify updated - int - timestamp since epoch in
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
           accompanies the error code and message terminated_code - int -
           internal reason why a job was terminated, one of: 0 - user
           cancellation 1 - admin cancellation 2 - terminated by some
           automatic process @optional error @optional error_code @optional
           errormsg @optional terminated_code @optional estimating @optional
           running @optional finished) -> structure: parameter "job_id" of
           type "job_id" (A job id.), parameter "user" of String, parameter
           "authstrat" of String, parameter "wsid" of Long, parameter
           "status" of String, parameter "job_input" of type "RunJobParams"
           (method - service defined in standard JSON RPC way, typically it's
           module name from spec-file followed by '.' and name of funcdef
           from spec-file corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "created" of Long, parameter
           "queued" of Long, parameter "estimating" of Long, parameter
           "running" of Long, parameter "finished" of Long, parameter
           "updated" of Long, parameter "error" of type "JsonRpcError" (Error
           block of JSON RPC response) -> structure: parameter "name" of
           String, parameter "code" of Long, parameter "message" of String,
           parameter "error" of String, parameter "error_code" of Long,
           parameter "errormsg" of String, parameter "terminated_code" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_workspace_jobs
        mr = SDKMethodRunner(self.config, user_id=ctx["user_id"], token=ctx["token"])
        returnVal = mr.check_workspace_jobs(
            params.get("workspace_id"),
            projection=params.get("projection", ["job_output"]),
            return_list=params.get("return_list", 1),
        )
        #END check_workspace_jobs

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_workspace_jobs return value ' +
                             'returnVal is not type dict as required.')
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
           (A job id.), parameter "terminated_code" of Long
        """
        # ctx is the context object
        #BEGIN cancel_job
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        mr.cancel_job(job_id=params["job_id"], terminated_code=params['terminated_code'] )
        #END cancel_job
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
           (A job id.), parameter "terminated_code" of Long
        :returns: instance of type "CheckJobCanceledResult" (job_id - id of
           job running method finished - indicates whether job is done
           (including error/cancel cases) or not canceled - whether the job
           is canceled or not. ujs_url - url of UserAndJobState service used
           by job service) -> structure: parameter "job_id" of type "job_id"
           (A job id.), parameter "finished" of type "boolean" (@range
           [0,1]), parameter "canceled" of type "boolean" (@range [0,1]),
           parameter "ujs_url" of String
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN check_job_canceled
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        result = mr.check_job_canceled(job_id=params["job_id"])
        #END check_job_canceled

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method check_job_canceled return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def get_job_status(self, ctx, job_id):
        """
        Just returns the status string for a job of a given id.
        :param job_id: instance of type "job_id" (A job id.)
        :returns: instance of type "GetJobStatusResult" -> structure:
           parameter "status" of String
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN get_job_status
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        result = mr.get_job_status(job_id)
        #END get_job_status

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method get_job_status return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def check_jobs_date_range_for_user(self, ctx, params):
        """
        :param params: instance of type "CheckJobsDateRangeParams" (Check job
           for all jobs in a given date/time range for all users (Admin
           function) float start_time; # Filter based on creation timestamp
           since epoch float end_time; # Filter based on creation timestamp
           since epoch list<string> projection; # A list of fields to include
           in the projection, default ALL See "Projection Fields"
           list<string> filter; # A list of simple filters to "AND" together,
           such as error_code=1, wsid=1234, terminated_code = 1 int limit; #
           The maximum number of records to return string user; # Optional.
           Defaults off of your token @optional projection @optional filter
           @optional limit @optional user @optional offset @optional
           ascending) -> structure: parameter "start_time" of Double,
           parameter "end_time" of Double, parameter "projection" of list of
           String, parameter "filter" of list of String, parameter "limit" of
           Long, parameter "user" of String, parameter "offset" of Long,
           parameter "ascending" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - id of the workspace where the job is bound
           authstrat - string - what strategy used to authenticate the job
           job_input - object - inputs to the job (from the run_job call)  ##
           TODO - verify updated - int - timestamp since epoch in
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
           accompanies the error code and message terminated_code - int -
           internal reason why a job was terminated, one of: 0 - user
           cancellation 1 - admin cancellation 2 - terminated by some
           automatic process @optional error @optional error_code @optional
           errormsg @optional terminated_code @optional estimating @optional
           running @optional finished) -> structure: parameter "job_id" of
           type "job_id" (A job id.), parameter "user" of String, parameter
           "authstrat" of String, parameter "wsid" of Long, parameter
           "status" of String, parameter "job_input" of type "RunJobParams"
           (method - service defined in standard JSON RPC way, typically it's
           module name from spec-file followed by '.' and name of funcdef
           from spec-file corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "created" of Long, parameter
           "queued" of Long, parameter "estimating" of Long, parameter
           "running" of Long, parameter "finished" of Long, parameter
           "updated" of Long, parameter "error" of type "JsonRpcError" (Error
           block of JSON RPC response) -> structure: parameter "name" of
           String, parameter "code" of Long, parameter "message" of String,
           parameter "error" of String, parameter "error_code" of Long,
           parameter "errormsg" of String, parameter "terminated_code" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_jobs_date_range_for_user
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.check_jobs_date_range_for_user(
            creation_start_time=params.get("start_time"),
            creation_end_time=params.get("end_time"),
            job_projection=params.get("projection"),
            job_filter=params.get("filter"),
            limit=params.get("limit"),
            user=params.get("user"),
            offset=params.get("offset"),
            ascending=params.get("ascending")
        )
        #END check_jobs_date_range_for_user

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs_date_range_for_user return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def check_jobs_date_range_for_all(self, ctx, params):
        """
        :param params: instance of type "CheckJobsDateRangeParams" (Check job
           for all jobs in a given date/time range for all users (Admin
           function) float start_time; # Filter based on creation timestamp
           since epoch float end_time; # Filter based on creation timestamp
           since epoch list<string> projection; # A list of fields to include
           in the projection, default ALL See "Projection Fields"
           list<string> filter; # A list of simple filters to "AND" together,
           such as error_code=1, wsid=1234, terminated_code = 1 int limit; #
           The maximum number of records to return string user; # Optional.
           Defaults off of your token @optional projection @optional filter
           @optional limit @optional user @optional offset @optional
           ascending) -> structure: parameter "start_time" of Double,
           parameter "end_time" of Double, parameter "projection" of list of
           String, parameter "filter" of list of String, parameter "limit" of
           Long, parameter "user" of String, parameter "offset" of Long,
           parameter "ascending" of type "boolean" (@range [0,1])
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs could be mapping<job_id, JobState> or list<JobState>) ->
           structure: parameter "job_states" of list of type "JobState"
           (job_id - string - id of the job user - string - user who started
           the job wsid - int - id of the workspace where the job is bound
           authstrat - string - what strategy used to authenticate the job
           job_input - object - inputs to the job (from the run_job call)  ##
           TODO - verify updated - int - timestamp since epoch in
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
           accompanies the error code and message terminated_code - int -
           internal reason why a job was terminated, one of: 0 - user
           cancellation 1 - admin cancellation 2 - terminated by some
           automatic process @optional error @optional error_code @optional
           errormsg @optional terminated_code @optional estimating @optional
           running @optional finished) -> structure: parameter "job_id" of
           type "job_id" (A job id.), parameter "user" of String, parameter
           "authstrat" of String, parameter "wsid" of Long, parameter
           "status" of String, parameter "job_input" of type "RunJobParams"
           (method - service defined in standard JSON RPC way, typically it's
           module name from spec-file followed by '.' and name of funcdef
           from spec-file corresponding to running method (e.g.
           'KBaseTrees.construct_species_tree' from trees service); params -
           the parameters of the method that performed this call; Optional
           parameters: service_ver - specific version of deployed service,
           last version is used if this parameter is not defined rpc_context
           - context of current method call including nested call history
           remote_url - run remote service call instead of local command line
           execution. source_ws_objects - denotes the workspace objects that
           will serve as a source of data when running the SDK method. These
           references will be added to the autogenerated provenance. app_id -
           the id of the Narrative application running this job (e.g.
           repo/name) mapping<string, string> meta - user defined metadata to
           associate with the job. This data is passed to the User and Job
           State (UJS) service. wsid - a workspace id to associate with the
           job. This is passed to the UJS service, which will share the job
           based on the permissions of the workspace rather than UJS ACLs.
           parent_job_id - UJS id of the parent of a batch job. Sub jobs will
           add this id to the NJS database under the field "parent_job_id")
           -> structure: parameter "method" of String, parameter "params" of
           list of unspecified object, parameter "service_ver" of String,
           parameter "rpc_context" of type "RpcContext" (call_stack -
           upstream calls details including nested service calls and parent
           jobs where calls are listed in order from outer to inner.) ->
           structure: parameter "call_stack" of list of type "MethodCall"
           (time - the time the call was started; method - service defined in
           standard JSON RPC way, typically it's module name from spec-file
           followed by '.' and name of funcdef from spec-file corresponding
           to running method (e.g. 'KBaseTrees.construct_species_tree' from
           trees service); job_id - job id if method is asynchronous
           (optional field).) -> structure: parameter "time" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           either the character Z (representing the UTC timezone) or the
           difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time) 2013-04-03T08:56:32Z (UTC time)), parameter "method" of
           String, parameter "job_id" of type "job_id" (A job id.), parameter
           "run_id" of String, parameter "remote_url" of String, parameter
           "source_ws_objects" of list of type "wsref" (A workspace object
           reference of the form X/Y or X/Y/Z, where X is the workspace name
           or id, Y is the object name or id, Z is the version, which is
           optional.), parameter "app_id" of String, parameter "meta" of
           mapping from String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String, parameter "created" of Long, parameter
           "queued" of Long, parameter "estimating" of Long, parameter
           "running" of Long, parameter "finished" of Long, parameter
           "updated" of Long, parameter "error" of type "JsonRpcError" (Error
           block of JSON RPC response) -> structure: parameter "name" of
           String, parameter "code" of Long, parameter "message" of String,
           parameter "error" of String, parameter "error_code" of Long,
           parameter "errormsg" of String, parameter "terminated_code" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_jobs_date_range_for_all
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.check_jobs_date_range_for_user(
            creation_start_time=params.get("start_time"),
            creation_end_time=params.get("end_time"),
            job_projection=params.get("projection"),
            job_filter=params.get("filter"),
            limit=params.get("limit"),
            offset=params.get("offset"),
            ascending=params.get("ascending"),
            user='ALL'
        )
        #END check_jobs_date_range_for_all

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method check_jobs_date_range_for_all return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def is_admin(self, ctx):
        """
        Check if current user has ee2 admin rights.
        :returns: instance of type "boolean" (@range [0,1])
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN is_admin
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.check_is_admin()
        #END is_admin

        # At some point might do deeper type checking...
        if not isinstance(returnVal, int):
            raise ValueError('Method is_admin return value ' +
                             'returnVal is not type int as required.')
        # return the results
        return [returnVal]

    def get_admin_permission(self, ctx):
        """
        Check if current user has ee2 admin rights.
        If so, return the type of rights and their roles
        :returns: instance of type "AdminRolesResults" (list<string>
           admin_roles;  # ('ee2_admin','ee2_admin_ro') or something else str
           permissions; # One of ('read' | 'write' | 'none')) -> structure:
           parameter "permission" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN get_admin_permission
        mr = SDKMethodRunner(self.config, user_id=ctx.get("user_id"), token=ctx.get("token"))
        returnVal = mr.get_admin_permission()
        #END get_admin_permission

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method get_admin_permission return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]
