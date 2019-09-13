# -*- coding: utf-8 -*-
#BEGIN_HEADER

from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner


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
    GIT_URL = "https://github.com/Tianhao-Gu/execution_engine2.git"
    GIT_COMMIT_HASH = "57951e6ea54aa02d2da166e0acc62ceaef620e11"

    #BEGIN_CLASS_HEADER
    MONGO_COLLECTION = "jobs"
    MONGO_AUTHMECHANISM = "DEFAULT"

    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.config = config
        self.config["mongo-collection"] = self.MONGO_COLLECTION
        self.config.setdefault("mongo-authmechanism", self.MONGO_AUTHMECHANISM)
        #END_CONSTRUCTOR
        pass


    def list_config(self, ctx):
        """
        :returns: instance of mapping from String to String
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN list_config
        public_keys = ['external-url', 'kbase-endpoint', 'workspace-url', 'catalog-url',
                       'shock-url', 'handle-url', 'auth-service-url', 'auth-service-url-v2',
                       'auth-service-url-allow-insecure',
                       'scratch', 'executable', 'docker_timeout',
                       'intialdir', 'transfer_input_files']

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
        Returns the current running version of the NarrativeJobService.
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
        :returns: instance of type "Status" -> structure: parameter
           "reboot_mode" of type "boolean" (@range [0,1]), parameter
           "stopping_mode" of type "boolean" (@range [0,1]), parameter
           "running_tasks_total" of Long, parameter "running_tasks_per_user"
           of mapping from String to Long, parameter "tasks_in_queue" of
           Long, parameter "config" of mapping from String to String,
           parameter "git_commit" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN status
        mr = SDKMethodRunner(self.config)
        returnVal = mr.status()
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
           reference of the form X/Y/Z, where X is the workspace name or id,
           Y is the object name or id, Z is the version, which is optional.),
           parameter "app_id" of String, parameter "meta" of mapping from
           String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        #BEGIN run_job
        mr = SDKMethodRunner(self.config)
        job_id = mr.run_job(params, ctx)
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
           reference of the form X/Y/Z, where X is the workspace name or id,
           Y is the object name or id, Z is the version, which is optional.),
           parameter "app_id" of String, parameter "meta" of mapping from
           String to String, parameter "wsid" of Long, parameter
           "parent_job_id" of String
        """
        # ctx is the context object
        # return variables are: params
        #BEGIN get_job_params
        mr = SDKMethodRunner(self.config)
        params = mr.get_job_params(job_id, ctx)
        #END get_job_params

        # At some point might do deeper type checking...
        if not isinstance(params, dict):
            raise ValueError('Method get_job_params return value ' +
                             'params is not type dict as required.')
        # return the results
        return [params]

    def update_job_status(self, ctx, params):
        """
        :param params: instance of type "UpdateJobStatusParams" (typedef
           structure { job_id job_id; boolean is_started; } UpdateJobParams;
           typedef structure { list<string> messages; } UpdateJobResults;
           funcdef update_job(UpdateJobParams params) returns
           (UpdateJobResults) authentication required;) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "status" of String
        :returns: instance of type "job_id" (A job id.)
        """
        # ctx is the context object
        # return variables are: job_id
        #BEGIN update_job_status
        mr = SDKMethodRunner(self.config)
        job_id = mr.update_job_status(params.get("job_id"), params.get("status"), ctx)
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
        :param lines: instance of list of type "LogLine" -> structure:
           parameter "line" of String, parameter "is_error" of type "boolean"
           (@range [0,1]), parameter "ts" of String
        :returns: instance of Long
        """
        # ctx is the context object
        # return variables are: line_number
        #BEGIN add_job_logs
        mr = SDKMethodRunner(self.config)
        line_number = mr.add_job_logs(job_id=job_id, log_lines=lines, ctx=ctx)
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
           of type "LogLine" -> structure: parameter "line" of String,
           parameter "is_error" of type "boolean" (@range [0,1]), parameter
           "ts" of String, parameter "last_line_number" of Long
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN get_job_logs
        mr = SDKMethodRunner(self.config)
        returnVal = mr.view_job_logs(
            job_id=params["job_id"], skip_lines=params.get("skip_lines", None), ctx=ctx
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
        :param params: instance of type "FinishJobParams" (error_message:
           optional if job is finished with error job_output: job output if
           job completed successfully) -> structure: parameter "job_id" of
           type "job_id" (A job id.), parameter "error_message" of String,
           parameter "error_code" of Long, parameter "error" of type
           "JsonRpcError" (Error block of JSON RPC response) -> structure:
           parameter "name" of String, parameter "code" of Long, parameter
           "message" of String, parameter "error" of String, parameter
           "job_output" of unspecified object
        """
        # ctx is the context object
        #BEGIN finish_job
        mr = SDKMethodRunner(self.config)
        mr.finish_job(params.get('job_id'),
                      ctx,
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
        mr = SDKMethodRunner(self.config)
        mr.start_job(
            params.get("job_id"),
            ctx,
            skip_estimation=params.get("skip_estimation", True),
        )
        #END start_job
        pass

    def check_job(self, ctx, params):
        """
        get current status of a job
        :param params: instance of type "CheckJobParams" (projection:
           projecct certain fields to return. default None.) -> structure:
           parameter "job_id" of type "job_id" (A job id.), parameter
           "projection" of list of String
        :returns: instance of unspecified object
        """
        # ctx is the context object
        # return variables are: job_state
        #BEGIN check_job
        mr = SDKMethodRunner(self.config)
        job_state = mr.check_job(
            params.get("job_id"),
            ctx,
            projection=params.get("projection", ["job_output"]),
        )
        #END check_job

        # At some point might do deeper type checking...
        if not isinstance(job_state, object):
            raise ValueError('Method check_job return value ' +
                             'job_state is not type object as required.')
        # return the results
        return [job_state]

    def check_jobs(self, ctx, params):
        """
        :param params: instance of type "CheckJobsParams" -> structure:
           parameter "job_ids" of list of type "job_id" (A job id.),
           parameter "projection" of list of String
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs) -> structure: parameter "job_states" of mapping from type
           "job_id" (A job id.) to unspecified object
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_jobs

        mr = SDKMethodRunner(self.config)
        returnVal = mr.check_jobs(
            params.get("job_ids"),
            ctx,
            projection=params.get("projection", ["job_output"]),
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
        :param params: instance of type "CheckWorkspaceJobsParams" (Check job
           for all jobs in a given workspace) -> structure: parameter
           "workspace_id" of String, parameter "projection" of list of String
        :returns: instance of type "CheckJobsResults" (job_states - states of
           jobs) -> structure: parameter "job_states" of mapping from type
           "job_id" (A job id.) to unspecified object
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN check_workspace_jobs
        mr = SDKMethodRunner(self.config)
        returnVal = mr.check_workspace_jobs(
            params.get("workspace_id"),
            ctx,
            projection=params.get("projection", ["job_output"]),
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
        :param params: instance of type "CancelJobParams" -> structure:
           parameter "job_id" of type "job_id" (A job id.)
        """
        # ctx is the context object
        #BEGIN cancel_job
        mr = SDKMethodRunner(self.config)
        mr.cancel_job(job_id=params["job_id"], ctx=ctx)
        #END cancel_job
        pass

    def check_job_canceled(self, ctx, params):
        """
        Check whether a job has been canceled. This method is lightweight compared to check_job.
        :param params: instance of type "CancelJobParams" -> structure:
           parameter "job_id" of type "job_id" (A job id.)
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
        mr = SDKMethodRunner(self.config)
        result = mr.check_job_canceled(job_id=params["job_id"], ctx=ctx)
        #END check_job_canceled

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method check_job_canceled return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def get_job_status(self, ctx, job_id):
        """
        :param job_id: instance of type "job_id" (A job id.)
        :returns: instance of type "GetJobStatusResult" -> structure:
           parameter "status" of String
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN get_job_status
        mr = SDKMethodRunner(self.config)
        result = mr.get_job_status(job_id, ctx)
        #END get_job_status

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method get_job_status return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]
