module execution_engine2 {

    /* @range [0,1] */
    typedef int boolean;

    /*
        A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is either the
        character Z (representing the UTC timezone) or the difference
        in time to UTC in the format +/-HHMM, eg:
            2012-12-17T23:24:06-0500 (EST time)
            2013-04-03T08:56:32+0000 (UTC time)
            2013-04-03T08:56:32Z (UTC time)
    */
    typedef string timestamp;

    /* A job id. */
    typedef string job_id;

    /*
        A structure representing the Execution Engine status
        reboot_mode - if 1, then in the process of rebooting
        stopping_mode - if 1, then in the process of stopping
        running_tasks_total - number of total running jobs
        running_tasks_per_user - mapping from user id to number of running jobs for that user
        tasks_in_queue - number of jobs in the queue that are not running
        config - the current service configuration (see also list_config())
        git_commit - the Git commit string denoting the deployed version of EE2
    */
    typedef structure {
        boolean reboot_mode;
        boolean stopping_mode;
        int running_tasks_total;
        mapping<string, int> running_tasks_per_user;
        int tasks_in_queue;
        mapping<string, string> config;
        string git_commit;
        timestamp servertime;
    } Status;

    /*
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
    */
    funcdef list_config() returns (mapping<string, string>) authentication optional;

    /* Returns the current running version of the execution_engine2 servicve as a semantic version string. */
    funcdef ver() returns (string);

    /* Simply check the status of this service to see queue details */
    funcdef status() returns (Status);

    /*================================================================================*/
    /*  Running long running methods through Docker images of services from Registry  */
    /*================================================================================*/
    /* A workspace object reference of the form X/Y or X/Y/Z, where
       X is the workspace name or id,
       Y is the object name or id,
       Z is the version, which is optional.
     */
    typedef string wsref;

    /*
        time - the time the call was started;
        method - service defined in standard JSON RPC way, typically it's
            module name from spec-file followed by '.' and name of funcdef
            from spec-file corresponding to running method (e.g.
            'KBaseTrees.construct_species_tree' from trees service);
        job_id - job id if method is asynchronous (optional field).
    */
    typedef structure {
        timestamp time;
        string method;
        job_id job_id;
    } MethodCall;

    /*
        call_stack - upstream calls details including nested service calls and
            parent jobs where calls are listed in order from outer to inner.
    */
    typedef structure {
        list<MethodCall> call_stack;
        string run_id;
    } RpcContext;

    /*
        method - service defined in standard JSON RPC way, typically it's
            module name from spec-file followed by '.' and name of funcdef
            from spec-file corresponding to running method (e.g.
            'KBaseTrees.construct_species_tree' from trees service);
        params - the parameters of the method that performed this call;

        Optional parameters:
        service_ver - specific version of deployed service, last version is
            used if this parameter is not defined
        rpc_context - context of current method call including nested call
            history
        remote_url - run remote service call instead of local command line
            execution.
        source_ws_objects - denotes the workspace objects that will serve as a
            source of data when running the SDK method. These references will
            be added to the autogenerated provenance.
        app_id - the id of the Narrative application running this job (e.g.
            repo/name)
        mapping<string, string> meta - user defined metadata to associate with
            the job. This data is passed to the User and Job State (UJS)
            service.
        wsid - a workspace id to associate with the job. This is passed to the
            UJS service, which will share the job based on the permissions of
            the workspace rather than UJS ACLs.
        parent_job_id - UJS id of the parent of a batch job. Sub jobs will add
        this id to the NJS database under the field "parent_job_id"
    */
    typedef structure {
        string method;
        list<UnspecifiedObject> params;
        string service_ver;
        RpcContext rpc_context;
        string remote_url;
        list<wsref> source_ws_objects;
        string app_id;
        mapping<string, string> meta;
        int wsid;
        string parent_job_id;
    } RunJobParams;

    /*
        Start a new job (long running method of service registered in ServiceRegistery).
        Such job runs Docker image for this service in script mode.
    */
    funcdef run_job(RunJobParams params) returns (job_id job_id) authentication required;

    /*
        Get job params necessary for job execution
    */
    funcdef get_job_params(job_id job_id) returns (RunJobParams params) authentication required;

    /*
        job_id - a job id
        status - the new status to set for the job.
    */
    typedef structure {
        job_id job_id;
        string status;
    } UpdateJobStatusParams;

    funcdef update_job_status(UpdateJobStatusParams params) returns (job_id job_id)
        authentication required;

    /*
        line - string - a string to set for the log line.
        is_error - int - if 1, then this line should be treated as an error, default 0
        ts - string - a timestamp for the log line (optional)

        @optional ts
    */
    typedef structure {
        string line;
        boolean is_error;
        string ts;
    } LogLine;
    funcdef add_job_logs(job_id job_id, list<LogLine> lines)
        returns (int line_number) authentication required;

    /*
        skip_lines - optional parameter, number of lines to skip (in case they were
            already loaded before).
    */
    typedef structure {
        job_id job_id;
        int skip_lines;
    } GetJobLogsParams;

    /*
        last_line_number - common number of lines (including those in skip_lines
            parameter), this number can be used as next skip_lines value to
            skip already loaded lines next time.
    */
    typedef structure {
        list<LogLine> lines;
        int last_line_number;
    } GetJobLogsResults;
    funcdef get_job_logs(GetJobLogsParams params) returns (GetJobLogsResults)
        authentication required;

    /* Error block of JSON RPC response */
    typedef structure {
        string name;
        int code;
        string message;
        string error;
    } JsonRpcError;

    /*
        job_id - string - the id of the job to mark finished
        error_message - string - optional if job is finished with and error
        error_code - int - optional if job finished with an error
        error - JsonRpcError - optional
        job_output - job output if job completed successfully
    */
    typedef structure {
        job_id job_id;
        string error_message;
        int error_code;
        JsonRpcError error;
        UnspecifiedObject job_output;
    } FinishJobParams;

    /*
        Register results of already started job
    */
    funcdef finish_job(FinishJobParams params) returns () authentication required;

    /*
        skip_estimation: default true. If set true, job will set to running status skipping estimation step
    */
    typedef structure {
        job_id job_id;
        boolean skip_estimation;
    } StartJobParams;
    funcdef start_job(StartJobParams params) returns () authentication required;

    /*
        projection: project certain fields to return. default None.
        projection strings can be one of:
            ...
    */
    typedef structure {
        job_id job_id;
        list<string> projection;
    } CheckJobParams;

    /*
        job_id - string - id of the job
        user - string - user who started the job
        wsid - int - id of the workspace where the job is bound
        authstrat - string - what strategy used to authenticate the job
        job_input - object - inputs to the job (from the run_job call)  ## TODO - verify
        updated - string - timestamp of the last time the status was updated
        running - string - timestamp of when it entered the running state
        created - string - timestamp when the job was created
        finished - string - timestamp when the job was finished
        status - string - status of the job. one of the following:
            created - job has been created in the service
            estimating - an estimation job is running to estimate resources required for the main
                         job, and which queue should be used
            queued - job is queued to be run
            running - job is running on a worker node
            finished - job was completed successfully
            error - job is no longer running, but failed with an error
            terminated - job is no longer running, terminated either due to user cancellation,
                         admin cancellation, or some automated task
        error_code - int - internal reason why the job is an error. one of the following:
            0 - unknown
            1 - job crashed
            2 - job terminated by automation
            3 - job ran over time limit
            4 - job was missing its automated output document
            5 - job authentication token expired
        errormsg - string - message (e.g. stacktrace) accompanying an errored job
        error - object - the JSON-RPC error package that accompanies the error code and message

        terminated_code - int - internal reason why a job was terminated, one of:
            0 - user cancellation
            1 - admin cancellation
            2 - terminated by some automatic process

        @optional error
        @optional error_code
        @optional errormsg
        @optional terminated_code
        @optional estimating
        @optional running
        @optional finished
    */


    typedef structure {
        job_id job_id;
        string user;
        string authstrat;
        int wsid;
        string status;
        RunJobParams job_input;
        timestamp created;
        timestamp updated;

        timestamp estimating;
        timestamp running;
        timestamp finished;

        JsonRpcError error;
        int error_code;
        string errormsg;

        int terminated_code;
    } JobState;

    /*
        get current status of a job
    */
    funcdef check_job(CheckJobParams params) returns (JobState job_state) authentication required;

    /*
        job_states - states of jobs
    */
    typedef structure {
        mapping<job_id, JobState> job_states;
    } CheckJobsResults;

    /*
        As in check_job, projection strings can be used to return only useful fields.
        see CheckJobParams for allowed strings.
    */
    typedef structure {
        list<job_id> job_ids;
        list<string> projection;
    } CheckJobsParams;

    funcdef check_jobs(CheckJobsParams params) returns (CheckJobsResults) authentication required;

    /*
        Check status of all jobs in a given workspace. Only checks jobs that have been associated
        with a workspace at their creation.
    */
    typedef structure {
        string workspace_id;
        list<string> projection;
    } CheckWorkspaceJobsParams;
    funcdef check_workspace_jobs(CheckWorkspaceJobsParams params) returns (CheckJobsResults) authentication required;

    typedef structure {
        job_id job_id;
    } CancelJobParams;

    /*
        Cancels a job. This results in the status becoming "terminated" with termination_code 0.
    */
    funcdef cancel_job(CancelJobParams params) returns () authentication required;

    /*
        job_id - id of job running method
        finished - indicates whether job is done (including error/cancel cases) or not
        canceled - whether the job is canceled or not.
        ujs_url - url of UserAndJobState service used by job service
    */
    typedef structure {
        job_id job_id;
        boolean finished;
        boolean canceled;
        string ujs_url;
    } CheckJobCanceledResult;

    /* Check whether a job has been canceled. This method is lightweight compared to check_job. */
    funcdef check_job_canceled(CancelJobParams params) returns (CheckJobCanceledResult result)
        authentication required;

    typedef structure {
        string status;
    } GetJobStatusResult;

    /* Just returns the status string for a job of a given id. */
    funcdef get_job_status(job_id job_id) returns (GetJobStatusResult result) authentication required;
};
