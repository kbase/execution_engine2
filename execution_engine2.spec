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
            git_commit - the Git hash of the version of the module.
            version - the semantic version for the module.
            service - the name of the service.
            server_time - the current server timestamp since epoch

            # TODO - add some or all of the following
            reboot_mode - if 1, then in the process of rebooting
            stopping_mode - if 1, then in the process of stopping
            running_tasks_total - number of total running jobs
            running_tasks_per_user - mapping from user id to number of running jobs for that user
            tasks_in_queue - number of jobs in the queue that are not running
        */
        typedef structure {
            string git_commit;
            string version;
            string service;
            float server_time;
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
            ts - float - a timestamp since epoch for the log line (optional)

            @optional ts
        */
        typedef structure {
            string line;
            boolean is_error;
            float ts;
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
            updated - int - timestamp since epoch in milliseconds of the last time the status was updated
            running - int - timestamp since epoch in milliseconds of when it entered the running state
            created - int - timestamp since epoch in milliseconds when the job was created
            finished - int - timestamp since epoch in milliseconds when the job was finished
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
            int created;
            int queued;
            int estimating;
            int running;
            int finished;
            int updated;
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

        /*
            job_id job_id;
            @optional int terminated;
            termination_code one of:

            """
            Reasons for why the job was cancelled
            """
            Current Default is `terminated_by_user 0` so as to not update narrative client
            terminated_by_user = 0
            terminated_by_admin = 1
            terminated_by_automation = 2
        */
        typedef structure {
            job_id job_id;
            int terminated;
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


        /*
        Projection Fields
            user = StringField(required=True)
            authstrat = StringField(
                required=True, default="kbaseworkspace", validation=valid_authstrat
            )
            wsid = IntField(required=True)
            status = StringField(required=True, validation=valid_status)
            updated = DateTimeField(default=datetime.datetime.utcnow, autonow=True)
            estimating = DateTimeField(default=None)  # Time when job began estimating
            running = DateTimeField(default=None)  # Time when job started
            # Time when job finished, errored out, or was terminated by the user/admin
            finished = DateTimeField(default=None)
            errormsg = StringField()
            msg = StringField()
            error = DynamicField()

            terminated_code = IntField(validation=valid_termination_code)
            error_code = IntField(validation=valid_errorcode)
            scheduler_type = StringField()
            scheduler_id = StringField()
            scheduler_estimator_id = StringField()
            job_input = EmbeddedDocumentField(JobInput, required=True)
            job_output = DynamicField()
        /*


        /*
            Results of check_jobs_date_range
        */
        typedef structure {
            mapping<job_id, JobState> jobs;
            int count;
            int query_count;
            list<string> filter;
            int skip;
            list<string> projection;
            int limit;
            string sort_order;
        } CheckJobsDateRangeResults;



        /*
          Check job for all jobs in a given date/time range for all users (Admin function)
            float start_time; # Filter based on creation timestamp since epoch
            float end_time; # Filter based on creation timestamp since epoch
            list<string> projection; # A list of fields to include in the projection, default ALL See "Projection Fields"
            list<string> filter; # A list of simple filters to "AND" together, such as error_code=1, wsid=1234, terminated_code = 1
            int limit; # The maximum number of records to return
            string user; # Optional. Defaults off of your token
            @optional projection
            @optional filter
            @optional limit
            @optional user
            @optional offset
            @optional ascending
        */
        typedef structure {
            float start_time;
            float end_time;
            list<string> projection;
            list<string> filter;
            int limit;
            string user;
            int offset;
            boolean ascending;
        } CheckJobsDateRangeParams;
        funcdef check_jobs_date_range_for_user(CheckJobsDateRangeParams params) returns (CheckJobsResults) authentication required;
        funcdef check_jobs_date_range_for_all(CheckJobsDateRangeParams params) returns (CheckJobsResults) authentication required;

        /*
            Check if given user (user_token) has admin rights.

            if user_token is given, current user must have admin rights.
            otherwise, return whether current user is an admin nor not.

            @optional user_token
        */
        typedef structure {
            string user_token;
        } IsAdminParams;
        funcdef is_admin(IsAdminParams params) returns (boolean) authentication required;

    };
