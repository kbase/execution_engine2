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
        /* A workspace object reference of the form X/Y/Z, where
           X is the workspace id,
           Y is the object id,
           Z is the version.
         */
        typedef string wsref;

        /* Narrative metadata for a job. All fields are optional.
            run_id - the Narrative-assigned ID of the job run. 1:1 with a job ID.
            token_id - the ID of the token used to run the method.
            tag - the release tag, e.g. dev/beta/release.
            cell_id - the ID of the narrative cell from which the job was run.
        */
        typedef structure {
            string run_id;
            string token_id;
            string tag;
            string cell_id;
        } Meta;

        /* Job requirements for a job. All fields are optional. To submit job requirements,
            the user must have full EE2 admin permissions. Ignored for the run concierge endpoint.

            request_cpus: the number of CPUs to request for the job.
            request_memory: the amount of memory, in MB, to request for the job.
            request_disk: the amount of disk space, in GB, to request for the job.
            client_group: the name of the client group on which to run the job.
            client_group_regex: Whether to treat the client group string, whether provided here,
                from the catalog, or as a default, as a regular expression when matching
                clientgroups. Default True for HTC, but the default depends on the scheduler.
                Omit to use the default.
            bill_to_user: the job will be counted against the provided user's fair share quota.
            ignore_concurrency_limits: ignore any limits on simultaneous job runs. Default false.
            scheduler_requirements: arbitrary key-value pairs to be provided to the job
                scheduler. Requires knowledge of the scheduler interface.
            debug_mode: Whether to run the job in debug mode. Default false.

        */
        typedef structure {
            int request_cpus;
            int requst_memory;
            int request_disk;
            string client_group;
            boolean client_group_regex;
            string bill_to_user;
            boolean ignore_concurrency_limits;
            mapping<string, string> scheduler_requirements;
            boolean debug_mode;
        } JobRequirements;

        /*
            method - the SDK method to run in module.method format, e.g.
                'KBaseTrees.construct_species_tree'
            params - the parameters to pass to the method.

            Optional parameters:
            app_id - the id of the Narrative application (UI) running this job (e.g.
                repo/name)
            service_ver - specific version of deployed service, last version is
                used if this parameter is not defined
            source_ws_objects - denotes the workspace objects that will serve as a
                source of data when running the SDK method. These references will
                be added to the autogenerated provenance. Must be in UPA format (e.g.
                6/90/4).
            meta - Narrative metadata to associate with the job.
            wsid - an optional workspace id to associate with the job. This is passed to the
                workspace service, which will share the job based on the permissions of
                the workspace rather than owner of the job
            parent_job_id - EE2 job id for the parent of the current job.
                For run_job and run_job_concierge, this value can be specified to denote
                the parent job of the job being created.
                Warning: No checking is done on the validity of the job ID, and the parent job
                record is not altered.
                Submitting a job with a parent ID to run_job_batch will cause an error to be
                returned.
            job_requirements: the requirements for the job. The user must have full EE2
                administration rights to use this parameter. Note that the job_requirements
                are not returned along with the rest of the job parameters when querying the EE2
                API - they are only considered when submitting a job.
            as_admin: run the job with full EE2 permissions, meaning that any supplied workspace
                IDs are not checked for accessibility and job_requirements may be supplied. The
                user must have full EE2 administration rights.
                Note that this field is not included in returned data when querying EE2.

        */
        typedef structure {
            string method;
            string app_id;
            list<UnspecifiedObject> params;
            string service_ver;
            list<wsref> source_ws_objects;
            Meta meta;
            int wsid;
            string parent_job_id;
            JobRequirements job_requirements;
            boolean as_admin;
        } RunJobParams;

        /*
            Start a new job.
        */
        funcdef run_job(RunJobParams params) returns (job_id job_id) authentication required;

        /* Additional parameters for a batch job.
            wsid: the workspace with which to associate the parent job.
            as_admin: run the job with full EE2 permissions, meaning that any supplied workspace
                IDs are not checked for accessibility and job_requirements may be supplied. The
                user must have full EE2 administration rights.
        */
        typedef structure {
            int wsid;
            boolean as_admin;
        } BatchParams;

        typedef structure {
            job_id parent_job_id;
            list<job_id> child_job_ids;
        } BatchSubmission;

        typedef structure {
            job_id parent_job_id;
            list<job_id> child_job_ids;
            boolean as_admin;
        } AbandonChildren;

        /* Run a batch job, consisting of a parent job and one or more child jobs.
            Note that the as_admin parameters in the list of child jobs are ignored -
            only the as_admin parameter in the batch_params is considered.
        */
        funcdef run_job_batch(list<RunJobParams> params, BatchParams batch_params)
            returns (BatchSubmission job_ids) authentication required;


        /*
            job_id of retried job
        */
        typedef structure {
            job_id job_id;
        } RetryResult;

        /*
            job_id of job to retry
        */
        typedef structure {
            job_id job_id;
            boolean as_admin;
        } RetryParams;

        /*
            job_ids of job to retry
        */
        typedef structure {
            list<job_id> job_ids;
            boolean as_admin;
        } BulkRetryParams;

        /*
            job_id of retried job
        */
        typedef structure {
            job_id job_id;
            string error;
        } BulkRetryResult;



        /*
            Retry a list of jobs based on records in ee2 db, return a job id or error out
        */
        funcdef retry_jobs(BulkRetryParams params) returns (list<BulkRetryResult> retry_results) authentication required;

        /*
            Retry a job based on record in ee2 db, return a job id or error out
        */
        funcdef retry_job(RetryParams params) returns (job_id job_id) authentication required;

        funcdef abandon_children(AbandonChildren params)
            returns (BatchSubmission parent_and_child_ids) authentication required;



        /* EE2Constants Concierge Params are
            request_cpus: int
            request_memory: int in MB
            request_disk: int in GB
            job_priority: int = None  range from -20 to +20, with higher values meaning better priority.
                Note: job_priority is currently not implemented.
            account_group: str = None # Someone elses account
            ignore_concurrency_limits: ignore any limits on simultaneous job runs.
                Default 1 (True).
            requirements_list: list = None ['machine=worker102','color=red']
            client_group: Optional[str] = CONCIERGE_CLIENTGROUP # You can leave default or specify a clientgroup
            client_group_regex: Whether to treat the client group string, whether provided here,
                from the catalog, or as a default, as a regular expression when matching
                clientgroups. Default True for HTC, but the default depends on the scheduler.
                Omit to use the default.
            debug_mode: Whether to run the job in debug mode. Default 0 (False).
        */

        typedef structure {
            int request_cpu;
            int request_memory;
            int request_disk;
            int job_priority;
            string account_group;
            boolean ignore_concurrency_limits;
            list<string> requirements_list;
            string client_group;
            boolean client_group_regex;
            boolean debug_mode;
        } ConciergeParams;


        funcdef run_job_concierge(RunJobParams params, ConciergeParams concierge_params) returns (job_id job_id) authentication required;


        /*
            Get job params necessary for job execution
            @optional as_admin
        */
        typedef structure {
            job_id job_id;
            boolean as_admin;
        } GetJobParams;

        funcdef get_job_params(GetJobParams params) returns (RunJobParams params) authentication required;

        /*
            job_id - a job id
            status - the new status to set for the job.
        */
        typedef structure {
            job_id job_id;
            string status;
            boolean as_admin;
        } UpdateJobStatusParams;

        funcdef update_job_status(UpdateJobStatusParams params) returns (job_id job_id) authentication required;


        /*
            line - string - a string to set for the log line.
            is_error - int - if 1, then this line should be treated as an error, default 0
            ts - int - a timestamp since epoch in milliseconds for the log line (optional)

            @optional ts
        */
        typedef structure {
            string line;
            boolean is_error;
            int ts;
        } LogLine;

        /*
            @success Whether or not the add operation was successful
            @line_number the line number of the last added log
        */
        typedef structure {
            boolean success;
            int line_number;
        } AddJobLogsResults;

        typedef structure {
            job_id job_id;
            boolean as_admin;
        } AddJobLogsParams;
        funcdef add_job_logs(AddJobLogsParams params, list<LogLine> lines) returns (AddJobLogsResults results) authentication required;



        /*
            last_line_number - common number of lines (including those in skip_lines
                parameter), this number can be used as next skip_lines value to
                skip already loaded lines next time.
        */
        typedef structure {
            list<LogLine> lines;
            int last_line_number;
            int count;
        } GetJobLogsResults;


        /*
            job id - the job id
            optional skip_lines Legacy Parameter for Offset
            optional offset  Number of lines to skip (in case they were already loaded before).
            optional limit  optional parameter, maximum number of lines returned
            optional as_admin  request read access to record normally not allowed..
        */
        typedef structure {
            job_id job_id;
            int skip_lines;
            int offset;
            int limit;
            boolean as_admin;
        } GetJobLogsParams;

        funcdef get_job_logs(GetJobLogsParams params) returns (GetJobLogsResults) authentication required;


        /* Error block of JSON RPC response */
        typedef structure {
            string name;
            int code;
            string message;
            string error;
        } JsonRpcError;

    /*
        job_id - string - the id of the job to mark completed or finished with an error
        error_message - string - optional unless job is finished with an error
        error_code - int - optional unless job finished with an error
        error - JsonRpcError - optional output from SDK Job Containers
        job_output - job output if job completed successfully
    */
    typedef structure {
        job_id job_id;
        string error_message;
        int error_code;
        JsonRpcError error;
        UnspecifiedObject job_output;
        boolean as_admin;
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
            boolean as_admin;
        } StartJobParams;
        funcdef start_job(StartJobParams params) returns () authentication required;

        /*
            exclude_fields: exclude certain fields to return. default None.
            exclude_fields strings can be one of fields defined in execution_engine2.db.models.models.Job
        */
        typedef structure {
            job_id job_id;
            list<string> exclude_fields;
            boolean as_admin;
        } CheckJobParams;

    /*
        job_id - string - id of the job
        user - string - user who started the job
        wsid - int - optional id of the workspace where the job is bound
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
            completed - job was completed successfully
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
            parent_job - state of parent job
            job_states - states of child jobs
            IDEA: ADD aggregate_states - count of all available child job states, even if they are zero
        */
        typedef structure {
            JobState parent_jobstate;
            list<JobState> child_jobstates;
        } CheckJobBatchResults;

        /*
            get current status of a parent job, and it's children, if it has any.
        */
        funcdef check_job_batch(CheckJobParams params) returns (CheckJobBatchResults) authentication required;

        /*
            job_states - states of jobs
            could be mapping<job_id, JobState> or list<JobState>
        */
        typedef structure {
            list<JobState> job_states;
        } CheckJobsResults;

        /*
            As in check_job, exclude_fields strings can be used to exclude fields.
            see CheckJobParams for allowed strings.

            return_list - optional, return list of job state if set to 1. Otherwise return a dict. Default 1.
        */
        typedef structure {
            list<job_id> job_ids;
            list<string> exclude_fields;
            boolean return_list;
        } CheckJobsParams;

        funcdef check_jobs(CheckJobsParams params) returns (CheckJobsResults) authentication required;


        /*
            Check status of all jobs in a given workspace. Only checks jobs that have been associated
            with a workspace at their creation.

            return_list - optional, return list of job state if set to 1. Otherwise return a dict. Default 0.
        */
        typedef structure {
            string workspace_id;
            list<string> exclude_fields;
            boolean return_list;
            boolean as_admin;
        } CheckWorkspaceJobsParams;
        funcdef check_workspace_jobs(CheckWorkspaceJobsParams params) returns (CheckJobsResults) authentication required;

        /*
        cancel_and_sigterm
            """
            Reasons for why the job was cancelled
            Current Default is `terminated_by_user 0` so as to not update narrative client
            terminated_by_user = 0
            terminated_by_admin = 1
            terminated_by_automation = 2
            """
            job_id job_id
            @optional terminated_code
        */
        typedef structure {
            job_id job_id;
            int terminated_code;
            boolean as_admin;
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
            boolean as_admin;
        } CheckJobCanceledResult;

        /* Check whether a job has been canceled. This method is lightweight compared to check_job. */
        funcdef check_job_canceled(CancelJobParams params) returns (CheckJobCanceledResult result)            authentication required;


        typedef structure {
            string status;
        } GetJobStatusResult;


        typedef structure {
            job_id job_id;
            boolean as_admin;
        } GetJobStatusParams;


        /* Just returns the status string for a job of a given id. */
        funcdef get_job_status(GetJobStatusParams params) returns (GetJobStatusResult result) authentication required;

        /*
        Projection Fields
            user = StringField(required=True)
            authstrat = StringField(
                required=True, default="kbaseworkspace", validation=valid_authstrat
            )
            wsid = IntField(required=False)
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
            Results of check_jobs_date_range methods.

            jobs - the jobs matching the query, up to `limit` jobs.
            count - the number of jobs returned.
            query_count - the number of jobs that matched the filters.
            filter - DEPRECATED - this field may change in the future. The filters that were
                applied to the jobs.
            skip - the number of jobs that were skipped prior to beginning to return jobs.
            projection - the list of fields included in the returned job. By default all fields.
            limit - the maximum number of jobs returned.
            sort_order - the order in which the results were sorted by the job ID - + for
                ascending, - for descending.

            TODO: DOCUMENT THE RETURN OF STATS mapping
        */
        typedef structure {
            list<JobState> jobs;
            int count;
            int query_count;
            mapping<string, string> filter;
            int skip;
            list<string> projection;
            int limit;
            string sort_order;
        } CheckJobsDateRangeResults;

        /*
          Check job for all jobs in a given date/time range for all users (Admin function)
            Notes on start_time and end_time:
                These fields are designated as floats but floats, ints, and strings are all
                accepted. Times are determined as follows:
                - if the field is a float or a string that contains a float and only a float,
                    the field value is treated as seconds since the epoch.
                - if the field is an int or a string that contains an int and only an int,
                    the field value is treated as milliseconds since the epoch.
                - if the field is a string not matching the criteria above, it is treated as
                    a date and time. Nearly any unambigous format can be parsed.

            float start_time - Filter based on job creation timestamp since epoch
            float end_time - Filter based on job creation timestamp since epoch
            list<string> projection - A list of fields to include in the projection, default ALL
                See "Projection Fields" above
            list<string> filter - DEPRECATED: this field may change or be removed in the future.
                A list of simple filters to "AND" together, such as error_code=1, wsid=1234,
                terminated_code = 1
            int limit - The maximum number of records to return
            string user - The user whose job records will be returned. Optional. Default is the
                current user.
            int offset - the number of jobs to skip before returning records.
            boolean ascending - true to sort by job ID ascending, false descending.
            boolean as_admin - true to run the query as an admin; user must have admin EE2
                permissions. Required if setting `user` to something other than your own.
                TODO: this seems to have no effect
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
            boolean as_admin;
        } CheckJobsDateRangeParams;

        funcdef check_jobs_date_range_for_user(CheckJobsDateRangeParams params)
            returns (CheckJobsDateRangeResults) authentication required;
        funcdef check_jobs_date_range_for_all(CheckJobsDateRangeParams params)
            returns (CheckJobsDateRangeResults) authentication required;

        typedef structure {
            UnspecifiedObject held_job;
        } HeldJob;
        /*
            Handle a held CONDOR job. You probably never want to run this, only the reaper should run it.
        */
        funcdef handle_held_job(string cluster_id) returns (HeldJob) authentication required;

        /*
            Check if current user has ee2 admin rights.
        */
        funcdef is_admin() returns (boolean) authentication required;


        /*
            str permission - One of 'r|w|x' (('read' | 'write' | 'none'))
        */
          typedef structure {
            string permission;
        } AdminRolesResults;

        /*
            Check if current user has ee2 admin rights.
            If so, return the type of rights and their roles
        */
        funcdef get_admin_permission()  returns (AdminRolesResults) authentication required;

        /* Get a list of clientgroups manually extracted from the config file */
        funcdef get_client_groups() returns (list<string> client_groups);


    };