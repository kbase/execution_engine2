# execution_engine2 (ee2) release notes
=========================================


## 0.0.5
  * TODO Refactor run_jobs_batch endpoint to cache catalog calls for batch jobs, submit entire batch to condor in one transaction
  * TODO: Added CreatedJobsReaper
  * Added retry_job and retry_jobs endpoint along with ADRs
  * TODO: Will deprecate run_job transaction and use scheduler API
  * Refactored tests
  * Removed slack messages for running jobs
  * Fix a bug that caused job requirements from the catalog in CSV format to be ignored other
    than the client group
  * Full EE2 admins can now submit job requirements when running jobs via run_job_batch and
    run_job. See the SDK spec for details.


## 0.0.4
  * Fix up tests
  * Remove dependency on slack
  * Add batch endpoints, cancel_jobs now cancels child jobs
  * Rename prod branch to "main"

## 0.0.3.4
  * Change 7 day periodic_remove to 7 day hold
  * Prevent reaper from prematurely exiting
  
## 0.0.3.3
  * Change log appends from update to $push
  * Change first log linepos to position 0 instead of 1

## 0.0.3.2
  * Change 2 db state updates for startjob into 1
  * Rework Perms Model 
  * Stop Pymongo/Mongoengine clients on each request, and make it an instance variable for each worker thread.

## 0.0.3.1
  * Logging endpoint returns success value now
  * Added script to fix backfilled records
  * Logs transferred for held jobs ON_EXIT_OR_EVICT
  * Enabled job log monitor again

## 0.0.3
  * Fix Quay Build

## 0.0.2

  *  Fixed bug with service version displaying release instead of git commit
  *  Updated clients
  *  Update transfer script to handle failures

## 0.0.0 
  *  Module created by kb-sdk init
  *  We Were Young
