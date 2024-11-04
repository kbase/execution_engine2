# execution_engine2 (ee2) release notes
=========================================


## 0.0.15
- Update JobRunner and Execution Engine to substitute internal urls to go through cloudflare

## 0.0.14
- Update clients to work with mongo7 by updating pymongo and mongoengine
-  Fix linting issues from flake8 and black
- Update outdated libraries that can no longer be pip installed
- The latest Dockerfile build contains conda which installs python 3.12.4 so these changes will pin python to 3.10 and save us headache in the future
- Speed up tests by moving docker build step out
- Create matrix of tests to run against mongo3.6 and mongo7

## 0.0.13
* Bump version in impl file and sync branches

## 0.0.12 
* Forcing black to 22.1.0 to make sure that GHA doesn't suddenly fail
* Prevent jobs that never ran from submitting job execution stats

## 0.0.11
* Add ability for `kbase` user to contact condor via token

## 0.0.10
* Fixes bug with ee2 not recording all jobs with the catalog during the process 
of finishing a job
* Updates GHA with black and flake8
* Fix flake8 and black formatting issues by formatting MANY files
* Updated docs for installing htcondor
* Update many python libs in requirements.txt


## 0.0.9
* Update GHA with latest actions, remove old actions
* Change job defaults to result in 
* NJS and KB_UPLOAD 5 Jobs
* Bigmem nodes with 250GB of ram take 1 job
* Bigmem nodes with 1TB of ram take 4 jobs
* Remove Jars from built image / cleanup for trivy

## 0.0.81
* Updated HTCondor Clients, New Base Image
* Use default GH actions
* Updated precommit hooks

## 0.0.8
* Fixed a bug that could, seemingly rarely, cause job and log updates to be applied to the
  wrong Mongo collection.
* Removed docker shaded jar that causes log4j scan to appear positive with Trivy

## 0.0.7
* Fixed a bug that could cause missing `queued` timestamps if many jobs were submitted in a
  batch

## 0.0.6
* Release of MVP


## 0.0.5
### New Endpoints
*   run_job_batch
*   retry_job
*   retry_jobs
*   abandon_children
  
### BugFixes
* Fix a bug that caused job requirements from the catalog in CSV format to be ignored other
    than the client group
  
### Other features and refactoring
* Refactor run_jobs_batch endpoint to cache catalog calls for batch jobs, submit entire batch to condor in one transaction
* Refactored tests
* Removed slack messages for running jobs
* Added CreatedJobsReaper
* Added retry_job and retry_jobs endpoint along with ADRs
* Full EE2 admins can now submit job requirements when running jobs via run_job_batch and
run_job. See the SDK spec for details.
* Added ADRs for retry endpoint


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
