# Retry Endpoint

Date: 2021-04-27


## For discussion on this ADR, see the following PR https://github.com/kbase/execution_engine2/pull/367

The current requirement for the Batch/Bulk UI is to be able to retry jobs. Using a job id, it should be possible to get information from the database.

The current implementation of retry is to run jobs using the `run_job` or `run_job_batch` endpoint. This is not adequate due to the following deficiencies:


* Lack of book-keeping for the parent job and the child job relationship: 
* 1) Launching a new job will not update the relationship between those jobs. 
* 2) e.g. the child job can specify a parent_job_id, but the parent_job will not know about the child_job
* 3) e.g. the parent will not know about new retried jobs
* 4) e.g. the child will not know how many times it was retried

* Lack of book-keeping for number of retries for a particular job / set of job inputs
* Lack of ability to launch multiple jobs using the `run_job_batch` endpoint without creating a new parent job
* Lack of ability to ensure that the proper catalog version /git commit of an app is used from the front end based on a tag, such as "beta/dev/release"
* Lack of ability to specify which retries succeeded and which ones failed during submit time. 

* Code is split more than is necessary

## Author(s)

@bio-boris

## Status

Pending

## Alternatives Considered

* Not book-keeping, or doing minimal book-keeping and calling run_job multiple times
* Re-writing run_job/run_job_batch to address the aforementioned deficiencies
* Creating a retry endpoint dedicated to addressing book-keeping and job launching features

## Decision Outcome

Pending

* Link to spec file with inputs and outputs for the retry endpoint goes here
* Link to Jira Ticket with business logic documentation for success and cancel cases

### Possible additional things to think about
* Creating sets of objects and what to do at the end of a batch run
* What to do about a set if a child task fails during processing 
* Convenience endpoints that operate on the parent_job_id or list of child job ids may be out of scope (e.g. cancel all jobs with a certain status)

## Consequences

Pending decision

## Pros and Cons of the Alternatives

### Not book-keeping, or doing minimal book-keeping and calling `run_job` multiple times
* `+` Can re-use existing endpoints without any additional work
* `+` Less api endpoints to manage
* `-` Issues on re-rendering/regenerating a cell based on just the job record
* `-` Loss of information about job runs, and ability to infer relationships between parents and child jobs. 
* `-` Loss of control of jobs, such as the ability to restrict a job's running based on number of retries/failures.
* `-` Wrong version of app will run if the app was updated after job completion, and a version tag rather than a git commit was provided
* `-` Increase complexity of `run_job*` methods
* `-` The client will have to keep track of the child_job relationship, so that info is lost once the client is terminated
 
### Re-writing `run_job` to address the aforementioned deficiencies without refactoring
* `+` Solves most requirements, but 
* `-` Adds more complexity to `run_job` methods
* `-` Increase difficulty in maintaining and testing `run_job` method
* `-` Wrong version of app will run if the app was updated after job completion, and a version tag rather than a git commit was provided
* `-` Inefficient job submission
* `-` Possibly Insufficient error handling 

### Re-writing `run_job/run_job_batch` to address the aforementioned deficiencies with some refactoring
* `+` Same as above, but if you are refactoring, you might as well have a retry endpoint, and clean out/decouple `run_job` endpoint from having so many features and branching logic

### Creating `retry` endpoint to address the aforementioned deficiencies with some refactoring
* `+` Decrease coupling between `run_job` and retry functionality, possibly making testing and development easier
* `+` Faster development than a full refactor
* `-` Faster development than a full refactor, but creates technical debt, might have to update both `run_job` and `retry` each time a change is made
* `-` Extra endpoint to manage


### Creating `retry` endpoint to address the aforementioned deficiencies with full refactoring where run_job functions are split out into their own functions
* `+` Decrease coupling between `run_job` and retry functionality, possibly making testing and development easier
* `+` Increase DRYNESS of the code
* `+` Allows retry to benefit from changes to `run_job` 
* `-` Slower development for a full refactor, but decreases technical debt
