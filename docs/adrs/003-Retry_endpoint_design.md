# Retry Endpoint

Date: 2021-05-19


## Motivation for the Endpoint:

The current requirement for the Batch/Bulk UI is to be able to retry jobs that have either "errored" out, or were cancelled.
The UI allows you to retry either single jobs, or multiple jobs, and saves you from having to cancel and resubmit each job individually,
which is not really possibly with the UI anyway.

### Motivation for the `code spike` for retry endpoint and follow up design ADR
```
As I mentioned, as the product owner, I find our ability to deliver functionality to be pretty awful. 
We have invested so much effort in refactoring that its killed our timeline - we started in late July, and it is now almost May with no functioning bulk uploader, which was just the first deliverable.
If we are going to refactor, we need to be able to do it in a timely fashion, and have it not kill the schedule any more than it has.
I want to see the estimate for a quick and dirty solution that implements a proposed retry endpoint, that can be deployed ASAP, and then once the API contract has been established, and the functional MVP is done, we begin the cleanup of the backend code.
Note that this is NOT business as usual, the usual way we do this is the nasty MVP gets deployed and then we don't go back until much later.
Here, we get the API working so that it doesn't block dependencies, and we immediately start the refactoring. The refactor needs to be broken down into smallish chunks of ~3 days estimated work, and each merge should maintain functionality and incrementally improve the codebase.
Tasks that take more than a couple of days are more likely to be far off in their estimate and this is how we mitigate the risk of poor estimation.
```


### High Level Behavior of the `retry` endpoint
The current implementation of retry is to run jobs using the `retry_job` or `retry_jobs` endpoint. 
The endpoint takes a job or list of job ids and then attempts to resubmit them to the queue, using the exact same set of parameters and version of the app.

### Current Behavior
* Spec file is located at https://github.com/kbase/execution_engine2/blob/8baab8e3ac5212f4bbe59fd935980aa41b4ee06d/execution_engine2.spec#L201-L247
  
* A job id is provided. If there are sufficient permissions, the call will proceed, if not, it will error out, unless the `as_admin` flag is provided by an admin
* The retry will only continue if the status of the job to be retried is in [Status.terminated.value, Status.error.value]
* If the job id points to a job that has already been retried, it will attempt to retry that job's `retry_parent` instead.
* If the job id has never been retried, it becomes the `retry_parent` 
* EE2 looks up the job versions and parameters, and then submits the job to be retried, incrementing the `retry_count`
  of the job being retried, and the newly launched job gains a pointer to the `_PARENT_RETRY_JOB_ID`
* The job is submitted and upon successful submission, notifies the `retry_parent` and notifies the `parent_job_id` that a new `child_job` has been added


### Batch Behavior
* If a job has the attribute of `batch_job=True` the retry will fail, since there is no method to re-run. This is a bug, as it doesn't fail gracefully.
* If a job has the attribute of `batch_job=True`, but is actually a child job, the parent will be notified of this new retried job
* Multiple in-flight retries are allowed.

## Retry_job behavior
* Blocking and single submit to HTCondor. It should be fine
  
## Retry_jobs behavior
* Submitting multiple jobs uses the `run_job` endpoint, and is blocking (NOT OK!)

### Desired Behavior
* Prevent multiple in-flight retries to prevent the user from wasting their own resources (and the queues resources)
* Add retry_count to retried jobs as well to aid in more book-keeping in a new field called `retry_number`
* Non blocking job submission for submitting multiple jobs, possibly via using `run_job_batch` (requires refactor of run_job_batch)
* One single submission to HTCondor instead of multiple job submissions
* Ability to gracefully handle jobs with children
* Ability to handle database consistentcy during retry failure


### Questions

#To Be Answered

#### Q: should the number of retries of a job be limited, and if so, where? e.g. a max_retries field in the parent job? wait and see whether people attempt to rerun jobs that have already failed nine zillion times?
A: Unknown TBD

#### Q: Preventing the same params from being re-run
A: We have decided to allow it

#### Q: Finding the most recent run of the job: I would very much like to avoid anything involving iterating over a chain of jobs before you can find the most recent run or the original run -- we can come up with better data structures than that!
A: Unknown TBD, maybe the frontend does it?

#### Q: It might be best to always submit a git commit for the module, maybe?
A: (This could be a narrative ticket)

#### Q: How do we handle DB consistentcy during retry failure? 



### Sort of answered
#### Q: how to prevent incorrect parent-child relationships being created -- should the client be allowed to specify a parent ID? Is it currently possible to add a new child to a parent job if the child is a new job, rather than an existing job ID / set of params that is being rerun?
A: Not necessarily relevant to this endpoint, more of a run_job_batch endpoint question. Currently the `retry_parent` and `parent_job_id` are looked up from the ee2 record on retry, and not specified in this endpoint.

#### Answered:

    Should we track a retry count? (Done)
    Should users see this retry count? (Unknown TBD)
    Are retried jobs saved in some sort of data structure linking them, possibly indirectly, to the parent job or are they orphaned? (Yes, retry_parent)
    If the former, is the retry relationship linear or a tree? E.g. what happens if there are two simultaneous calls to retry a job? (Tree, simultaneous jobs run)
    Should it be at least theoretically possible to see the list of retried jobs in order? (It is possible by sorting on creation date)
    Should there be a maximum retry count? Or a warning that more retries are not likely to help?  (Unknown TBD)
    Can a job in states other than failed or canceled be retried? Or should the user be required to cancel a job before it can be retried? (Job must be in Error/Cancel state)


# Work estimation
Priority descending
* Non blocking job submission for submitting multiple jobs, possibly via using `run_job_batch` (requires refactor of run_job_batch)
* One single submission to HTCondor instead of multiple job submission ()
* Ability to gracefully handle jobs with children (may require refactoring models)
* Prevent multiple in-flight retries to prevent the user from wasting their own resources (and the queues resources)
* Add retry_count to retried jobs as well to aid in more book-keeping in a new field called `retry_number`

# Time / Tickets to be created
* Non blocking job submission for submitting multiple jobs, possibly via using `run_job_batch` (requires refactor of run_job_batch)
> Requires refactor of run_job_batch to add jobs to an existing batch job, and force the same app `git_commit versions` and `JobRequirements`
> Estimate 3-4 days
* One single submission to HTCondor instead of multiple job submission ()
> Dependent on run_job_batch to be threaded first : Estimate 1 day
* Ability to gracefully handle jobs with children
> (may require refactoring models. Especially when children spawn more jobs) : Estimate 3 day
* Prevent multiple in-flight retries to prevent the user from wasting their own resources (and the queues resources)
> Some sort of locking mechanism or something else : Estimate 3 day
* Add retry_count to retried jobs as well to aid in more book-keeping in a new field called `retry_number`
> Requires addition to run_job and new field in model : Estimate 1.25 day
