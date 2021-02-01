# Probably need more comprehensive integration tests, not sure which can be tested in this repo

# Case 1: User submits batch job. All parameters are good. Condor submission is good
# * Endpoint returns list of job ids
# * Jobs get submitted to condor. They start running and either crash or suceed
# User gets notified via the UI on the return of `run_job_batch` that there is an issue


# Case 2: User submits batch job. One or more parameters are bad.
# * Endpoint returns an error ("one or more parameters are bad")
# * No jobs are submitted to condor
# User gets notified via the UI on the return of `run_job_batch` that there is an issue

# Case 3: User submits batch job. All parameters are good.
# * Endpoint returns list of job ids
# * Thread submits jobs. Upon failure, jobs failed submit, does the entire batch abort? #TODO Look into it
# * If the entire batch aborts or some make it through, run a canceljob on all failures and successes.
# Not sure how we notify the user. Do we add logs into each job? Do we do something to the parent job? How do we tell the main cell that something went wrong?

# Case 4: User submits batch job. All parameters are good. But job submission thread dies. Or ee2 dies. Or something else dies
# * Endpoint returns a list of job_ids
# * CreatedJobs Reaper runs...
# * If job is in created state for longer than 5 minutes, has a parent_job, then fetch parent_job, and cancel all child jobs via ee2.cancel_job
# (which calls a condor_rm on each of the jobs as well)
# # Not sure how we notify the user. Do we add logs into each job? Do we do something to the parent job? How do we tell the main cell that something went wrong?
