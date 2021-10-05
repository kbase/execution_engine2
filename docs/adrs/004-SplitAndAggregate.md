# Replace KBParallels with another solution to avoid Deadlocks

Date: 2021-09-22

[Related ADR](https://github.com/kbase/execution_engine2/blob/develop/docs/adrs/rework-batch-analysis-architecture.md)

## Intro
Sometimes a calculation requires too many resources from one node (walltime, memory, disk), so the calculation gets spread across multiple machines.
The final step of the app that uses KBParallels is to create a report. This step may use results from all of the computed jobs to create the final report.
In order to do this, some apps use a mechanism called KBParallel. The apps are listed at [DATAUP-111](https://kbase-jira.atlassian.net/browse/DATAUP-111)

## The current implementation of Batch Analysis in [kb_BatchApp](https://github.com/kbaseapps/kb_BatchApp) at KBase has the following issues:

* Current UI is not adequate: Users shouldn’t have to code in order to run batch analysis. Also it’s difficult to do so, even for those familiar with KBase code (have to find object names)
* Dependency on [KBParallel](https://github.com/kbaseapps/KBParallel): any changes to KBParallel could affect KB Batch and subsequently all other apps. 
* Queue deadlocking: users have a max of 10 slots in the queue, with the current implementation one is taken up just to manage the jobs. Could lead to deadlock scenarios
* Missing the ability to be able to run, manage and track jobs and their subjobs 
* No good way to test and hard to benchmark or measure performance 
* Code is split more than is necessary
* UI doesn't properly display progress of batch jobs

Background:

## Author(s)

@bio-boris, @mrcreosote

## Status

Pending

## Alternatives Considered

* Ignore most issues and just make apps that run kbparallels limited to N instances of kbparallels per user to avoid deadlocks
* Writing new ee2 endpoints to entirely handle batch execution and possibly use a DAG
* Remove kbparallels and change apps to a collection of 2-3 apps that do submit, split and aggregate and an use an ee2 endpoint to create a DAG

## Decision Outcome

For the first pass, we would likely limit the number of kbparallel runs
For the next pass, we would want to create a comprehensive generalized solution to submit,split and aggregate, with recipes or conveniences for common operations for creating sets, reports, or things of that nature.
We would also want to do a user study on what we want from the UI and which functionality we want, as the UI may inform the design of the backend system.

## Consequences

* We will have to roll out fixes in multiple stages
* We will have to implement a new narrative UI, however this was work that would happen regardless due as we are looking to improve the UX for batch upload and analysis at KBase. 
* This will take significant time to further research and engineer the solutions

Still to be determined (not in scope of this ADR): 
* UI and how it relates to the bulk execution
* XSV Analysis and how it relates to the bulk execution

## Pros and Cons of the Alternatives

### Limit multiple instances of kbparallels

* `+` Simplest solution, quickest turnaround, fixes deadlock issue
* `-` UI still broken for batch analysis

## Seperate Queue for kbparallels apps

### Deprecate kbparallels, and write new ee2 endpoints to entirely handle batch execution and possibly use a DAG
* `+` No longer uses an app
* `+` Custom endpoint
* `+` No longer uses a slot in the queue
* `+` Can manage jobs more closely: have tables, more control over lifecycle, resource requests, canceling subjobs
* `-` Still deadlocks and uses a slot in the queue

### Implementing a standalone Narrative Widget
* `+` No longer uses an app
* `+` No longer uses a slot in the queue
* `+` Can reuse FE UI logic to build widget / cell, drive the batch submission process, and keep track of job progresses
* `-` Have to maintain new cell type (possible code duplication), adds extra load to eventual narrative refactor
* `-` Tightly coupled to Narrative UI means that if we want to run batch outside of narrative or in any other capacity work will have to be duplicated
* `-` Job resource and queue management has to happen in the narrative 
* `-` If the narrative is closed or reaped, job state management stops

### Implementing a new (set) of EE2 endpoint(s)
* `+` No longer uses a slot in the queue, deadlock is no longer an issue 
* `+` No longer uses an app: more logical paradigm, services are easier to test, scale, monitor
* `+` More control over job lifecycle: management, tables, resource requests, subjob cancellation 
* `+` Possible to control job priority in queue
  * Batch jobs could have lower priority, so individual jobs could also run concurrently. E.g. A user could start a batch of 1000 imports, and still run some analysis task that would be able to start and finish first.
  * There’s a possibility for users (and/or admins) to control job priority in queue after submission
* [Code cohesion](https://en.wikipedia.org/wiki/Cohesion_(computer_science))
