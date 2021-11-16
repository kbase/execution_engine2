# Replace KBParallels with another solution to avoid Deadlocks

Date: 2021-09-22

[Related ADR](https://github.com/kbase/execution_engine2/blob/develop/docs/adrs/rework-batch-analysis-architecture.md)

## Note
This ADR is more of a place to keep the current discussions we had at https://docs.google.com/document/d/1AWjayMoqCoGkpO9-tjXxEvO40yYnFtcECbdne5vTURo
Rather than to make a decision. There is still more planning, scoping and testing involved before we can fully design this system.

Still to be determined (not in scope of this ADR):
* UI and how it relates to the bulk execution
* XSV Analysis and how it relates to the bulk execution


## Intro
Sometimes a calculation requires too many resources from one node (walltime, memory, disk), so the calculation gets spread across multiple machines.
The final step of the app that uses KBParallels is to create a report. This step may use results from all of the computed jobs to create the final report.
In order to do this, the following apps use a mechanism called KBParallel
* kb_Bowtie2
* refseq_importer
* kb_concoct
* kb_phylogenomics
* kb_hisat2
* kb_meta_decoder
* kb_Bwa

## The current implementation of Batch Analysis in [kb_BatchApp](https://github.com/kbaseapps/kb_BatchApp) at KBase has the following issues:

* Current UI is not adequate: Users shouldn’t have to code in order to run batch analysis. Also it’s difficult to do so, even for those familiar with KBase code (have to find object names)
* Dependency on [KBParallel](https://github.com/kbaseapps/KBParallel): any changes to KBParallel could affect KB Batch and subsequently all other apps. 
* Queue deadlocking: users have a max of 10 slots in the queue, with the current implementation one management job is created to manage the jobs that it submits. This could lead to deadlock scenarios, as there can be 10 management jobs waiting to submit computation jobs, but they cannot, as there all slots are being used up.
* KBP can spawn other KBP jobs. Batch jobs can spawn other batch jobs. 
* Missing the ability to be able to run, manage (cancel) and track jobs and their subjobs along with the ability to specify resources differently between the main and sub jobs
* No good way to test and hard to benchmark or measure performance 
* Code is split more than is necessary
* UI doesn't properly display progress of batch jobs

## Author(s)

@bio-boris, @mrcreosote

## Status
Needs more planning, but current ideas are documented here


## Decision Outcome (pending more research to iron out more details)

For the first pass, we would likely limit the number of kbparallel runs.

For the next pass, we would want to create a comprehensive generalized solution to submit,split and aggregate, with recipes or conveniences for common operations for creating sets, reports, or things of that nature.

We would also want to do a user study on what we want from the UI and which functionality we want, as the UI may inform the design of the backend system.


### Deprecate KBP and instead break out apps into 3 parts

* Fan out (FO)
* Process in parallel (PIP)
* Fan in (FI)


### Steps:
1. User launches job as normal
2. Possibly the job is marked as a FO job, Makes it easier for the UI to display the job correctly initially, Ideally would be marked in the spec, but this might be a lot of work Could potentially be marked in the catalog UI (e.g. along with the job requirements) 
3. Job figures out what the PIP / sub jobs should be
4. Job sends the following info to EE2
* Its own job ID
* The parameters for each of the sub jobs
* The app of the FI job, e.g. kb_phylogenomics/build_microbial_speciestree_reduce
* EE2 starts the subjobs and associates them with with FO job (Probably need retry handling for the subjobs to deal with transient errors)
5. Whenever a subjob finishes, EE2 checks to see if all the subjobs are finished
* If true, EE2 starts the FI job, providing the outputs of the subjobs as a list to the reduce job
* When the FI job is finished, the job is done.
* The various jobs can communicate by storing temporary data in the caching service or in the Blobstore. If the latter is used, the FI job should clean up the Blobstore nodes when its complete.
* Could make a helper app for this?
* What about workflow engines (WDL, Cromwell)? Are we reinventing the wheel here?
* Can new EE2 endpoints speed up or reduce the complexity of any of these steps?

### Notes about DAG in ee2 Endpoints
```
Your dag would need to have (at least) a first job followed by a SUBDAG EXTERNAL.
Somewhere in the first job you'd generate a new dag workflow that
defines the N clusters followed by the N+1 job, which runs in the
subdag.

As for DAGMan support in the Python bindings, we do this in the
following two ways:

1) There is a htcondor.Submit.from_dag() option which takes the name
of a dag filename. You then submit the resulting object just like any
regular job.
2) We have a htcondor.dags library which can be used to
programmatically construct a DAG workflow in computer memory, then
write to a .dag file and submit using the function mentioned in 1)
above.
```

Between these there are several different ways to do what you want.

There's a useful example here that shows the general workflow in the
bindings: https://htcondor.readthedocs.io/en/latest/apis/python-bindings/tutorials/DAG-Creation-And-Submission.html#Describing-the-DAG-using-htcondor.dags

## Consequences

* We will have to roll out fixes in multiple stages
* We will have to implement a new narrative UI, however this was work that would happen regardless due as we are looking to improve the UX for batch upload and analysis at KBase. 
* This will take significant time to further research and engineer the solutions

Still to be determined (not in scope of this ADR): 
* UI and how it relates to the bulk execution
* XSV Analysis and how it relates to the bulk execution

## Alternatives Considered

* Ignore most issues and just make apps that run kbparallels limited to N instances of kbparallels per user to avoid deadlocks
* Writing new ee2 endpoints to entirely handle batch execution and possibly use a DAG
* Remove kbparallels and change apps to a collection of 2-3 apps that do submit, split and aggregate and an use an ee2 endpoint to create a DAG
* Different DevOps solutions

## Pros and Cons of the Alternatives

### Limit multiple instances of kbparallels

* `+` Simplest solution, quickest turnaround, fixes deadlock issue
* `-` Addresses only the deadlocking issue, UI still broken for regular runs and batch runs 

### Increase number of slots per user > 10
* `+` Simple solutions, quick turnarounds, fixes deadlock issue for small numbers of jobs.
* `-` Doesn't fix deadlock issue as the user can still submit more KBP jobs
* `-` Addresses only the deadlocking issue, UI still broken for regular runs and batch runs
* `-` A small amount of users can take over the entire system
* `-` The calculations done by the apps will interfere with other apps and cause crashes/failures

###  Seperate Queue for kbparallels apps that may or may not have its own limit to running jobs.
* `+` Simple solutions, quick turnarounds, fixes deadlock issue 
* `+` Requires minimum changes to ee2 and condor if condor supports this feature
* `-` Addresses only the deadlocking issue, UI still broken for regular runs and batch runs
* `-` A small amount of users can take over the new  queue unless the new queue has its own limit to running jobs then it prevents users from taking over.
* `-` The calculations done by the apps will interfere with other apps and cause crashes/failures


### Modify KBP to do only local submission, Move the job to a machine with larger resources
* `+` Simple solutions, quick turnarounds, fixes deadlock issue, fixes UI issues
* `-` We have a limited number of larger resources machines
* `-` Continued dependency on deprecated KBP tools

### Deprecate kbparallels, and write new ee2 endpoints to entirely handle split and aggregate
* `+` No longer uses an app, No longer uses a slot in the queue
* `-` All jobs would have to change to stop using KBP in favor of these via the ee2 client,
* `+` Can manage jobs more closely: have tables, more control over lifecycle, resource requests, canceling subjobs
* `-` Job monitoring thread would have to run in ee2
* `-`  Requires new data structures and models in ee2 to make sure that the relationship is preserved between jobs and sub jobs, and to make sure deadlocking does not occur
* `-` Requires storing the job outside of condor to prevent job submission, unless we can mark jobs as runnable or not via the HTCondor API* 
* `-` The endpoint would need to know how to split up input files and aggregate them
