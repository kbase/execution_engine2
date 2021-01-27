# Log into the ee2 container, cd to /condor_shared as the kbase user and run this script
import htcondor

schedd = htcondor.Schedd()
import time

sub = {
    "executable": "/bin/sleep",
    "ShouldTransferFiles": "YES",
    "transfer_input_files": "/condor_shared/JobRunner.tgz",
    "arguments": "10s",  # sleep for 10 seconds
    "output": "sleep-$(ProcId).out",  # output and error for each job, using the $(ProcId) macro
    "error": "sleep-$(ProcId).err",
    "log": "sleep.log",  # we still send all of the HTCondor logs for every job to the same file (not split up!)
    "request_cpus": "1",
    "request_memory": "128MB",
    "request_disk": "128MB",
}

sleep_job = htcondor.Submit(sub)

submit_batch_modifier = []
for i in range(100):
    submit_batch_modifier.append(sub)

start = time.time()
print("Start", start)
with schedd.transaction() as txn:
    cluster_id = sleep_job.queue_with_itemdata(txn, 1, iter(submit_batch_modifier))

stop = time.time()
print("id", cluster_id)
print(cluster_id)
print("stop", stop)
print("duration", stop - start)
