#!/usr/bin/env python
# type: ignore
from collections import defaultdict

from bson import ObjectId

try:
    from .transfer_ujs_njs import MigrateDatabases
except Exception:
    from transfer_ujs_njs import MigrateDatabases

njs_jobs_db = MigrateDatabases().njs_jobs
ujs_jobs_db = MigrateDatabases().ujs_jobs

count = 0
missing_ujs = []
c = defaultdict(int)
for job in njs_jobs_db.find():
    job_id = job["ujs_job_id"]
    c[job_id] += 1
    count += 1
    ujs_job = ujs_jobs_db.find_one({"_id": ObjectId(job_id)})
    if not ujs_job:
        print(f"Couldn't find {job_id}, ")
        missing_ujs.append(job_id)

print("Max occurences", max(c.values()))

print("Number of njs jobs", count)
print("Number of ujs jobs found", count - len(missing_ujs))
print("Number of missing ujs jobs", len(missing_ujs))
