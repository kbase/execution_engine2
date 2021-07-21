#!/usr/bin/env python3
# Script to purge jobs that have been queued for too long, or stuck in the created state for too long

import logging
import os
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
import pymongo
from time import sleep as _sleep

# Must be run in /kb/module
# I wish a knew a better way to do this
sys.path.append(".")

from lib.execution_engine2.utils.SlackUtils import SlackClient
from lib.installed_clients.execution_engine2Client import execution_engine2
from lib.execution_engine2.db.models.models import TerminatedCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config = ConfigParser()
config.read(os.environ["KB_DEPLOYMENT_CONFIG"])
ee2_endpoint = config.get(section="execution_engine2", option="ee2-url")
slack_token = config.get(section="execution_engine2", option="slack-token")

ee2 = execution_engine2(url=ee2_endpoint, token=os.environ["EE2_ADMIN_SERVICE_TOKEN"])
slack_client = SlackClient(slack_token, channel="#ee_notifications", debug=True, endpoint=ee2_endpoint)
db_client = pymongo.MongoClient(
    host=config.get(section="execution_engine2", option="mongo-host"),
    port=int(config.get(section="execution_engine2", option="mongo-port")),
    username=config.get(section="execution_engine2", option="mongo-user"),
    password=config.get(section="execution_engine2", option="mongo-password"),
    authSource=config.get(section="execution_engine2", option="mongo-database"),
    authMechanism=config.get(section="execution_engine2", option="mongo-authmechanism"),
    serverSelectionTimeoutMS=1000,
)
ee2_db = db_client.get_database(config.get(section="execution_engine2", option="mongo-database"))
ee2_jobs_collection = ee2_db.get_collection(config.get(section="execution_engine2", option="mongo-jobs-collection"))


def cancel_jobs_stuck_in_queue():
    """
    For jobs over 14 days old, cancel them
    :return:
    """
    queue_threshold_days = 14
    before_days = (datetime.today() - timedelta(days=queue_threshold_days + 1)).timestamp()
    print({"status": "queued", "queued": {"$lt": before_days}})
    stuck_jobs = ee2_jobs_collection.find({"status": "queued", "queued": {"$lt": before_days}})
    print(f"Found {stuck_jobs.count()} jobs that were stuck in the queue state over {queue_threshold_days} days")
    for record in stuck_jobs:
        job_id = str(record['_id'])
        scheduler_id = record.get('scheduler_id')
        queued_time = record['queued']
        cjp = {'as_admin': True, 'job_id': job_id,
               'terminated_code': TerminatedCode.terminated_by_automation.value}
        now = datetime.now(timezone.utc).timestamp()
        elapsed = now - queued_time
        print("queued days=", elapsed / 86000)
        print("About to cancel job", cjp)
        ee2.cancel_job(params=cjp)
        slack_client.cancel_job_message(job_id=job_id, scheduler_id=scheduler_id, termination_code=TerminatedCode.terminated_by_automation.value)
        # Avoid rate limit of 1 msg per second
        _sleep(1)

def cancel_created():
    """
    For jobs that are not batch jobs, and have been in the created state for more than 5 minutes, uh oh, spagettio, time to go
    """
    created_threshold_minutes = 5
    d = datetime.today().timestamp() - 5
    stuck_jobs = db.ee2.find({"status": "created", "_id": {"$gt": d)
    logger.info(
        f"Found {len(stuck_jobs)} jobs that were stuck in the created state for over {created_threshold_minutes} mins")
    for record in stuck_jobs:
        cjp = {'as_admin': True, 'job_id': str(record.id),
               'terminated_code': TerminatedCode.terminated_by_automation.value}
    ee2.cancel_job(params=cjp)
    slack_client.cancel_job_message(job_id=str(record.id), scheduler_id=record.scheduler_id)


def clean_retried_jobs():
    """Clean up jobs that couldn't finish the retry lifecycle """


def purge():
    cancel_jobs_stuck_in_queue()
    # cancel_created()


if __name__ == "__main__":
    try:
        cancel_jobs_stuck_in_queue()
        cancel_created()
        ee2.update_job_status({'job_id' : '601af2afeeb773acaf9de80d', 'as_admin' : True, 'status': 'queued'})
    except Exception as e:
        slack_client.ee2_reaper_failure(endpoint=ee2_endpoint)
        raise e
