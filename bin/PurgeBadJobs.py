#!/usr/bin/env python3
import logging
import os
import sys
from configparser import ConfigParser

# I wish a knew a better way to do this
sys.path.append(".")

from lib.execution_engine2.utils.SlackUtils import SlackClient
from datetime import datetime, timedelta
from lib.installed_clients.execution_engine2Client import execution_engine2
from lib.execution_engine2.db.models.models import TerminatedCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config = ConfigParser()
config.read_file(f=os.environ["KB_DEPLOYMENT_CONFIG"])
ee2_endpoint = config.get(section="execution_engine2", option="ee2-url")
slack_token = config.get(section="execution_engine2", option="slack-token")

ee2 = execution_engine2(url=ee2_endpoint, token=os.environ["EE2_ADMIN_SERVICE_TOKEN"])
slack_client = SlackClient(slack_token, channel="#ee_notifications", debug=True)


def cancel_jobs_stuck_in_queue():
    """
    For jobs over 14 days old, cancel them
    :return:
    """
    queue_threshold_days = 14
    d = datetime.today() - timedelta(days=queue_threshold_days + 1)
    stuck_jobs = db.ee2.find({"status": "queued", "queued": {"$gt": d.timestamp()})
    logger.info(f"Found {len(stuck_jobs)} jobs that were stuck in the queue state over {queue_threshold_days} days")
    for record in stuck_jobs:
        cjp = {'as_admin': True, 'job_id': str(record.id),
               'terminated_code': TerminatedCode.terminated_by_automation.value}
        ee2.cancel_job(params=cjp)
    slack_client.cancel_job_message(job_id=str(record.id),scheduler_id=record.scheduler_id)

def cancel_created():
    """
    For jobs that are not batch jobs, and have been in the created state for more than 5 minutes, uh oh, spagettio, time to go
    """
    created_threshold_minutes = 5
    d = datetime.today().timestamp() - 5
    stuck_jobs = db.ee2.find({"status": "created", "_id": {"$gt": d)
    logger.info(f"Found {len(stuck_jobs)} jobs that were stuck in the created state for over {created_threshold_minutes} mins")
    for record in stuck_jobs:
        cjp = {'as_admin': True, 'job_id': str(record.id),
               'terminated_code': TerminatedCode.terminated_by_automation.value}
        ee2.cancel_job(params=cjp)
    slack_client.cancel_job_message(job_id=str(record.id),scheduler_id=record.scheduler_id)

def clean_retried_jobs():
    """Clean up jobs that couldn't finish the retry lifecycle """

def purge():
    cancel_jobs_stuck_in_queue()
    cancel_created()



if __name__ == "__main__":
    try:
        purge()
    except Exception as e:
        slack_client.ee2_reaper_failure(endpoint=ee2_endpoint)
