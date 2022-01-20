#!/usr/bin/env python3
# Script to purge jobs that have been queued for too long, or stuck in the created state for too long

import logging
import os
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from time import sleep

import pymongo
from bson import ObjectId

from lib.execution_engine2.db.models.models import TerminatedCode, Status
from lib.execution_engine2.utils.SlackUtils import SlackClient
from lib.installed_clients.execution_engine2Client import execution_engine2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config = ConfigParser()
config.read(os.environ["KB_DEPLOYMENT_CONFIG"])
ee2_endpoint = config.get(section="execution_engine2", option="ee2-url")
slack_token = config.get(section="execution_engine2", option="slack-token")

ee2 = execution_engine2(url=ee2_endpoint, token=os.environ["EE2_ADMIN_SERVICE_TOKEN"])
slack_client = SlackClient(
    slack_token, channel="#ee_notifications", debug=True, endpoint=ee2_endpoint
)
db_client = pymongo.MongoClient(
    host=config.get(section="execution_engine2", option="mongo-host"),
    port=int(config.get(section="execution_engine2", option="mongo-port")),
    username=config.get(section="execution_engine2", option="mongo-user"),
    password=config.get(section="execution_engine2", option="mongo-password"),
    authSource=config.get(section="execution_engine2", option="mongo-database"),
    authMechanism=config.get(section="execution_engine2", option="mongo-authmechanism"),
    serverSelectionTimeoutMS=1000,
)
ee2_db = db_client.get_database(
    config.get(section="execution_engine2", option="mongo-database")
)
ee2_jobs_collection = ee2_db.get_collection(
    config.get(section="execution_engine2", option="mongo-jobs-collection")
)

CREATED_MINUTES_AGO = 5
QUEUE_THRESHOLD_DAYS = 14
RUNNING_THRESHOLD_DAYS = 8


def cancel(record):
    job_id = str(record["_id"])
    scheduler_id = record.get("scheduler_id")
    cjp = {
        "as_admin": True,
        "job_id": job_id,
        "terminated_code": TerminatedCode.terminated_by_automation.value,
    }
    print("About to cancel ee2 job", cjp)
    ee2.cancel_job(params=cjp)
    slack_client.cancel_job_message(
        job_id=job_id,
        scheduler_id=scheduler_id,
        termination_code=TerminatedCode.terminated_by_automation.value,
    )
    # Avoid rate limit of 1 msg per second
    sleep(1)


def cancel_jobs_stuck_in_state(threshold_days, state):
    before_days = (datetime.today() - timedelta(days=threshold_days + 1)).timestamp()
    print({"status": state, state: {"$lt": before_days}})
    stuck_jobs = ee2_jobs_collection.find(
        {"status": state, state: {"$lt": before_days}}
    )
    print(
        f"Found {stuck_jobs.count()} jobs that were stuck in the {state} state over {threshold_days} days"
    )
    for record in stuck_jobs:
        queued_time = record[state]
        now = datetime.now(timezone.utc).timestamp()
        elapsed = now - queued_time
        print(f"{state} days=", elapsed / 86000)
        cancel(record)


def cancel_jobs_stuck_in_running():
    """
    For running jobs over 8 days old, cancel them
    Update a completed Job as necessary to test this out:
    ee2.update_job_status({'job_id': '601af2afeeb773acaf9de80d', 'as_admin': True, 'status': 'queued'})
    :return:
    """
    cancel_jobs_stuck_in_state(
        threshold_days=RUNNING_THRESHOLD_DAYS, state=Status.running.value
    )


def cancel_jobs_stuck_in_queue():
    """
    For queued jobs over 14 days old, cancel them
    Update a completed Job as necessary to test this out:
    ee2.update_job_status({'job_id': '601af2afeeb773acaf9de80d', 'as_admin': True, 'status': 'running'})
    :return:
    """
    cancel_jobs_stuck_in_state(
        threshold_days=QUEUE_THRESHOLD_DAYS, state=Status.queued.value
    )


def cancel_created():
    """
    For jobs that are not batch jobs, and have been in the created state for more than 5 minutes, uh oh, spaghettio, time to go
    """

    five_mins_ago = ObjectId.from_datetime(
        datetime.now(timezone.utc) - timedelta(minutes=CREATED_MINUTES_AGO)
    )
    stuck_jobs = ee2_jobs_collection.find(
        {"status": "created", "_id": {"$lt": five_mins_ago}, "batch_job": {"$ne": True}}
    )
    print(
        f"Found {stuck_jobs.count()} jobs that were stuck in the {Status.created.value} state for over 5 mins"
    )
    for record in stuck_jobs:
        cancel(record)


def clean_retried_jobs():
    """Clean up jobs that couldn't finish the retry lifecycle"""
    # TODO


def purge():
    cancel_jobs_stuck_in_queue()
    # Use this after an outage
    # cancel_jobs_stuck_in_running()
    cancel_created()


if __name__ == "__main__":
    try:
        purge()
    except Exception as e:
        slack_client.ee2_reaper_failure(endpoint=ee2_endpoint, e=e)
        raise e
