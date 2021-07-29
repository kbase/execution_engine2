from db.models.models import TerminatedCode
from execution_engine2.db.MongoUtil import MongoUtil
from apscheduler.schedulers.gevent import GeventScheduler
from installed_clients.execution_engine2Client import execution_engine2 as ee2
from lib.execution_engine2.utils.SlackUtils import SlackClient
from time import sleep


class JobReaper:
    def __init__(self, mongoutil: MongoUtil, ee2_client: ee2, slack_client: SlackClient):
        self.mongoutil = mongoutil
        self.ee2_client = ee2_client
        self.slack_client = slack_client

    def _reap(self, job_id):
        cjp = {
            "as_admin": True,
            "job_id": job_id,
            "terminated_code": TerminatedCode.terminated_by_automation.value,
        }
        self.ee2_client.cancel_job(params=cjp)
        self.slack_client.cancel_job_message(
            job_id=job_id,
            scheduler_id='todo',
            termination_code=TerminatedCode.terminated_by_automation.value,
        )
        # Avoid rate limit of 1 msg per second
        sleep(1)

    def get_aps_connection(self):
        return self.mongoutil.get_aps_connection()

    def cancel_created_jobs(self):
        """
        For jobs that are not batch jobs, and have been in the created state for more than 5 minutes, uh oh, spaghettio, time to go
        """
        stuck_jobs = self.mongoutil.find_old_created_jobs()
        for job_id in stuck_jobs:
            self._reap(job_id)
        return stuck_jobs

    def cancel_queued_jobs(self):
        """
        For jobs that are not batch jobs, and have been in the queued state for more than 14 days, uh oh, spaghettio, time to go
        """
        stuck_jobs = self.mongoutil.find_old_queued_jobs()
        for job_id in stuck_jobs:
            self._reap(job_id)
        return stuck_jobs


def schedule_bad_jobstate_reaping(jr: JobReaper):
    schd = GeventScheduler()
    jobstores = {
        'mongo': jr.get_aps_connection()
    }
    job_defaults = {
        'coalesce': True,
        'max_instances': 1
    }
    schd.configure(jobstores=jobstores, job_defaults=job_defaults)
    schd.add_job(jr.cancel_created_jobs, 'interval', seconds=600, id='cancel_created_jobs', max_instances=1)
    schd.add_job(jr.cancel_queued_jobs, 'interval', seconds=600, id='cancel_queued_jobs', max_instances=1)
    schd.start(paused=True)
    return schd
