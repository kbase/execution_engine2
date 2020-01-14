import slack


class SlackClient:
    def __init__(self, token, channel="#execution_engine_notifications"):
        if token is None:
            raise Exception("Please set add slack token to deploy.cfg")
        self.client = slack.WebClient(token=token)
        self.channel = channel

    def run_job_message(self, job_id, scheduler_id, username):
        message = (
            f"{username} has submitted job_id:{job_id} scheduler_id:{scheduler_id}"
        )
        self.client.client.chat_postMessage(channel=self.channel, text=message)

    def cancel_job_message(self, job_id, scheduler_id, termination_code):
        message = f"scheduler_id:{scheduler_id} job_id:{job_id} has been canceled due to {termination_code}"
        self.client.client.chat_postMessage(channel=self.channel, text=message)

    def finish_job_message(self, job_id, scheduler_id, finish_status, error_code=None):
        message = f"scheduler_id:{scheduler_id} job_id:{job_id} has ended with a status of {finish_status}"
        if error_code is not None:
            message += f" Error code is {error_code}"
        self.client.client.chat_postMessage(channel=self.channel, text=message)
