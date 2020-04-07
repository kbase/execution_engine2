#!/usr/bin/env python3

import logging
import os
import sys
import time
from configparser import ConfigParser
from pathlib import Path

import htcondor

try:
    from lib.execution_engine2.utils.SlackUtils import SlackClient
    from lib.installed_clients.execution_engine2Client import execution_engine2
    from lib.execution_engine2.utils.Condor import Condor
except Exception:
    from SlackUtils import SlackClient
    from execution_engine2Client import execution_engine2
    from Condor import Condor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config = ConfigParser()
config_filepath = os.environ["KB_DEPLOYMENT_CONFIG"]

# Condor
condor = Condor(config_filepath=config_filepath)
# EE2
cfg = condor.config["ee2-url"]
ee2_endpoint = cfg["ee2-url"]
ee2 = execution_engine2(url=ee2_endpoint, token=os.environ["EE2_ADMIN_SERVICE_TOKEN"])
# Slack
slack_token = cfg["slack-token"]
slack_client = SlackClient(
    cfg.get("slack-token"), channel="#ee_notifications", debug=True
)


def read_events(path):
    """Produce events from a schedd event log forever, blocking until they appear."""
    yield from htcondor.JobEventLog(path.as_posix()).events(None)


def process_events(events, callbacks=None, skip_through=None):
    """
    Consumes job events from an iterator and runs handlers on them.
    Parameters
    ----------
    events
        An iterator of events, as produced by read_event().
    callbacks
        A dictionary mapping htcondor.JobEventType values to handler functions.
        Handler functions should have signature handler(event).
        Their return values are not used.
    skip_through
        The latest timestamp that we have already seen.
        Events with smaller timestamps will be skipped.
        Skipped events will not have handlers called on them.
    """
    if callbacks is None:
        callbacks = {}

    event_timestamp = None
    for event in events:
        event_id = f"{event.type} for {event.cluster}.{event.proc} with timestamp {event.timestamp}"
        logger.debug(f"Processing event {event_id}")
        event_timestamp = event.timestamp
        if skip_through is not None and event.timestamp <= skip_through:
            logger.debug(
                f"Skipping event {event_id} because skipping through event timestamp {skip_through}"
            )
            continue

        handler = callbacks.get(event.type, None)
        if handler is not None:
            logger.debug(f"Calling handler {handler} on event {event_id}")
            handler(event)
        else:
            logger.debug(f"No handler found for event {event_id}")
    return event_timestamp


def get_base_json(event):
    """Get a dictionary populated with basic event information."""
    return {
        "timestamp": event.timestamp,
        "event_type": str(event.type),
        "cluster": event.cluster,
        "proc": event.proc,
    }


def handle_hold_event(event):
    j = get_base_json(event)

    # stuff some extra information into the JSON
    # j["hold_reason_code"] = int(event["HoldReasonCode"])
    # j["hold_reason"] = event.get("HoldReason", "UNKNOWN").strip()

    # time.+(1)
    job_id = j["cluster"]
    if int(event["HoldReasonCode"]) != 16:
        print(f"JSON for job id hold event: {j} {event}")
        try:
            new_job_record = ee2.handle_held_job(cluster_id=job_id)
            print(f"This job was held because of {new_job_record}")
        # slack_client.ee2_reaper_success(job_id=job_id,
        #                                 calculated_hold_reason=calculated_hold_reason)
        except Exception as e:
            print(e)
            pass
        # failure info         job_info = condor.get_job_info(cluster_id=job_id)
        # slack_client.ee2_reaper_failure(endpoint=ee2_endpoint, job_id=job_id)
        # sys.exit(f"{e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    event_log_fp = Path("/usr/local/condor/log/condor/event_log")
    last_timestamp = None
    while True:
        try:
            last_timestamp = process_events(
                events=read_events((event_log_fp)),
                callbacks={htcondor.JobEventType.JOB_HELD: handle_hold_event},
                skip_through=last_timestamp,
            )
            time.sleep(5)
        except Exception as e:
            slack_client.ee2_reaper_failure(endpoint=cfg.get("ee2-url"))
            sys.exit(f"{e}")
