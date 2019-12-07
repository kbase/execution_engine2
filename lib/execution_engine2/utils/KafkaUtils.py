# -*- coding: utf-8 -*-
"""Module to provide kafka handlers for internal logging facility."""

import json
import logging

from execution_engine2.db.models.models import Status

logging.basicConfig(level=logging.INFO)
from dataclasses import dataclass

from confluent_kafka import Producer

KAFKA_EVENT_TYPE = "job_status_update"
CONDOR_EVENT_TYPE = "condor_request"
DEFAULT_TOPIC = "ee2"


def send_kafka_update_status(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdate)


def send_kafka_condor_update(data):
    send_message_to_kafka(data=data, data_class=KafkaCondorCommandUpdate)


def send_kafka_update_start(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateStartJob)


def send_kafka_update_finish(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateFinishJob)


def send_kafka_update_cancel(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateCancelJob)


@dataclass
class KafkaStatusUpdate:
    job_id: str = None
    previous_status: str = None
    new_status: str = None
    scheduler_id: int = None
    topic: str = DEFAULT_TOPIC
    event_type: str = KAFKA_EVENT_TYPE
    error: bool = False

    def __post_init__(self):
        if self.job_id is None:
            raise Exception("Need to provide a job id")

        if self.new_status is None:
            raise Exception("Need to provide a new_status")

        # All state transitions need a previous status, except the created one
        if self.previous_status is None:
            if self.new_status != Status.created.value:
                raise Exception("Need to provide a previous_status")

        # A created job may not have been able to have been submitted to condor, so it may not have a scheduler_id
        if self.new_status not in ["created", "terminated", "canceled"]:
            if self.scheduler_id is None:
                message = f"You must pass a scheduler id once the job has been created already. Job status is {self.new_status}"
                logging.error(message)
                raise Exception(message)


@dataclass
class KafkaCondorCommandUpdate(KafkaStatusUpdate):
    job_id: int = None
    requested_condor_deletion: bool = None
    event_type: str = CONDOR_EVENT_TYPE

    def __post_init__(self):
        if self.job_id is None:
            raise Exception("Need to provide a job id")

        if self.requested_condor_deletion is None:
            raise Exception("Need to provide requested_condor_deletion")


@dataclass
class KafkaStatusUpdateStartJob(KafkaStatusUpdate):
    def __post_init__(self):
        allowed_states = [
            Status.created.value,
            Status.queued.value,
            Status.estimating.value,
        ]
        if self.previous_status not in allowed_states:
            raise Exception(
                f"Invalid previous status ({self.previous_status}), it should have been in {allowed_states}.  "
            )

        if self.new_status not in [
            Status.queued.value,
            Status.estimating.value,
            Status.running.value,
            Status.created.value,
        ]:
            raise Exception(f"Invalid new state {self.new_status}")

        if self.previous_status == self.new_status:
            if self.previous_status != Status.created.value:
                raise Exception(
                    f"State not updated prev:{self.previous_status} new:{self.new_status}"
                )


@dataclass
class KafkaStatusUpdateCancelJob(KafkaStatusUpdate):
    terminated_code: int = None

    def __post_init__(self):
        if self.terminated_code is None:
            raise Exception("Need to provide a termination reason code")


@dataclass
class KafkaStatusUpdateFinishJob(KafkaStatusUpdate):
    error_message: str = None
    error_code: int = None

    def __post_init__(self):
        if self.new_status is not Status.completed.value:
            if self.error_message is None:
                raise Exception("Need to provide error_msg for unsuccessful jobs")
            if self.error_code is None:
                raise Exception("Need to provide error_code for unsuccessful jobs")


def _delivery_report(err, msg):
    if err is not None:
        msg = "Message delivery failed:", err
        logging.error(msg)


def send_message_to_kafka(
    data, data_class, topic=DEFAULT_TOPIC, server_address="kafka"
):
    message = data_class(**data)
    producer = Producer({"bootstrap.servers": server_address})
    producer.produce(topic, json.dumps(message.__dict__), callback=_delivery_report)
    producer.poll(2)
