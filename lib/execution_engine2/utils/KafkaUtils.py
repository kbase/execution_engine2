# -*- coding: utf-8 -*-
"""Module to provide kafka handlers for internal logging facility."""

import json
import logging

logging.basicConfig(level=logging.INFO)
from dataclasses import dataclass

from confluent_kafka import Producer

KAFKA_EVENT_TYPE = "status_update"
DEFAULT_TOPIC = "ee2"


@dataclass
class KafkaStatusUpdate:
    job_id: str = None
    previous_status: str = None
    new_status: str = None
    topic: str = DEFAULT_TOPIC
    event_type: str = KAFKA_EVENT_TYPE
    error: bool = False

    def __post_init__(self):
        if self.job_id is None:
            raise Exception("Need to provide a job id")

        if self.previous_status is None:
            raise Exception("Need to provide a previous_status")

        if self.new_status is None:
            raise Exception("Need to provide a new_status")

@dataclass
class KafkaCondorCommandUpdate(KafkaStatusUpdate):
    scheduler_id: int = None
    job_id: int = None
    requested_condor_deletion: bool = None

    def __post_init__(self):
        if self.job_id is None:
            raise Exception("Need to provide a job id")

        if self.requested_condor_deletion is None:
            raise Exception("Need to provide requested_condor_deletion")




@dataclass
class KafkaStatusUpdateStartJob(KafkaStatusUpdate):
    scheduler_id: int = None

@dataclass
class KafkaStatusUpdateCancelJob(KafkaStatusUpdate):
    terminated_code: int = None
    scheduler_id: int = None

@dataclass
class KafkaStatusUpdateFinishJob(KafkaStatusUpdate):
    scheduler_id: int = None


def _delivery_report(err, msg):
    if err is not None:
        msg = "Message delivery failed:", err
        print(msg)
        logging.error(msg)
    else:
        msg = f"Message delivered to topic '{msg.topic()}': {msg.value()}"
        print(msg)
        logging.error(msg)




def send_kafka_condor_update(data):
    send_message_to_kafka(data=data, data_class=KafkaCondorCommandUpdate)


def send_kafka_update_status(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdate)


def send_kafka_update_start(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateStartJob)


def send_kafka_update_finish(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateFinishJob)


def send_kafka_update_cancel(data):
    send_message_to_kafka(data=data, data_class=KafkaStatusUpdateCancelJob)


def send_message_to_kafka(
        data, data_class, topic=DEFAULT_TOPIC, event_type=KAFKA_EVENT_TYPE, server_address="kafka"
):
    message = data_class(**data)
    producer = Producer({"bootstrap.servers": server_address})
    producer.produce(topic, json.dumps(message.__dict__), callback=_delivery_report)
    producer.poll(2)
