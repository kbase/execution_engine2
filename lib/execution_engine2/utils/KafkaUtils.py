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
    job_id: str
    previous_status: str
    new_status: str
    topic: str = DEFAULT_TOPIC
    event_type: str = KAFKA_EVENT_TYPE
    error: bool = False


def _delivery_report(err, msg):
    if err is not None:
        msg = "Message delivery failed:", err
        print(msg)
        logging.error(msg)
    else:
        msg = f"Message delivered to topic '{msg.topic()}': {msg.value()}"
        print(msg)
        logging.error(msg)


def send_message_to_kafka(
    data, topic=DEFAULT_TOPIC, event_type=KAFKA_EVENT_TYPE, server_address="kafka"
):

    message = KafkaStatusUpdate(**data)
    producer = Producer({"bootstrap.servers": server_address})
    producer.produce(topic, json.dumps(message.__dict__), callback=_delivery_report)
    producer.poll(2)
