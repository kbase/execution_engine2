# -*- coding: utf-8 -*-
"""Module to provide kafka handlers for internal logging facility."""

import json
import logging
import sys

from confluent_kafka import Producer, KafkaError, Consumer

_TOPICS = ["ee2"]


def _test_sample_consumer():
    """
    This is for testing
    :return:
    """
    consumer = Consumer(
        {
            "bootstrap.servers": "kafka:9096",
            "group.id": sys.argv[1],
            "auto.offset.reset": "earliest",
        }
    )
    topics = ["ee2"]
    consumer.subscribe(_TOPICS)
    print("Consuming from", topics)
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of the stream.")
            else:
                print(f"Error: {msg.error()}")
                continue
        print(f"New message: {msg.value().decode('utf-8')}")
    consumer.close()


def _delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        print(f"Message delivered to topic '{msg.topic()}': {msg.value()}")


def send_message_to_kafka(data, topic="ee2"):
    required_keys = ["job_id", "new_status", "previous_status"]
    errors = 0
    for item in required_keys:
        if item not in data.keys():
            logging.error(f"You need to include {item} in your kafka message")
            errors += 1
    # if errors > 0:
    #     raise Exception("Malformed kafka message")

    producer = Producer({"bootstrap.servers": "kafka"})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(2)