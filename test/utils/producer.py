import logging
import json

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
DEFAULT_TOPIC = "ee2"


def _delivery_report(err, msg):
    if err is not None:
        msg = "Message delivery failed:", err
        print(msg)


class KafkaClient:
    def __init__(self, server_address):
        self.server_address = server_address

    def send_kafka_message(self, message, topic=DEFAULT_TOPIC):
        try:
            producer = Producer({"bootstrap.servers": self.server_address})
            producer.produce(topic, str(message), callback=_delivery_report)
            producer.poll(2)
            logging.info(
                f"Successfully sent message to kafka at topic={topic} message={json.dumps(message)} server_address={self.server_address}"
            )
        except Exception as e:
            logging.info(
                f"Failed to send message to kafka at topic={topic} message={json.dumps(message)} server_address={self.server_address}"
            )
            raise Exception(e)


a = KafkaClient("ci-kafka:9092")
a.send_kafka_message({"msg": "hello"}, topic="ee2")
