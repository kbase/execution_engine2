# -*- coding: utf-8 -*-
"""Module to test out logging to kafka."""
import json
import copy
import time
import unittest
from io import StringIO
from unittest.mock import patch

from execution_engine2.utils.KafkaUtils import send_kafka_message, KafkaStatusUpdate
from test.utils.KafkaTestUtils import Consumer, KafkaError


class ExecutionEngine2SchedulerTest(unittest.TestCase):

    # TODO Integration Test with EE2/KAFKA
    # TODO Test to make sure that the correct messages are being sent from the various methods
    # TODO Create a test for each method.. But they are already being tested in the ee2 sdkmr tests though

    def test_status_update(self):
        # #data = {
        #     "job_id": "123",
        #     "previous_status": "created",
        #     "new_status": "estimating",
        # }
        with self.assertRaisesRegex(
            expected_exception=Exception,
            expected_regex="You must pass a scheduler id once the job has been created already.",
        ):
            send_kafka_message(
                KafkaStatusUpdate(
                    job_id="123", previous_status="created", new_status="new_status"
                )
            )

        send_kafka_message(
            KafkaStatusUpdate(
                job_id="123",
                previous_status="created",
                new_status="new_status",
                scheduler_id=123,
            )
        )

    def xtest_produce_and_consume(self):
        """
        Send a message via the kafka utils
        and test that the producer callback function works
        :return:
        """
        data = {"job_id": "123", "previous_status": "running", "new_status": "created"}
        data2 = copy.copy(data)
        data2["error"] = True

        with patch("sys.stdout", new=StringIO()) as fakeOutput:
            # send_message_to_kafka(data)
            # send_message_to_kafka(data2)
            stdout = fakeOutput.getvalue().strip()
            self.assertIn(f"Message delivered to topic 'ee2':", stdout)

        for k, v in data.items():
            self.assertIn(k, stdout)
            self.assertIn(v, stdout)

        # Is order assured?
        last_msg = self._consume_last("ee2", "ee2")
        for key in data.keys():
            value = data[key]
            self.assertIn(key, last_msg)
            data_value = last_msg[key]
            self.assertEquals(value, data_value)

        self.assertIn("error", last_msg)

        last_msg2 = self._consume_last("ee2", "ee2")
        for key in data2.keys():
            value2 = data2[key]
            self.assertIn(key, last_msg2)
            data_value2 = last_msg2[key]
            self.assertEquals(value2, data_value2)

        self.assertIn("error", last_msg2)

    @staticmethod
    def _consume_last(topic, key, timeout=10):
        """Consume the most recent message from the topic stream."""
        consumer = Consumer(
            {
                "bootstrap.servers": "kafka",
                "group.id": "test_only",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])
        start_time = time.time()
        while True:
            # get time elapsed in seconds
            elapsed = time.time() - start_time
            if elapsed > timeout:
                # close consumer before throwing error
                consumer.close()
                raise TimeoutError(
                    f"Error: Consumer waited past timeout of {timeout} seconds"
                )
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("End of stream.")
                else:
                    print(f"Error: {msg.error()}")
                continue
            if msg:
                consumer.close()
                return json.loads(msg.value())
