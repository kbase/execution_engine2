# -*- coding: utf-8 -*-
"""Module to test out logging to kafka."""

import json
import logging

from execution_engine2.utils.KafkaUtils import (
    KafkaHandler,
    send_message_to_kafka,
    _test_sample_consumer,
)
import unittest


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        This test is used for sending commands to a live environment, such as CI.
        TravisCI doesn't need to run this test.
        :return:
        """

    @classmethod
    def tearDownClass(cls):
        pass

    def test_kafka_producer(self):
        message = {
            "heading": "Dear Dolly",
            "body": "Thank you for being a great cloned sheep.",
        }
        send_message_to_kafka(data=message)

    def test_sample_consumer(self):
        message = {
            "heading": "Dear Dolly",
            "body": "Thank you for being a great cloned sheep.",
        }
        send_message_to_kafka(data=message)
        message = {
            "heading": "Dear Dolly",
            "body": "Thank you for being a great cloned sheep.",
            "error": "true",
        }
        send_message_to_kafka(data=message)

        _test_sample_consumer()

    def test_kafka_logger(self):

        """Run the actual connections."""

        logger = logging.getLogger(__name__)
        # enable the debug logger if you want to see ALL of the lines
        # logging.basicConfig(level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)

        kh = KafkaHandler(["kafka:9092"], "ee2")
        logger.addHandler(kh)

        logger.info("I'm a little logger, short and stout")
        logger.debug("Don't tase me bro!")
