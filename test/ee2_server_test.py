import unittest
import os
import json
from configparser import ConfigParser

from lib.execution_engine2.execution_engine2Impl import execution_engine2
from .test_utils import is_timestamp

class EE2ServerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        token = os.environ.get("KB_AUTH_TOKEN")
        cls.ctx = {'token': token}
        cls.cfg = dict()
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('execution_engine2'):
            cls.cfg[nameval[0]] = nameval[1]
        cls.impl = execution_engine2(cls.cfg)

    def test_status(self):
        status = self.impl.status(self.ctx)[0]
        self.assertTrue(is_timestamp(status.get('servertime')))
        self.assertIsNotNone(status.get('gitcommit'))
