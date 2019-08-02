from configparser import ConfigParser
import os
from dotenv import load_dotenv
import pathlib
from shutil import copyfile



def _create_sample_params(self):
    params = dict()
    params["job_id"] = self.job_id
    params["user"] = "kbase"
    params["token"] = "test_token"
    params["client_group_and_requirements"] = "njs"
    return params

def read_config_into_dict(config='deploy.cfg', section='execution_engine2'):
    config_parser = ConfigParser()
    config_parser.read(config)
    config = dict()
    for key,val in config_parser[section].items():
        config[key] = val
    return config

def bootstrap():
    test_env = "test.env"
    pwd = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    if not os.path.exists(test_env):
        copyfile(f"{pwd}/test/env/{test_env}", f"{test_env}")
    load_dotenv("test.env", verbose=True)