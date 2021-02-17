'''
A controller for the KBase Auth2 service (https://github.com/kbase/auth2) for use in testing
auth-enabled applications.
'''

# Ported from:
# https://github.com/kbase/sample_service/blob/master/test/auth_controller.py
# May want to set up a python package for this...?

import os
import requests
import shutil
import subprocess
import tempfile
import time
import zipfile

from pathlib import Path
from utils_shared.test_utils import TestException
from utils_shared import test_utils

_AUTH_CLASS = 'us.kbase.test.auth2.StandaloneAuthServer'
_JARS_FILE = Path(__file__).resolve().parent.joinpath('authjars')


class AuthController:
    """
    The main Auth controller class.

    Attributes:
    version - the version of the auth service
    port - the port for the Auth service.
    temp_dir - the location of the Auth data and logs.
    """

    def __init__(
            self,
            jars_dir: Path,
            mongo_host: str,
            mongo_db: str,
            root_temp_dir: Path,
            mongo_user: str = None,
            mongo_pwd: str = None):
        '''
        Create and start a new Auth service. An unused port will be selected for the server.

        :param jars_dir: The path to the lib/jars dir of the KBase Jars repo
            (https://github.com/kbase/jars), e.g /path_to_repo/lib/jars.
        :param mongo_host: The address of the MongoDB server to use as the Auth service database,
            e.g. localhost:27017.
        :param mongo_db: The database in which to store Auth data.
        :param root_temp_dir: A temporary directory in which to store Auth data and log files.
            The files will be stored inside a child directory that is unique per invocation.
        :param mongo_user: if the MongoDB server requires authentication, the user name.
        :param mongo_pwd: if the MongoDB server requires authentication, the user password.
        '''
        if not jars_dir or not os.access(jars_dir, os.X_OK):
            raise TestException('jars_dir {} does not exist or is not executable.'
                                .format(jars_dir))
        if not mongo_host:
            raise TestException('mongo_host must be provided')
        if not mongo_db:
            raise TestException('mongo_db must be provided')
        if not root_temp_dir:
            raise TestException('root_temp_dir is None')

        if bool(mongo_user) ^ bool(mongo_pwd):  # xor
            raise TestException('Neither or both of mongo_user and mongo_pwd is required')

        jars_dir = jars_dir.resolve()
        class_path = self._get_class_path(jars_dir)

        # make temp dirs
        root_temp_dir = root_temp_dir.absolute()
        os.makedirs(root_temp_dir, exist_ok=True)
        self.temp_dir = Path(tempfile.mkdtemp(prefix='AuthController-', dir=str(root_temp_dir)))

        self.port = test_utils.find_free_port()

        template_dir = self.temp_dir.joinpath('templates')
        self._install_templates(jars_dir, template_dir)

        command = ['java',
                   '-classpath', class_path,
                   '-DAUTH2_TEST_MONGOHOST=' + mongo_host,
                   '-DAUTH2_TEST_MONGODB=' + mongo_db,
                   '-DAUTH2_TEST_TEMPLATE_DIR=' + str(template_dir),
                   _AUTH_CLASS,
                   str(self.port)
                   ]
        if mongo_user:
            command.insert(5, '-DAUTH2_TEST_MONGOPWD=' + mongo_pwd)
            command.insert(5, '-DAUTH2_TEST_MONGOUSER=' + mongo_user)

        self._outfile = open(self.temp_dir.joinpath('auth.log'), 'w')

        self._proc = subprocess.Popen(command, stdout=self._outfile, stderr=subprocess.STDOUT)

        for count in range(40):
            err = None
            time.sleep(1)  # wait for server to start
            try:
                res = requests.get(
                    f'http://localhost:{self.port}', headers={'accept': 'application/json'})
                if res.ok:
                    self.version = res.json()['version']
                    break
                err = TestException(res.text)
            except requests.exceptions.ConnectionError as e:
                err = TestException(e.args[0])
                err.__cause__ = e
        if err:
            raise err
        self.startup_count = count + 1

    def destroy(self, delete_temp_files: bool = True):
        '''
        Shut down the server and optionally delete any files generated.

        :param delete_temp_files: if true, delete all the temporary files generated as part of
            running the server.
        '''
        if self._proc:
            self._proc.terminate()
        if self._outfile:
            self._outfile.close()
        if delete_temp_files and self.temp_dir:
            shutil.rmtree(self.temp_dir)

    def _install_templates(self, jars_dir: Path, template_dir: Path):
        with open(_JARS_FILE) as jf:
            template_zip_file = jars_dir.joinpath(jf.readline().strip())
        with zipfile.ZipFile(template_zip_file) as z:
            # should really check to see that the entries are safe, but it's our zipfile, so
            # don't bother for now.
            z.extractall(template_dir)

    def _get_class_path(self, jars_dir: Path):
        cp = []
        with open(_JARS_FILE) as jf:
            jf.readline()  # 1st line is template file
            for line in jf:
                if line.strip() and not line.startswith('#'):
                    p = jars_dir.joinpath(line.strip())
                    if not p.is_file():
                        raise TestException(f'Required jar does not exist: {p}')
                    cp.append(str(p))
        return ':'.join(cp)
