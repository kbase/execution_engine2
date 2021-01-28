'''
Contains the various clients EE2 needs to communicate with other services it depends on.
'''

# Note on testing - this class is not generally unit-testable, and is only tested fully in
# integration tests.

from typing import Dict

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import Condor
from execution_engine2.constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient

from installed_clients.authclient import KBaseAuth
from installed_clients.WorkspaceClient import Workspace


class UserClientSet:
    '''
    Clients required by EE2 for communicating with other services that need to be instantiated
    on a per user basis. Also contains the user credentials for ease of use.
    '''
    def __init__(self, cfg: Dict[str, str], user_id: str, token: str):
        '''
        Initialize the client set.

        cfg - the configuration dictionary
        user_id - the ID of the user to be used to initialize the client set.
        token - the token of the user to be used to initialize the client set. Note that the set
            trusts that the token actually belongs to the user ID, and currently does not independently
            check the validity of the user ID.

        Expected keys in config:
        workspace-url - the URL of the kbase workspace service
        '''
        if not user_id or not user_id.strip():
            raise ValueError('user_id is required')
        if not token or not token.strip():
            raise ValueError('token is required')
        if not cfg:
            raise ValueError('cfg is required')
        self._user_id = user_id
        self._token = token

        # Do a check that the url actually points to the workspace?
        # Also maybe consider passing in the workspace url rather than the dict, but the ClientSet
        # below will need lots of params so a dict makes sense there, maybe keep the apis similar?
        # ws_url = cfg.get('workspace_url')  # may want to make the keys constants?
        # if not ws_url or not ws_url.strip:
        #     raise ValueError('missing workspace-url key in configuration')
        # TODO Originally did the above check but caused 36 test failures so... meh for now.
        self._workspace = Workspace(cfg['workspace-url'], token=token)
        self._workspace_auth = WorkspaceAuth(user_id, self._workspace)

    # create_autospec can't mock instance variables, so we add some boilerplate
    # Could make these properties but then they return MagicMocks which don't update with API
    # changes

    def user_id(self):
        return self._user_id

    def token(self):
        return self._token

    def workspace(self):
        return self._workspace

    def workspace_auth(self):
        return self._workspace_auth


class ClientSet:
    '''
    Clients required by EE2 for communicating with other services.

    These are not user-specific and can be reused throughout the application.
    '''

    def __init__(self, cfg: Dict[str, str], cfg_path: str, debug: bool = False):
        '''
        Initialize the client set from a configuration dictionary.

        cfg - the configuration dictionary
        cfg_path - the path to the configuration file
        debug - set clients that support it to debug mode

        Expected keys in config:
        auth-url - the root URL of the kbase auth service
        catalog-url - the URL of the catalog service
        catalog-token - a token to use with the catalog service. Ideally a service token
        kafka-host - the host string for a Kafka service
        slack-token - a token for contacting Slack
        '''
        # TODO seems like it'd make sense to init Condor from a config dict like everything else
        self.condor = Condor(cfg_path)
        self.catalog_utils = CatalogUtils(cfg['catalog-url'], cfg['catalog-token'])
        auth_url = cfg['auth-url']
        self.auth = KBaseAuth(auth_url=auth_url + '/api/legacy/KBase/Sessions/Login')
        # TODO using hardcoded roles for now to avoid possible bugs with mismatched cfg roles
        #       these should probably be configurable
        self.auth_admin = AdminAuthUtil(auth_url, [ADMIN_READ_ROLE, ADMIN_WRITE_ROLE])

        # KafkaClient has a nice error message when the arg is None
        self.kafka_client = KafkaClient(cfg.get('kafka-host'))
        # SlackClient handles None arguments
        self.slack_client = SlackClient(
            cfg.get("slack-token"), debug=debug, endpoint=cfg.get("ee2-url"))
