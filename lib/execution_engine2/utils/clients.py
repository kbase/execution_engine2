""" Contains the various clients EE2 needs to communicate with other services it depends on. """

# Note on testing - this class is not generally unit-testable, and is only tested fully in
# integration tests.

from typing import Dict, Iterable

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient
from execution_engine2.utils.arg_processing import not_falsy as _not_falsy
from execution_engine2.utils.arg_processing import parse_bool
from execution_engine2.utils.job_requirements_resolver import JobRequirementsResolver
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace
from installed_clients.authclient import KBaseAuth
from installed_clients.execution_engine2Client import execution_engine2 as ee2


class UserClientSet:
    """
    Clients required by EE2 for communicating with other services that need to be instantiated
    on a per user basis. Also contains the user credentials for ease of use.
    """

    def __init__(
        self,
        user_id: str,
        token: str,
        workspace: Workspace,
        workspace_auth: WorkspaceAuth,
    ):
        """
        Initialize the client set.

        user_id - The user's ID.
        token - The users's token
        workspace - A workspace client initialized with the user's token.
        workspace_auth - A workspace auth client initialized with the user's token.
        """
        if not user_id or not user_id.strip():
            raise ValueError("user_id is required")
        if not token or not token.strip():
            raise ValueError("token is required")
        if not workspace:
            raise ValueError("workspace is required")
        if not workspace_auth:
            raise ValueError("workspace_auth is required")
        self.user_id = user_id
        self.token = token
        self.workspace = workspace
        self.workspace_auth = workspace_auth


def get_user_client_set(cfg: Dict[str, str], user_id: str, token: str):
    """
    Create the client set from a configuration dictionary.

    cfg - the configuration dictionary
    user_id - the ID of the user to be used to initialize the client set.
    token - the token of the user to be used to initialize the client set. Note that the set
        trusts that the token actually belongs to the user ID, and currently does not
        independently check the validity of the user ID.

    Expected keys in config:
    workspace-url - the URL of the kbase workspace service
    """
    if not cfg:
        raise ValueError("cfg is required")
    # Do a check that the url actually points to the workspace?
    # Also maybe consider passing in the workspace url rather than the dict, but the ClientSet
    # below will need lots of params so a dict makes sense there, maybe keep the apis similar?
    # TODO the client throws a 'X is not a valid url' error if the url isn't valid, improve
    #      by catching & rethrowing with a more clear message that the config is wrong
    ws_url = cfg.get("workspace-url")  # may want to make the keys constants?
    if not ws_url or not ws_url.strip():
        raise ValueError("missing workspace-url in configuration")
    workspace = Workspace(ws_url, token=token)
    workspace_auth = WorkspaceAuth(user_id, workspace)
    return UserClientSet(user_id, token, workspace, workspace_auth)


class ClientSet:
    """
    There is only one instance of this class globally. The codebase effectively treats this as a singleton.
    Clients required by EE2 for communicating with other services.
    These are not user-specific and can be reused throughout the application.
    """

    def __init__(
        self,
        auth: KBaseAuth,
        auth_admin: AdminAuthUtil,
        condor: Condor,
        catalog: Catalog,
        catalog_no_auth: Catalog,
        requirements_resolver: JobRequirementsResolver,
        kafka_client: KafkaClient,
        mongo_util: MongoUtil,
        slack_client: SlackClient,
        ee2_admin_client: ee2,
    ):
        """
        Initialize the client set from the individual clients.
        """

        self.auth = _not_falsy(auth, "auth")
        self.auth_admin = _not_falsy(auth_admin, "auth_admin")
        self.condor = _not_falsy(condor, "condor")
        self.catalog = _not_falsy(catalog, "catalog")
        self.catalog_no_auth = _not_falsy(catalog_no_auth, "catalog_no_auth")
        self.requirements_resolver = _not_falsy(
            requirements_resolver, "requirements_resolver"
        )
        self.kafka_client = _not_falsy(kafka_client, "kafka_client")
        self.mongo_util = _not_falsy(mongo_util, "mongo_util")
        self.slack_client = _not_falsy(slack_client, "slack_client")
        self.ee2_admin_client = _not_falsy(ee2_admin_client, "ee2_admin_client")


# the constructor allows for mix and match of mocks and real implementations as needed
# the method below handles all the client set up for going straight from a config


def get_clients(
    cfg: Dict[str, str],
    cfg_file: Iterable[str],
    override_client_group: str = None,
) -> (
    KBaseAuth,
    AdminAuthUtil,
    Condor,
    Catalog,
    Catalog,
    JobRequirementsResolver,
    KafkaClient,
    MongoUtil,
    SlackClient,
):
    """
    Get the set of clients used in the EE2 application that are not user-specific and can be
    reused from user to user.

    cfg - the configuration dictionary
    cfg_file - the full configuration file as a file like object or iterable.
    override_client_group - a client group name to override any client groups provided by
        users or the catalog service.

    Expected keys in config:
    auth-url - the root URL of the kbase auth service
    catalog-url - the URL of the catalog service
    catalog-token - a token to use with the catalog service. Ideally a service token
    kafka-host - the host string for a Kafka service
    slack-token - a token for contacting Slack
    """
    # Condor needs access to the entire deploy.cfg file, not just the ee2 section
    condor = Condor(cfg)
    # Do a check to ensure the urls and tokens actually work correctly?
    # TODO check keys are present - make some general methods for dealing with this
    # token is needed for running log_exec_stats in EE2Status
    catalog = Catalog(cfg["catalog-url"], token=cfg["catalog-token"])
    # instance of catalog without creds is used here
    catalog_no_auth = Catalog(cfg["catalog-url"])
    jrr = JobRequirementsResolver(cfg_file, override_client_group)
    auth_url = cfg["auth-url"]
    auth = KBaseAuth(auth_url=auth_url + "/api/legacy/KBase/Sessions/Login")
    # TODO using hardcoded roles for now to avoid possible bugs with mismatched cfg roles
    #      these should probably be configurable.
    #      See https://github.com/kbase/execution_engine2/issues/295
    auth_admin = AdminAuthUtil(auth_url, [ADMIN_READ_ROLE, ADMIN_WRITE_ROLE])

    # KafkaClient has a nice error message when the arg is None
    kafka_client = KafkaClient(cfg.get("kafka-host"))

    debug = parse_bool(cfg.get("debug"))
    # SlackClient handles None arguments
    slack_client = SlackClient(
        cfg.get("slack-token"), debug=debug, endpoint=cfg.get("ee2-url")
    )
    # TODO check how MongoUtil handles a bad config + that error messages are understandable
    mongo_util = MongoUtil(cfg)
    ee2_admin_client = ee2(url=cfg['external-url'],token=cfg[''])
    return (
        auth,
        auth_admin,
        condor,
        catalog,
        catalog_no_auth,
        jrr,
        kafka_client,
        mongo_util,
        slack_client,
        ee2_admin_client,
    )


def get_client_set(
    cfg: Dict[str, str],
    cfg_file: Iterable[str],
    override_client_group: str = None,
) -> ClientSet:
    """
    A helper method to create a ClientSet from a config dict rather than constructing and passing
    in clients individually.

    cfg - the configuration dictionary
    cfg_file - the full configuration file as a file like object or iterable.
    override_client_group - a client group name to override any client groups provided by
        users or the catalog service.

    Expected keys in config:
    auth-url - the root URL of the kbase auth service
    catalog-url - the URL of the catalog service
    catalog-token - a token to use with the catalog service. Ideally a service token
    kafka-host - the host string for a Kafka service
    slack-token - a token for contacting Slack
    """

    return ClientSet(*get_clients(cfg, cfg_file, override_client_group))
