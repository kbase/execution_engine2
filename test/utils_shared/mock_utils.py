
from unittest.mock import create_autospec

from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient

from installed_clients.authclient import KBaseAuth

from installed_clients.CatalogClient import Catalog
from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.utils.Condor import Condor
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.utils.clients import ClientSet

ALL_CLIENTS = [KBaseAuth,
               AdminAuthUtil,
               Condor,
               CatalogUtils,
               KafkaClient,
               MongoUtil,
               SlackClient]


def get_client_mocks(config, config_path, *to_be_mocked):
    """
    Create a client set containing a mix of mocks and real implementations as needed for
    a test.

    config is the config dict from the ee2 section of the deploy.cfg.
    config_path is the path to the configfile.
    to_be_mocked is the classes in the client set that should be mocked, e.g. KBaseAuth, etc.

    Returns a dict of the class to the the class's mock or implementation as specified in
    the arguments.
    """

    # could make a dict of methods to call on a non-mock build but that seems like overkill
    # and almost as much code as this
    if KBaseAuth in to_be_mocked:
        kbase_auth = create_autospec(KBaseAuth, instance=True, spec_set=True)
    else:
        kbase_auth = KBaseAuth(auth_url=config["auth-url"]
                               + "/api/legacy/KBase/Sessions/Login")

    if AdminAuthUtil in to_be_mocked:
        authadmin = create_autospec(AdminAuthUtil, instance=True, spec_set=True)
    else:
        authadmin = AdminAuthUtil(config["auth-url"], [ADMIN_READ_ROLE, ADMIN_WRITE_ROLE])

    if Condor in to_be_mocked:
        condor = create_autospec(Condor, instance=True, spec_set=True)
    else:
        condor = condor = Condor(config_path)

    if CatalogUtils in to_be_mocked:
        catutil = create_autospec(CatalogUtils, instance=True, spec_set=True)
    else:
        catutil = CatalogUtils(config["catalog-url"], config["catalog-token"])

    if KafkaClient in to_be_mocked:
        kafka = create_autospec(KafkaClient, instance=True, spec_set=True)
    else:
        kafka = KafkaClient(config["kafka-host"])

    if MongoUtil in to_be_mocked:
        mongo = create_autospec(MongoUtil, instance=True, spec_set=True)
    else:
        mongo = MongoUtil(config)

    if SlackClient in to_be_mocked:
        slack = create_autospec(SlackClient, instance=True, spec_set=True)
    else:
        slack = SlackClient(
            config["slack-token"], debug=True , endpoint=config["ee2-url"]
        )

    cs = ClientSet(kbase_auth, authadmin, condor, catutil, kafka, mongo, slack)
    return {
        ClientSet: cs,
        KBaseAuth: kbase_auth,
        AdminAuthUtil: authadmin,
        Condor: condor,
        CatalogUtils: catutil,
        KafkaClient: kafka,
        MongoUtil: mongo,
        SlackClient: slack,
    }
