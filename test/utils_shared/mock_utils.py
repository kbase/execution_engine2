from unittest.mock import create_autospec

from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient

from installed_clients.authclient import KBaseAuth

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.utils.Condor import Condor
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.utils.clients import ClientSet

_CLASS_IMPLEMENTATION_BUILDERS = {
    KBaseAuth: lambda config, cfgfile: KBaseAuth(
        auth_url=config["auth-url"] + "/api/legacy/KBase/Sessions/Login"
    ),
    AdminAuthUtil: lambda config, cfgfile: AdminAuthUtil(
        config["auth-url"], [ADMIN_READ_ROLE, ADMIN_WRITE_ROLE]
    ),
    Condor: lambda config, cfgfile: Condor(cfgfile),
    CatalogUtils: lambda config, cfgfile: CatalogUtils(
        config["catalog-url"], config["catalog-token"]
    ),
    KafkaClient: lambda config, cfgfile: KafkaClient(config["kafka-host"]),
    MongoUtil: lambda config, cfgfile: MongoUtil(config),
    SlackClient: lambda config, cfgfile: SlackClient(
        config["slack-token"], debug=True, endpoint=config["ee2-url"]
    ),
}

ALL_CLIENTS = _CLASS_IMPLEMENTATION_BUILDERS.keys()


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
    ret = {}
    for clazz in ALL_CLIENTS:
        if clazz in to_be_mocked:
            ret[clazz] = create_autospec(clazz, instance=True, spec_set=True)
        else:
            ret[clazz] = _CLASS_IMPLEMENTATION_BUILDERS[clazz](config, config_path)
    ret[ClientSet] = ClientSet(
        ret[KBaseAuth],
        ret[AdminAuthUtil],
        ret[Condor],
        ret[CatalogUtils],
        ret[KafkaClient],
        ret[MongoUtil],
        ret[SlackClient],
    )
    return ret
