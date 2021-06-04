from unittest.mock import create_autospec

from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.utils.job_requirements_resolver import JobRequirementsResolver
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient

from installed_clients.authclient import KBaseAuth
from installed_clients.CatalogClient import Catalog

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.utils.Condor import Condor
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.utils.clients import ClientSet


def _build_job_reqs(config, cfgfile, impls):
    with open(cfgfile) as cf:
        return JobRequirementsResolver(impls[Catalog], cf)


_CLASS_IMPLEMENTATION_BUILDERS = {
    KBaseAuth: lambda config, cfgfile, impls: KBaseAuth(
        auth_url=config["auth-url"] + "/api/legacy/KBase/Sessions/Login"
    ),
    AdminAuthUtil: lambda config, cfgfile, impls: AdminAuthUtil(
        config["auth-url"], [ADMIN_READ_ROLE, ADMIN_WRITE_ROLE]
    ),
    Condor: lambda config, cfgfile, impls: Condor(config),
    Catalog: lambda config, cfgfile, impls: Catalog(config["catalog-url"]),
    JobRequirementsResolver: _build_job_reqs,
    KafkaClient: lambda config, cfgfile, impls: KafkaClient(config["kafka-host"]),
    MongoUtil: lambda config, cfgfile, impls: MongoUtil(config),
    SlackClient: lambda config, cfgfile, impls: SlackClient(
        config["slack-token"], debug=True, endpoint=config["ee2-url"]
    ),
}

ALL_CLIENTS = sorted(_CLASS_IMPLEMENTATION_BUILDERS.keys(), key=lambda x: x.__name__)


def get_client_mocks(config, config_path, *to_be_mocked):
    """
    Create a client set containing a mix of mocks and real implementations as needed for
    a test.

    config is the config dict from the ee2 section of the deploy.cfg.
    config_path is the path to the configfile.
    to_be_mocked is the classes in the client set that should be mocked, e.g. KBaseAuth, etc.

    Returns a dict of the class to the class's mock or implementation as specified in
    the arguments.
    """
    ret = {}
    for clazz in ALL_CLIENTS:
        if clazz in to_be_mocked:
            ret[clazz] = create_autospec(clazz, instance=True, spec_set=True)
        else:
            # this is a hack - only one client depends on another (JRR -> Cat)
            # so we rely on the ALL_CLIENTS sort to make sure the dependency is built before the
            # dependent module. If things become more complicated we'll need a dependency graph.
            ret[clazz] = _CLASS_IMPLEMENTATION_BUILDERS[clazz](config, config_path, ret)
    ret[ClientSet] = ClientSet(
        ret[KBaseAuth],
        ret[AdminAuthUtil],
        ret[Condor],
        ret[Catalog],  # This one is for "catalog"
        ret[Catalog],  # This one is for "catalog_no_auth"
        ret[JobRequirementsResolver],
        ret[KafkaClient],
        ret[MongoUtil],
        ret[SlackClient],
    )
    return ret
