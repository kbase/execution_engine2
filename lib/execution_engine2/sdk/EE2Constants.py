from dataclasses import dataclass
from typing import Optional, NamedTuple

# May want to make this configurable. Hardcoded for now as we want concierge data to be owned
# by this user.
# An alternative approach would be to configure a kbaseconcierge token in the config, and then
# specify an auth2 role that allows users to replace their token with the kbaseconcierge token
# when running jobs. Needs more thought.
KBASE_CONCIERGE_USERNAME = "kbaseconcierge"
CONCIERGE_CLIENTGROUP = "kbase_concierge"

EE2_CONFIG_SECTION = "execution_engine2"
EE2_DEFAULT_SECTION = "DEFAULT"
EE2_DEFAULT_CLIENT_GROUP = "default_client_group"

# these also probably should be configurable.
ADMIN_READ_ROLE = "EE2_ADMIN_RO"
ADMIN_WRITE_ROLE = "EE2_ADMIN"


class JobError(NamedTuple):
    name: str
    message: str
    code: int
    error: str


@dataclass()
class ConciergeParams:
    """ Set requested params. If you don't specify CG, its automatically set for you"""

    request_cpus: int
    request_memory: int
    request_disk: int
    job_priority: int = None
    account_group: str = None
    requirements_list: list = None
    client_group: Optional[str] = CONCIERGE_CLIENTGROUP
