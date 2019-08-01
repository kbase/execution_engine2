from typing import Dict, List, NamedTuple

from lib.execution_engine2.utils.Scheduler import Scheduler


class Condor(Scheduler):
    # TODO: Is this the best way to do this?
    class condor_resources(NamedTuple):
        request_cpus: int
        request_memory: int
        request_disk: str
        client_group: str

    def get_default_resources(self, client_group: str) -> Dict[str, str]: ...

    def setup_environment_vars(self, client_group: Dict[str, str]) -> Dict[str, str]: ...

    def check_for_missing_runjob_params(self, client_group: Dict[str, str]): ...

    def normalize(self, client_group: Dict[str, str]) -> Dict[str, str]: ...

    def extract_resources(self, client_group: Dict[str, str]) -> condor_resources: ...

    def extract_requirements(self, cgr: Dict[str, str] = None, client_group: str = None) -> List[
        str]: ...
