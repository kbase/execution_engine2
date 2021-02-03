"""
Contains classes and fuctions for use with the EE2 SDK API class (e.g. the *Impl.py file).
"""

from typing import Dict
from execution_engine2.utils.clients import UserClientSet, get_user_client_set


# this class is only tested as part of integration tests.
class GenerateFromConfig:
    """
    Utility methods to generate constructs from the service configuration.
    """

    def __init__(self, cfg: Dict[str, str]):
        """
        Create an instance from a configuration.

        cfg - the configuration.
        """
        self.cfg = cfg

    def get_user_clients(self, ctx) -> UserClientSet:
        """
        Create a user client set from an SDK context object.

        ctx - the context object. This is passed in to SDK methods in the *Impl.py file. It is
            expected that the context object contains the user_id and token keys, and this method
            will fail with a KeyError if it does not.
        """
        return get_user_client_set(self.cfg, ctx["user_id"], ctx["token"])
