import logging
from enum import Enum

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.exceptions import AuthError


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


def check_is_admin(self):
    """
    Check Auth for your admin role and see if it is an allowed admin role
    :return:
    """
    return int(self._is_admin(self.token))





def _is_admin(self, token: str) -> bool:
    try:
        self.is_admin = AdminAuthUtil(self.auth_url, self.admin_roles).is_admin(token)
        return self.is_admin
    except AuthError as e:
        logging.error(f"An auth error occurred: {str(e)}")
        raise e
    except RuntimeError as e:
        logging.error(f"A runtime error occurred while looking up user roles: {str(e)}")
        raise e
