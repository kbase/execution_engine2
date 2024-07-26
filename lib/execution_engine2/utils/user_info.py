"""
User information classes and methods.
"""

from execution_engine2.utils.arg_processing import check_string as _check_string


class UserCreds:
    """
    Contains a user's username and token.

    Instance variables:
    username - the users's username.
    token - the user's token.
    """

    # TODO replace the creds in the clients.UserClientSet with this class

    def __init__(self, username: str, token: str):
        """
        Create the creds.

        username - the user's username.
        token - the user's token. It is expected that the client programmer verifies that the
            token is indeed tied to the user.
        """
        self.username = _check_string(username, "username")
        self.token = _check_string(token, "token")

    def __eq__(self, other):
        if type(self) == type(other): # noqa E721
            return (self.username, self.token) == (other.username, other.token)
        return False

    def __hash__(self):
        return hash((self.username, self.token))
