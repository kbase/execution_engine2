from pytest import raises
from execution_engine2.utils.user_info import UserCreds
from execution_engine2.exceptions import IncorrectParamsException
from utils_shared.test_utils import assert_exception_correct


def test_user_creds_init_success():
    uc = UserCreds("   username   ", "    some token   ")
    assert uc.username == "username"
    assert uc.token == "some token"


def test_user_creds_init_fail():
    _user_creds_init_fail(None, "t", IncorrectParamsException("Missing input parameter: username"))
    _user_creds_init_fail("   \t    ", "t", IncorrectParamsException(
        "Missing input parameter: username"))
    _user_creds_init_fail("u", None, IncorrectParamsException("Missing input parameter: token"))
    _user_creds_init_fail("u", "   \t    ", IncorrectParamsException(
        "Missing input parameter: token"))


def _user_creds_init_fail(username, token, expected):
    with raises(Exception) as got:
        UserCreds(username, token)
    assert_exception_correct(got.value, expected)


def test_user_creds_eq():
    u1 = "u1"
    u1a = "u1"
    u2 = "u2"
    t1 = "t1"
    t1a = "t1"
    t2 = "t2"

    assert UserCreds(u1, t1) == UserCreds(u1a, t1a)
    assert UserCreds(u1, t1) != UserCreds(u1, t2)
    assert UserCreds(u1, t1) != UserCreds(u2, t1)
    assert UserCreds(u1, t1) != (u1, t1)


def test_user_creds_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    u1 = "u1"
    u1a = "u1"
    u2 = "u2"
    t1 = "t1"
    t1a = "t1"
    t2 = "t2"

    assert hash(UserCreds(u1, t1)) == hash(UserCreds(u1a, t1a))
    assert hash(UserCreds(u1, t1)) != hash(UserCreds(u1, t2))
    assert hash(UserCreds(u1, t1)) != hash(UserCreds(u2, t1))
