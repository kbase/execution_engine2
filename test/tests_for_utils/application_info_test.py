from pytest import raises
from execution_engine2.utils.application_info import AppInfo
from execution_engine2.exceptions import IncorrectParamsException
from utils_shared.test_utils import assert_exception_correct


def test_app_info_strict_init_success():
    ai = AppInfo("   \t  mod   .   meth   ", "mod/  appthing")
    assert ai.module == "mod"
    assert ai.method == "meth"
    assert ai.application_module == "mod"
    assert ai.application == "appthing"
    assert ai.get_method_id() == "mod.meth"
    assert ai.get_application_id() == "mod/appthing"


def test_app_info_without_app_module_strict_init_success():
    ai = AppInfo("   \t  mod   .   meth   ", "  appthing \t  ")
    assert ai.module == "mod"
    assert ai.method == "meth"
    assert ai.application_module == "mod"
    assert ai.application == "appthing"
    assert ai.get_method_id() == "mod.meth"
    assert ai.get_application_id() == "mod/appthing"


def test_app_info_init_success():
    ai = AppInfo("   \t  mod   .   meth   ", "mod2.  appthing", strict=False)
    assert ai.module == "mod"
    assert ai.method == "meth"
    assert ai.application_module == "mod2"
    assert ai.application == "appthing"
    assert ai.get_method_id() == "mod.meth"
    assert ai.get_application_id() == "mod2/appthing"


def test_app_info_init_fail():
    m = "m.n"
    a = "m.b"
    _app_info_init_fail(
        None, a, False, IncorrectParamsException("Missing input parameter: method ID")
    )
    _app_info_init_fail(
        "   \t    ",
        a,
        False,
        IncorrectParamsException("Missing input parameter: method ID"),
    )
    _app_info_init_fail(
        "   method   ",
        a,
        False,
        IncorrectParamsException("Expected exactly one '.' in method ID 'method'"),
    )
    _app_info_init_fail(
        "   mod.innermod.method   ",
        a,
        False,
        IncorrectParamsException(
            "Expected exactly one '.' in method ID 'mod.innermod.method'"
        ),
    )
    _app_info_init_fail(
        "    .  meth",
        a,
        False,
        IncorrectParamsException(
            "Missing input parameter: module portion of method ID"
        ),
    )
    _app_info_init_fail(
        " mod   .  ",
        a,
        False,
        IncorrectParamsException(
            "Missing input parameter: method portion of method ID"
        ),
    )

    _app_info_init_fail(
        m,
        None,
        False,
        IncorrectParamsException("Missing input parameter: application ID"),
    )
    _app_info_init_fail(
        m,
        "   \t    ",
        False,
        IncorrectParamsException("Missing input parameter: application ID"),
    )
    _app_info_init_fail(
        m,
        "mod / meth.bak ",
        False,
        IncorrectParamsException(
            "Application ID 'mod / meth.bak' has both '/' and '.' separators"
        ),
    )
    _app_info_init_fail(
        m,
        "mod / meth / bak ",
        False,
        IncorrectParamsException(
            "Expected exactly one '/' in application ID 'mod / meth / bak'"
        ),
    )
    _app_info_init_fail(
        m,
        "mod.meth",
        True,
        IncorrectParamsException("Application ID 'mod.meth' contains a '.'"),
    )
    _app_info_init_fail(
        m,
        "mod.meth.anothermeth",
        False,
        IncorrectParamsException(
            "Expected exactly one '.' in application ID 'mod.meth.anothermeth'"
        ),
    )

    _app_info_init_fail(
        "mod.meth",
        "  mod2  /meth",
        True,
        IncorrectParamsException(
            "Application module 'mod2' must equal method module 'mod'"
        ),
    )

    _app_info_init_fail(
        m,
        "mod/",
        False,
        IncorrectParamsException(
            "Missing input parameter: application portion of application ID"
        ),
    )
    _app_info_init_fail(
        m,
        "/meth",
        False,
        IncorrectParamsException(
            "Missing input parameter: module portion of application ID"
        ),
    )
    _app_info_init_fail(
        m,
        "mod.   ",
        False,
        IncorrectParamsException(
            "Missing input parameter: application portion of application ID"
        ),
    )
    _app_info_init_fail(
        m,
        "   .meth",
        False,
        IncorrectParamsException(
            "Missing input parameter: module portion of application ID"
        ),
    )


def _app_info_init_fail(meth, app, strict, expected):
    with raises(Exception) as got:
        AppInfo(meth, app, strict)
    assert_exception_correct(got.value, expected)


def test_equals():
    assert AppInfo("m.n", "m/p") == AppInfo("m.n", "m/p")
    assert AppInfo("m.n", "p/p", False) == AppInfo("m.n", "p/p", False)

    assert AppInfo("m.n", "m/p", False) != AppInfo("n.n", "m/p", False)
    assert AppInfo("m.n", "m/p") != AppInfo("m.x", "m/p")
    assert AppInfo("m.n", "m/p", False) != AppInfo("m.n", "x/p", False)
    assert AppInfo("m.n", "m/p") != AppInfo("m.n", "m/x")
    assert AppInfo("m.n", "m/p") != ("m.n", "m/x")


def test_hashcode():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    assert hash(AppInfo("m.n", "m/p")) == hash(AppInfo("m.n", "m/p"))
    assert hash(AppInfo("m.n", "p/p", False)) == hash(AppInfo("m.n", "p/p", False))

    assert hash(AppInfo("m.n", "m/p", False)) != hash(AppInfo("n.n", "m/p", False))
    assert hash(AppInfo("m.n", "m/p")) != hash(AppInfo("m.x", "m/p"))
    assert hash(AppInfo("m.n", "m/p", False)) != hash(AppInfo("m.n", "x/p", False))
    assert hash(AppInfo("m.n", "m/p")) != hash(AppInfo("m.n", "m/x"))
