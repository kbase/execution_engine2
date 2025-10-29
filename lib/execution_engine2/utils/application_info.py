"""
Contains information about KBase applications.
"""

from typing import Union
from execution_engine2.utils.arg_processing import check_string as _check_string
from execution_engine2.exceptions import IncorrectParamsException


def _get2part_string(s, sep, name, err_pt1, err_pt2, desired_sep=None):
    desired_sep = desired_sep if desired_sep else sep
    parts = s.split(sep)
    if len(parts) != 2:
        raise IncorrectParamsException(
            f"Expected exactly one '{desired_sep}' in {name} '{s}'"
        )
    return _check_string(parts[0], err_pt1), _check_string(parts[1], err_pt2)


class AppInfo:
    """
    Information about a KBase app.

    Instance variables:
    module - the app's module, e.g. kb_uploadmethods.
    method - the SDK method the app will run, e.g. import_reads_from_staging
    application_module - the module containing the application. Under normal conditions this
        will be the same as 'module', if not None. Always supplied if 'application' is not None.
    application - the id of the application, e.g. import_fastq_interleaved_as_reads_from_staging.
        This is the name of the folder in the 'ui/narrative/methods' folder in the app repo
        contining the spec files for the app. May be None.
    """

    def __init__(self, method: str, app_id: Union[str, None] = None, strict=True):
        """
        Create the application information.

        method - the method name, e.g. kb_uploadmethods.import_reads_from_staging
        app_id - the app name in the module/app_name format (e.g.
            kb_uploadmethods/import_fastq_interleaved_as_reads_from_staging). For historical
            reasons, this class will also accept only the module name or the module.app_name
            format. In both cases the module name must match that given for the method argument.
            Optional.
        strict - whether the app_id should be processed strictly or not. Without strict=True,
            the application module name may be different from the method module name.
        """
        # Implementation notes: as of this writing, there are KBase processes that
        # submit app_ids to ee2 that:
        # 1) have a . separator rather than a /
        #    - Narrative downloads are a known place where this happens, although
        #      there are many other jobs in the database with this pattern, so there may be
        #      unknown processes submitting jobs like this. In most cases, this is just the
        #      process using the method for the app_id (and note that is often inaccurate).
        # 2) consist only of a module ID with no separator
        #    - KBParallel does this. That may be the only source or there may be other sources
        #      as well, unknown.
        # There are also some records in the database where the module for the application and
        # method is not the same - to the best of our knowledge this was one off test data and
        # shouldn't be expected to happen in practice.
        # As such:
        # 1) The only requirement for the app ID is that, if provided, it starts with the module
        #    given in the method argument. That must be followed by either nothing, or
        #    a '.' or '/' separator containing an arbitrary string.
        # 2) We provide a 'strict' argument to disable even that check, which should be used for
        #    data loaded from the database.
        self.module, self.method = _get2part_string(
            _check_string(method, "method ID"),
            ".",
            "method ID",
            "module portion of method ID",
            "method portion of method ID",
        )
        app_id = _check_string(app_id, "application ID", optional=True)
        app = None
        sep = None
        mod = None
        if app_id:
            err1 = "module portion of application ID"
            err2 = "application portion of application ID"
            if "/" in app_id and "." in app_id:
                raise IncorrectParamsException(
                    f"Application ID '{app_id}' has both '/' and '.' separators"
                )
            if "/" in app_id:
                mod, app = _get2part_string(app_id, "/", "application ID", err1, err2)
                sep = "/"
            elif "." in app_id:
                mod, app = _get2part_string(
                    app_id, ".", "application ID", err1, err2, "/"
                )
                sep = "."
            else:
                mod = app_id
        if strict and mod and mod != self.module:
            raise IncorrectParamsException(
                f"Application module '{mod}' must equal method module '{self.module}'"
            )
        self.application = app
        self.application_module = mod
        self._sep = sep

    def get_method_id(self) -> str:
        """
        Get the method id, e.g. module.method.
        """
        return f"{self.module}.{self.method}"

    def get_application_id(self) -> str:
        """
        Get the application id, e.g. module/application, if present
        """
        if not self.application_module:
            return None
        if self.application:
            return f"{self.application_module}{self._sep}{self.application}"
        return self.application_module

    def __eq__(self, other):
        if type(self) == type(other):  # noqa E721
            return (
                self.module,
                self.method,
                self.application_module,
                self.application,
                self._sep,
            ) == (
                other.module,
                other.method,
                other.application_module,
                other.application,
                other._sep,
            )
        return False

    def __hash__(self):
        return hash(
            (
                self.module,
                self.method,
                self.application_module,
                self.application,
                self._sep,
            )
        )
