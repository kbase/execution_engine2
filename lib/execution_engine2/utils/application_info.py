"""
Contains information about KBase applications.
"""

from execution_engine2.utils.arg_processing import check_string as _check_string
from execution_engine2.exceptions import IncorrectParamsException


def _get2part_string(s, sep, name):
    parts = s.split(sep)
    if len(parts) != 2:
        raise IncorrectParamsException(f"Expected exactly one '{sep}' in {name} '{s}'")
    return parts[0].strip(), parts[1].strip()


class AppInfo:
    """
    Information about a KBase app.

    Instance variables:
    module - the app's module, e.g. kb_uploadmethods.
    method - the SDK method the app will run, e.g. import_reads_from_staging
    application_module - the module containing the application. Under normal conditions this
        will be the same as 'module'.
    application - the id of the application, e.g. import_fastq_interleaved_as_reads_from_staging.
        This is the name of the folder in the 'ui/narrative/methods' folder in the app repo
        contining the spec files for the app.
    """

    def __init__(self, method: str, app_id: str, strict=True):
        """
        Create the application information.

        method - the method name, e.g. kb_uploadmethods.import_reads_from_staging
        app_id - the app name, either fully qualified (e.g.
            kb_uploadmethods/import_fastq_interleaved_as_reads_from_staging or unqualified (e.g.
            import_fastq_interleaved_as_reads_from_staging). If fully qualified, the module name
            of the app (kb_uploadmethds in this example) must match the module name for the method.
        strict - whether the app_id should be processed strictly or not. Without strict=True,
            1) The application module name may be different from the method module name
            2) The application module may be separated from the application name with a '.'
                rather than a '/'.
        """
        # Implementation notes: as of this writing, there are app_ids in the ee2 database
        # that have a . separator rather than a /, and, in some cases, test data where the
        # module for the application and method is not the same, although that should never
        # happen in practice. Hence we support non-strict mode to allow for those cases.
        mod, meth = _get2part_string(_check_string(method, "method ID"), '.', "method ID")
        self.module = _check_string(mod, "module portion of method ID")
        self.method = _check_string(meth, "method portion of method ID")
        app_id = _check_string(app_id, "application ID")
        if "/" in app_id and "." in app_id:
            raise IncorrectParamsException(
                f"Application ID '{app_id}' has both '/' and '.' separators")
        if "/" in app_id:
            mod, app = _get2part_string(app_id, "/", "application ID")
        elif '.' in app_id:
            if strict:
                raise IncorrectParamsException(f"Application ID '{app_id}' contains a '.'")
            mod, app = _get2part_string(app_id, ".", "application ID")
        else:
            mod = self.module
            app = app_id
        if strict and mod != self.module:
            raise IncorrectParamsException(
                f"Application module '{mod}' must equal method module '{self.module}'"
            )
        self.application_module = _check_string(mod, "module portion of application ID")
        self.application = _check_string(app, "application portion of application ID")

    def get_method_id(self) -> str:
        """
        Get the method id, e.g. module.method.
        """
        return f"{self.module}.{self.method}"

    def get_application_id(self) -> str:
        """
        Get the application id, e.g. module/application
        """
        return f"{self.application_module}/{self.application}"

    def __eq__(self, other):
        if type(self) == type(other):
            return (self.module, self.method, self.application_module, self.application) == (
                other.module,
                other.method,
                other.application_module,
                other.application,
            )
        return False

    def __hash__(self):
        return hash((self.module, self.method, self.application_module, self.application))
