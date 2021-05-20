class ExecutionEngineValueError(ValueError):
    """Base Class for ee2 exceptions"""

    pass


class ExecutionEngineException(Exception):
    pass


class IncorrectParamsException(ExecutionEngineValueError):
    pass


class MissingRunJobParamsException(ExecutionEngineValueError):
    """Missing a required run_job_parameter"""

    pass


class InvalidStatusTransitionException(ExecutionEngineValueError):
    pass


class InvalidOperationForStatusException(ExecutionEngineValueError):
    pass


class MissingCondorRequirementsException(ExecutionEngineValueError):
    pass


class MalformedJobIdException(ExecutionEngineValueError):
    pass


class MalformedTimestampException(ExecutionEngineException):
    pass


class ChildrenNotFoundError(ExecutionEngineException):
    pass


class RecordNotFoundException(ExecutionEngineException):
    pass


class CondorJobNotFoundException(ExecutionEngineException):
    pass


class RetryFailureException(ExecutionEngineException):
    """General exception for couldn't Retry the job failures'"""

    pass


class CannotRetryJob(ExecutionEngineException):
    """Can only retry errored or cancelled jobs, and not batch parents"""

    pass


class AuthError(ExecutionEngineException):
    """Raised if a user is unauthorized for a particular action, or doesn't have the right auth role"""

    pass
