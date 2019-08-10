class IncorrectParamsException(ValueError):
    pass


class MissingRunJobParamsException(ValueError):
    pass


class MissingCondorRequirementsException(ValueError):
    pass


class MalformedJobIdException(Exception):
    pass


class RecordNotFoundException(Exception):
    pass
