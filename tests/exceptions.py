class APIException(Exception):  # noqa: B903
    def __init__(self, message, httperror):
        self.message = message
        self.httperror = httperror


class CouldNotDeleteDatabaseException(APIException):
    pass


class CouldNotDeleteDatasetException(APIException):
    pass
