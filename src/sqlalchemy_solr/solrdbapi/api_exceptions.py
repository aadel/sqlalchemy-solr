class Error(Exception):  # noqa: B903
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(f"{self.message}")


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class DatabaseHTTPError(DatabaseError):
    def __init__(self, message, http_error):
        super().__init__(message)

        self.http_error = http_error

    def __str__(self):
        return repr(f"HTTP Error {self.http_error} {self.message}")


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class AuthenticationError(OperationalError):
    pass


class ProgrammingError(DatabaseError):
    pass


class CursorClosedException(InternalError):
    pass


class ConnectionClosedException(InternalError):
    pass


class UninitializedResultSetError(ProgrammingError):
    pass
