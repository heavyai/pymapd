
try:
    # python 2
    from exceptions import StandardError as Exception
except ImportError:
    pass


class Warning(Exception):
    """Emitted for important warnings, e.g. data truncatiions"""

class Error(Exception):
    """Base class for all pymapd errors."""


class InterfaceError(Error):
    """Raised whenever you use pymapd interface incorrectly."""


class DatabaseError(Error):
    """Raised when the database encounters an error."""


class DataError(DatabaseError):
    """Raised for data processing errors like division by zero, etc."""


class OperationalError(DatabaseError):
    """Raised for non-programmer related database errors, e.g.
    an unexpected disconnect.
    """


class IntegrityError(DatabaseError):
    """Raised when the relational integrity of the database is affected."""


class InternalError(DatabaseError):
    """Raised for errors internal to the database, e.g. and invalid cursor."""


class ProgrammingError(DatabaseError):
    """Raised for programming errors, e.g. syntax errors, table already
    exists.
    """


class NotSupportedError(DatabaseError):
    """Raised when an API not supported by the database is used."""
