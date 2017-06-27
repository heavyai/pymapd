# module constants
from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

del get_distribution, DistributionNotFound

from mapd import MapD  # noqa


apilevel = "2.0"
threadsafety = 1  # TODO: check
paramstyle = "qmark"

from .connection import (  # noqa
    connect, Connection
)

from .cursor import Cursor # noqa

from .exceptions import (  # noqa
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)


from .dtypes import (  # noqa
    Binary,
    Date,
    Time,
    Timestamp,
    BINARY,
    STRING,
    NUMBER,
    DATETIME,
    ROWID,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)
