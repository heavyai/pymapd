"""
Connect to a MapD database.
"""
from typing import List, Tuple, Any, Union, Optional  # noqa

import six
from thrift.protocol import TBinaryProtocol, TJSONProtocol
from thrift.transport import TSocket, THttpClient, TTransport
from thrift.transport.TSocket import TTransportException
from mapd.MapD import Client
from mapd.ttypes import TMapDException

from .cursor import Cursor
from .exceptions import _translate_exception, OperationalError


def connect(user=None,         # type: Optional[str]
            password=None,     # type: Optional[str]
            host=None,         # type: Optional[str]
            port=9091,         # type: Optional[int]
            dbname=None,       # type: Optional[str]
            protocol='binary'  # type: Optional[str]
            ):
    # type: (...) -> Connection
    """
    Crate a new Connection.

    Parameters
    ----------
    user : str
    password : str
    host : str
    port : int
    dbname : str
    protocol : {'binary', 'http'}

    Returns
    -------
    conn : Connection
    """
    # TODO: accept a dsn URI like sqlalchemy.Engine
    return Connection(user=user, password=password, host=host, port=port,
                      dbname=dbname, protocol=protocol)


class Connection(object):
    """Connect to your mapd database."""

    def __init__(self,
                 user=None,         # type: Optional[str]
                 password=None,     # type: Optional[str]
                 host=None,         # type: Optional[str]
                 port=9091,         # type: Optional[int]
                 dbname=None,       # type: Optional[str]
                 protocol='binary'  # type: Optional[str]
                 ):
        # type: (...) -> None
        if host is None:
            raise TypeError("`host` parameter is required.")
        if protocol == "http":
            if not host.startswith(protocol):
                # the THttpClient expects http[s]://localhost
                host = protocol + '://' + host
            transport = THttpClient.THttpClient("{}:{}".format(host, port))
            proto = TJSONProtocol.TJSONProtocol(transport)
            socket = None
        elif protocol == "binary":
            socket = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(socket)
            proto = TBinaryProtocol.TBinaryProtocol(transport)
        else:
            raise TypeError("`protocol` should be one of ['http', 'binary'], ",
                            "got {} instead".format(protocol))
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._dbname = dbname
        self._transport = transport
        self._protocol = protocol
        self._socket = socket
        self._closed = 0
        try:
            self._transport.open()
        except TTransportException as e:
            if e.NOT_OPEN:
                err = OperationalError("Could not connect to database")
                six.raise_from(err, e)
            else:
                raise
        self._client = Client(proto)
        try:
            self._session = self._client.connect(user, password, dbname)
        except TMapDException as e:
            six.raise_from(_translate_exception(e), e)

    def __repr__(self):
        # type: () -> str
        tpl = ('Connection(mapd://{user}:***@{host}:{port}/{dbname}?protocol'
               '={protocol})')
        return tpl.format(user=self._user, host=self._host, port=self._port,
                          dbname=self._dbname, protocol=self._protocol)

    def __del__(self):
        # type: () -> None
        self.close()

    def __enter__(self):
        return self.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def closed(self):
        return self._closed

    def close(self):
        # type: () -> None
        """Disconnect from the database"""
        try:
            self._client.disconnect(self._session)
        except (TMapDException, AttributeError):
            pass
        finally:
            self._closed = 1

    def commit(self):
        # type: () -> None
        """This is a noop, as mapd does not provide transactions.

        Implementing to comply with the specification.
        """
        return None

    def execute(self, operation, parameters=None):
        # type: (str, Optional[Tuple]) -> Cursor
        """Execute a SQL statement

        Parameters
        ----------
        operation : str
            A SQL statement to exucute

        Returns
        -------
        c : Cursor
        """
        c = Cursor(self)
        return c.execute(operation, parameters=parameters)

    def cursor(self):
        # type: () -> Cursor
        """Create a new :class:`Cursor` object attached to this connection."""
        return Cursor(self)

    def select_ipc(self, operation):
        """Execute a ``SELECT`` operation.
        """
        # TODO: figure out what `first_n` does, add to API
        return self._client.sql_execute_df(
            self._session, operation, first_n=-1)

    def select_ipc_gpu(self, operation, device_id=0):
        """Execute a ``SELECT`` operation.
        """
        # TODO: figure out what `first_n` does, add to API
        return self._client.sql_execute_gpudf(
            self._session, operation, device_id=device_id, first_n=-1)
