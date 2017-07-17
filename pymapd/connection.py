"""
Connect to a MapD database.
"""
import ctypes
import json
from typing import List, Tuple, Any, Union, Optional  # noqa
from collections import namedtuple

import six
from thrift.protocol import TBinaryProtocol, TJSONProtocol
from thrift.transport import TSocket, THttpClient, TTransport
from thrift.transport.TSocket import TTransportException
from mapd.MapD import Client
from mapd.ttypes import TMapDException

from .cursor import Cursor
from .exceptions import _translate_exception, OperationalError

ConnectionInfo = namedtuple("ConnectionInfo", ['user', 'password', 'host',
                                               'port', 'dbname', 'protocol'])


def connect(user=None,          # type: Optional[str]
            password=None,      # type: Optional[str]
            host=None,          # type: Optional[str]
            port=9091,          # type: Optional[int]
            dbname=None,        # type: Optional[str]
            protocol='binary',  # type: Optional[str]
            uri=None            # type: Optional[str]
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
                      dbname=dbname, protocol=protocol, uri=uri)


def _parse_uri(uri):
    # type: (str) -> ConnectionInfo
    """
    Parse connection string

    Parameters
    ----------
    uri : str
        a URI containing connection information

    Returns
    -------
    info : ConnectionInfo

    Notes
    ------
    The URI may include information on

    - user
    - password
    - host
    - port
    - dbname
    - protocol
    """
    try:
        from sqlalchemy.engine.url import make_url
    except ImportError:
        # TODO: We could remove this requirement and do the parsing ourselves
        raise ImportError("URI parsing requires SQLAlchemy")
    url = make_url(uri)
    user = url.username
    password = url.password
    host = url.host
    port = url.port
    dbname = url.database
    protocol = url.query.get('protocol', 'binary')

    return ConnectionInfo(user, password, host, port, dbname, protocol)


class Connection(object):
    """Connect to your mapd database."""

    def __init__(self,
                 user=None,          # type: Optional[str]
                 password=None,      # type: Optional[str]
                 host=None,          # type: Optional[str]
                 port=9091,          # type: Optional[int]
                 dbname=None,        # type: Optional[str]
                 protocol='binary',  # type: Optional[str]
                 uri=None            # type: Optional[str]
                 ):
        # type: (...) -> None
        if uri is not None:
            if not all([user is None,
                        password is None,
                        host is None,
                        port == 9091,
                        dbname is None,
                        protocol == 'binary']):
                raise TypeError("Cannot specify both URI and other arguments")
            user, password, host, port, dbname, protocol = _parse_uri(uri)
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
        tdf = self._client.sql_execute_df(
            self._session, operation, device_type=0, device_id=0,
            first_n=-1)
        return _parse_tdf_cpu(tdf)

    def select_ipc_gpu(self, operation, device_id=0):
        """Execute a ``SELECT`` operation.
        """
        # TODO: figure out what `first_n` does, add to API
        return self._client.sql_execute_gpudf(
            self._session, operation, device_id=device_id, first_n=-1)


def _parse_tdf_cpu(tdf):
    try:
        import pyarrow as pa  # noqa
    except ImportError:
        raise ImportError("Parsing CPU shared memory requires pyarrow. ")

    cptr = ctypes.c_void_p(_fish_sm_ptr(tdf))
    buffer_ = _read_schema(tdf, cptr)
    schema = _load_schema(buffer_)
    return schema


def _fish_sm_ptr(tdf):
    rt = ctypes.CDLL(None)
    smkey = _get_sm_key(tdf.sm_handle)

    shmget = rt.shmget
    shmget.argtypes = [ctypes.c_int, ctypes.c_size_t, ctypes.c_int]
    shmget.restype = ctypes.c_int

    shmat = rt.shmat
    shmat.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_void_p),
                      ctypes.c_int]
    shmat.restype = ctypes.c_void_p

    shmdt = rt.shmdt
    shmdt.argtypes = [ctypes.c_void_p]

    shmctl = rt.shmctl
    shmctl.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    shid = shmget(smkey, tdf.sm_size, 0)
    ptr = shmat(shid, None, 0)
    return ptr


def _get_sm_key(sm_handle):
    """Parse the shared memory key from a TDataFrame.sm_handle
    """
    import numpy as np
    return np.ndarray(shape=1, dtype=np.uint32, buffer=sm_handle)[0]


def _read_schema(tdf, cptr):
    import numpy as np

    start = ctypes.cast(cptr, ctypes.POINTER(ctypes.c_int8 * tdf.sm_size))[0]
    return np.ndarray(shape=tdf.sm_size, dtype=np.ubyte, buffer=start)


def _cast_schema(buffer_):
    """Convert from a numpy array of bytes to a pointer for the schema

    Parameters
    ----------
    buffer_ : np.array[uint8]

    Returns
    -------
    schema_ptr : CData <cdata 'void *'>
    """
    from libgdf_cffi import ffi
    return ffi.cast("void*", buffer_.ctypes.data)


def _load_schema(buffer_):
    from libgdf_cffi import ffi, libgdf
    ptr = _cast_schema(buffer_)
    ipch = libgdf.gdf_ipc_parser_open(ptr, buffer_.size)

    if libgdf.gdf_ipc_parser_failed(ipch):
        raise ValueError(libgdf.gdf_ipc_parser_get_error(ipch))

    jsonraw = libgdf.gdf_ipc_parser_get_schema_json(ipch)
    jsontext = ffi.string(jsonraw).decode()
    return json.loads(jsontext)
