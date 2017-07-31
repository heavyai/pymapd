"""
Connect to a MapD database.
"""
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

    def select_ipc_gpu(self, operation, device_id=0):
        """Execute a ``SELECT`` operation using GPU memory.

        Parameters
        ----------
        operation : str
            A SQL statement
        device_id : int
            GPU to return results to

        Returns
        -------
        gdf : pygdf.GpuDataFrame

        Notes
        -----
        This requires the option ``pygdf`` and ``libgdf`` libraries.
        An ``ImportError`` is raised if those aren't available.
        """
        # TODO: figure out what `first_n` does, add to API
        try:
            from pygdf.gpuarrow import GpuArrowReader  # noqa
            from pygdf.dataframe import DataFrame      # noqa
        except ImportError:
            raise ImportError("Install pygdf")

        tdf = self._client.sql_execute_gdf(
            self._session, operation, device_id=device_id, first_n=-1)
        return _parse_tdf_gpu(tdf)

    def select_ipc(self, operation):
        """Execute a ``SELECT`` operation.
        """
        # TODO: accept first_n
        from .shm import load_buffer
        tdf = self._client.sql_execute_df(
            self._session, operation, device_type=0, device_id=0, first_n=-1)

        sm_buf = load_buffer(tdf.sm_handle, tdf.sm_size)
        df_buf = load_buffer(tdf.df_handle, tdf.df_size)
        schema = _load_schema(sm_buf)
        df = _load_data(df_buf, schema)
        return df


def _load_schema(buf):
    """
    Load a `pyarrow.Schema` from a buffer written to shared memory

    Parameters
    ----------
    buf : pyarrow.Buffer

    Returns
    -------
    schema : pyarrow.Schema
    """
    import pyarrow as pa

    reader = pa.RecordBatchStreamReader(buf)
    return reader.schema


def _load_data(buf, schema):
    """
    Load a `pandas.DataFrame` from a buffer written to shared memory

    Parameters
    ----------
    buf : pyarrow.Buffer
    shcema : pyarrow.Schema

    Returns
    -------
    df : pandas.DataFrame
    """
    import pyarrow as pa

    message = pa.read_message(buf)
    rb = pa.read_record_batch(message, schema)
    return rb.to_pandas()


def _parse_tdf_gpu(tdf):
    """
    Parse the results of a select ipc_gpu into a GpuDataFrame

    Parameters
    ----------
    tdf : TDataFrame

    Returns
    -------
    gdf : GpuDataFrame
    """
    import numpy as np
    from pygdf.gpuarrow import GpuArrowReader
    from pygdf.dataframe import DataFrame
    from numba import cuda
    from numba.cuda.cudadrv import drvapi

    from .shm import load_buffer

    ipc_handle = drvapi.cu_ipc_mem_handle(*tdf.df_handle)
    ipch = cuda.driver.IpcHandle(None, ipc_handle, size=tdf.df_size)
    ctx = cuda.current_context()
    dptr = ipch.open(ctx)

    schema_buffer = load_buffer(tdf.sm_handle, tdf.sm_size)
    dtype = np.dtype(np.byte)
    darr = cuda.devicearray.DeviceNDArray(shape=dptr.size,
                                          strides=dtype.itemsize,
                                          dtype=dtype,
                                          gpu_data=dptr)
    reader = GpuArrowReader(schema_buffer, darr)
    df = DataFrame()
    for k, v in reader.to_dict().items():
        df[k] = v

    return df
