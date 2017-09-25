"""
Connect to a MapD database.
"""
from collections import namedtuple

import six
from sqlalchemy.engine.url import make_url
from thrift.protocol import TBinaryProtocol, TJSONProtocol
from thrift.transport import TSocket, THttpClient, TTransport
from thrift.transport.TSocket import TTransportException
from mapd.MapD import Client
from mapd.ttypes import TMapDException

from .cursor import Cursor
from .exceptions import _translate_exception, OperationalError

from ._parsers import (
    _load_data, _load_schema, _parse_tdf_gpu, _bind_parameters,
    _extract_column_details
)
from ._loaders import _build_input_rows

try:
    import pyarrow as pa
    _HAS_ARROW = True
except ImportError:
    _HAS_ARROW = False


ConnectionInfo = namedtuple("ConnectionInfo", ['user', 'password', 'host',
                                               'port', 'dbname', 'protocol'])


def connect(uri=None,           # type: Optional[str]
            user=None,          # type: Optional[str]
            password=None,      # type: Optional[str]
            host=None,          # type: Optional[str]
            port=9091,          # type: Optional[int]
            dbname=None,        # type: Optional[str]
            protocol='binary',  # type: Optional[str]
            ):
    # type: (...) -> Connection
    """
    Crate a new Connection.

    Parameters
    ----------
    uri : str
    user : str
    password : str
    host : str
    port : int
    dbname : str
    protocol : {'binary', 'http'}

    Returns
    -------
    conn : Connection

    Examples
    --------
    You can either pass a string ``uri`` or all the individual components

    >>> connect('mapd://mapd:HyperInteractive@localhost:9091/mapd?'
    ...         'protocol=binary')
    Connection(mapd://mapd:***@localhost:9091/mapd?protocol=binary)

    >>> connect(user='mapd', password='HyperInteractive', host='localhost',
    ...         port=9091, dbname='mapd')

    """
    return Connection(uri=uri, user=user, password=password, host=host,
                      port=port, dbname=dbname, protocol=protocol)


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
                 uri=None,           # type: Optional[str]
                 user=None,          # type: Optional[str]
                 password=None,      # type: Optional[str]
                 host=None,          # type: Optional[str]
                 port=9091,          # type: Optional[int]
                 dbname=None,        # type: Optional[str]
                 protocol='binary',  # type: Optional[str]
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
            proto = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        else:
            raise ValueError("`protocol` should be one of ['http', 'binary'],",
                             " got {} instead".format(protocol))
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

    def select_ipc_gpu(self, operation, parameters=None, device_id=0,
                       first_n=-1):
        """Execute a ``SELECT`` operation using GPU memory.

        Parameters
        ----------
        operation : str
            A SQL statement
        parameters : dict, optional
            Parameters to insert into a parametrized query
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
        try:
            from pygdf.gpuarrow import GpuArrowReader  # noqa
            from pygdf.dataframe import DataFrame      # noqa
        except ImportError:
            raise ImportError("The 'pygdf' package is required for "
                              "`select_ipc_gpu`")

        if parameters is not None:
            operation = str(_bind_parameters(operation, parameters))

        tdf = self._client.sql_execute_gdf(
            self._session, operation, device_id=device_id, first_n=first_n)
        return _parse_tdf_gpu(tdf)

    def select_ipc(self, operation, parameters=None, first_n=-1):
        """Execute a ``SELECT`` operation using CPU shared memory

        Parameters
        ----------
        operation : str
            A SQL select statement
        parameters : dict, optional
            Parameters to insert for a parametrized query

        Returns
        -------
        df : pandas.DataFrame

        Notes
        -----
        This method requires pandas and pyarrow to be installed
        """
        try:
            import pyarrow  # noqa
        except ImportError:
            raise ImportError("pyarrow is required for `select_ipc`")

        try:
            import pandas  # noqa
        except ImportError:
            raise ImportError("pandas is required for `select_ipc`")

        from .shm import load_buffer

        if parameters is not None:
            operation = str(_bind_parameters(operation, parameters))

        tdf = self._client.sql_execute_df(
            self._session, operation, device_type=0, device_id=0,
            first_n=first_n
        )

        sm_buf = load_buffer(tdf.sm_handle, tdf.sm_size)
        df_buf = load_buffer(tdf.df_handle, tdf.df_size)

        schema = _load_schema(sm_buf)
        df = _load_data(df_buf, schema)
        return df

    # --------------------------------------------------------------------------
    # Convenience methods
    # --------------------------------------------------------------------------
    def get_tables(self):
        """List all the tables in the database

        Examples
        --------
        >>> con.get_tables()
        ['flights_2008_10k', 'stocks']
        """
        return self._client.get_tables(self._session)

    def get_table_details(self, table_name):
        """Get the column names and data types associated with a table.

        Parameters
        ----------
        table_name : str

        Returns
        -------
        details : List[tuples]

        Examples
        --------
        >>> con.get_table_details('stocks')
        [ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                       scale=0, comp_param=32),
         ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                       scale=0, comp_param=32),
         ...
        ]
        """
        details = self._client.get_table_details(self._session, table_name)
        return _extract_column_details(details.row_desc)

    def load_table(self, table_name, data, method='infer',
                   preserve_index=False):
        """Load data into a table

        Parameters
        ----------
        table_name : str
        data : pyarrow.Table, pandas.DataFrame, or iterable of tuples
        method : {'infer', 'columnar', 'rows'}
            Method to use for loading the data. Three options are available

            1. ``pyarrow`` and Apache Arrow loader
            2. columnar loader
            3. row-wise loader

            The Arrow loader is typically the fastest, followed by the
            columnar loader, followed by the row-wise loader. If a DataFrame
            or ``pyarrow.Table`` is passed and ``pyarrow`` is installed, the
            Arrow-based loader will be used. If arrow isn't available, the
            columnar loader is used. Finally, ``data`` is an iterable of tuples,
            the row-wise loader is used.
        preserve_index : bool, default False
            Whether to keep the index when loading a pandas DataFrame

        See Also
        --------
        load_table_arrow
        load_table_columnar
        """
        if method == 'infer':
            if (_is_pandas(data) or _is_arrow(data)) and _HAS_ARROW:
                return self.load_table_arrow(table_name, data)

            elif _is_pandas(data):
                return self.load_table_columnar(table_name, data)

        elif method == 'columnar' and not _is_pandas(data):
            raise ValueError("'data' must be a DataFrame with "
                             "`method='columnar'`. Got {} instead".format(
                                 type(data)
                             ))
        elif method != 'rows':
            raise ValueError("Method must be one of {{'infer', 'columnar', "
                             "'rows'}}. Got {} instead".format(method))

        input_data = _build_input_rows(data)
        self._client.load_table(self._session, table_name, input_data)

    def load_table_rowwise(self, table_name, data):
        """Load data into a table row-wise

        Parameters
        ----------
        table_name : str
        data : Iterable of tuples
            Each element of `data` should be a row to be inserted

        See Also
        --------
        load_table
        load_table_arrow
        load_table_columnar

        Examples
        --------
        >>> data = [(1, 'a'), (2, 'b'), (3, 'c')]
        >>> con.load_table('bar', data)
        """
        input_data = _build_input_rows(data)
        self._client.load_table(self._session, table_name, input_data)

    def load_table_columnar(self, table_name, data, preserve_index=False):
        """Load a pandas DataFrame to the database using MapD's Thrift-based
        columnar format

        Parameters
        ----------
        table_name : str
        data : DataFrame
        preserve_index : bool, default False
            Whether to include the index of a pandas DataFrame when writing.

        Examples
        --------
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": ['d', 'e', 'f']})
        >>> con.load_table_columnar('foo', df, preserve_index=False)

        See Also
        --------
        load_table
        load_table_arrow
        load_table_rowwise
        """
        from . import _pandas_loaders

        if _is_pandas(data):
            input_cols = _pandas_loaders.build_input_columnar(
                data, preserve_index=preserve_index
            )
        else:
            raise TypeError("Unknown type {}".format(type(data)))
        self._client.load_table_binary_columnar(self._session, table_name,
                                                input_cols)

    def load_table_arrow(self, table_name, data, preserve_index=False):
        """Load a pandas.DataFrame or a pyarrow Table or RecordBatch to the
        database using Arrow columnar format for interchange

        Parameters
        ----------
        table_name : str
        data : pandas.DataFrame, pyarrow.RecordBatch, pyarrow.Table
        preserve_index : bool, default False
            Whether to include the index of a pandas DataFrame when writing.

        Examples
        --------
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": ['d', 'e', 'f']})
        >>> con.load_table_arrow('foo', df, preserve_index=False)

        See Also
        --------
        load_table
        load_table_columnar
        load_table_rowwise
        """
        metadata = self.get_table_details(table_name)
        from ._pandas_loaders import _serialize_arrow_payload
        payload = _serialize_arrow_payload(data, metadata,
                                           preserve_index=preserve_index)
        self._client.load_table_binary_arrow(self._session, table_name,
                                             payload.to_pybytes())


def _is_pandas(data):
    try:
        import pandas as pd
    except ImportError:
        return False
    else:
        return isinstance(data, pd.DataFrame)


def _is_arrow(data):
    """Whether `data` is an arrow `Table` or `RecordBatch`"""
    if _HAS_ARROW:
        return isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch)
    return False
