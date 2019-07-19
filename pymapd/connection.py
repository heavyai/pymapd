"""
Connect to an OmniSci database.
"""
from collections import namedtuple
import base64
import pandas as pd
import pyarrow as pa
import ctypes
from sqlalchemy.engine.url import make_url
from thrift.protocol import TBinaryProtocol, TJSONProtocol
from thrift.transport import TSocket, THttpClient, TTransport
from thrift.transport.TSocket import TTransportException
from omnisci.mapd.MapD import Client, TCreateParams
from omnisci.common.ttypes import TDeviceType
from omnisci.mapd.ttypes import TMapDException, TFileType

from .cursor import Cursor
from .exceptions import _translate_exception, OperationalError

from ._parsers import (
    _load_data, _load_schema, _parse_tdf_gpu, _bind_parameters,
    _extract_column_details
)

from ._loaders import _build_input_rows
from ._transforms import change_dashboard_sources
from .ipc import load_buffer, shmdt
from ._pandas_loaders import build_row_desc, _serialize_arrow_payload
from . import _pandas_loaders

from packaging.version import Version


ConnectionInfo = namedtuple("ConnectionInfo", ['user', 'password', 'host',
                                               'port', 'dbname', 'protocol'])


def connect(uri=None,
            user=None,
            password=None,
            host=None,
            port=6274,
            dbname=None,
            protocol='binary',
            sessionid=None,
            ):
    """
    Create a new Connection.

    Parameters
    ----------
    uri: str
    user: str
    password: str
    host: str
    port: int
    dbname: str
    protocol: {'binary', 'http', 'https'}
    sessionid: str

    Returns
    -------
    conn: Connection

    Examples
    --------
    You can either pass a string ``uri``, all the individual components,
    or an existing sessionid excluding user, password, and database

    >>> connect('mapd://admin:HyperInteractive@localhost:6274/omnisci?'
    ...         'protocol=binary')
    Connection(mapd://mapd:***@localhost:6274/mapd?protocol=binary)

    >>> connect(user='admin', password='HyperInteractive', host='localhost',
    ...         port=6274, dbname='omnisci')

    >>> connect(sessionid='XihlkjhdasfsadSDoasdllMweieisdpo', host='localhost',
    ...         port=6273, protocol='http')

    """
    return Connection(uri=uri, user=user, password=password, host=host,
                      port=port, dbname=dbname, protocol=protocol,
                      sessionid=sessionid)


def _parse_uri(uri):
    """
    Parse connection string

    Parameters
    ----------
    uri: str
        a URI containing connection information

    Returns
    -------
    info: ConnectionInfo

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


class Connection:
    """Connect to your OmniSci database."""

    def __init__(self,
                 uri=None,
                 user=None,
                 password=None,
                 host=None,
                 port=6274,
                 dbname=None,
                 protocol='binary',
                 sessionid=None,
                 ):
        if sessionid is not None:
            if any([user, password, uri, dbname]):
                raise TypeError("Cannot specify sessionid with user, password,"
                                " dbname, or uri")
        if uri is not None:
            if not all([user is None,
                        password is None,
                        host is None,
                        port == 6274,
                        dbname is None,
                        protocol == 'binary']):
                raise TypeError("Cannot specify both URI and other arguments")
            user, password, host, port, dbname, protocol = _parse_uri(uri)
        if host is None:
            raise TypeError("`host` parameter is required.")
        if protocol in ("http", "https"):
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
            raise ValueError("`protocol` should be one of",
                             " ['http', 'https', 'binary'],",
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
        self._tdf = None
        try:
            self._transport.open()
        except TTransportException as e:
            if e.NOT_OPEN:
                err = OperationalError("Could not connect to database")
                raise err from e
            else:
                raise
        self._client = Client(proto)
        try:
            # If a sessionid was passed, we should validate it
            if sessionid:
                self._session = sessionid
                self.get_tables()
                self.sessionid = sessionid
            else:
                self._session = self._client.connect(user, password, dbname)
                self.sessionid = None
        except TMapDException as e:
            raise _translate_exception(e) from e
        except TTransportException:
            raise ValueError(f"Connection failed with port {port} and "
                             f"protocol '{protocol}'. Try port 6274 for "
                             "protocol == binary or 6273, 6278 or 443 for "
                             "http[s]"
                             )

        # if OmniSci version <4.6, raise RuntimeError, as data import can be
        # incorrect for columnar date loads
        # Caused by https://github.com/omnisci/pymapd/pull/188
        semver = self._client.get_version()
        if Version(semver.split("-")[0]) < Version("4.6"):
            raise RuntimeError(f"Version {semver} of OmniSci detected. "
                               "Please use pymapd <0.11. See release notes "
                               "for more details.")

    def __repr__(self):
        tpl = ('Connection(omnisci://{user}:***@{host}:{port}/{dbname}?'
               'protocol={protocol})')
        return tpl.format(user=self._user, host=self._host, port=self._port,
                          dbname=self._dbname, protocol=self._protocol)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def closed(self):
        return self._closed

    def close(self):
        """Disconnect from the database unless created with sessionid"""
        if not self.sessionid:
            try:
                self._client.disconnect(self._session)
            except (TMapDException, AttributeError, TypeError):
                pass
        self._closed = 1

    def commit(self):
        """This is a noop, as OmniSci does not provide transactions.

        Implemented to comply with the DBI specification.
        """
        return None

    def execute(self, operation, parameters=None):
        """Execute a SQL statement

        Parameters
        ----------
        operation: str
            A SQL statement to exucute

        Returns
        -------
        c: Cursor
        """
        c = Cursor(self)
        return c.execute(operation.strip(), parameters=parameters)

    def cursor(self):
        """Create a new :class:`Cursor` object attached to this connection."""
        return Cursor(self)

    def select_ipc_gpu(self, operation, parameters=None, device_id=0,
                       first_n=-1, release_memory=True):
        """Execute a ``SELECT`` operation using GPU memory.

        Parameters
        ----------
        operation: str
            A SQL statement
        parameters: dict, optional
            Parameters to insert into a parametrized query
        device_id: int
            GPU to return results to
        first_n: int, optional
            Number of records to return
        release_memory: bool, optional
            Call ``self.deallocate_ipc_gpu(df)`` after DataFrame created

        Returns
        -------
        gdf: cudf.GpuDataFrame

        Notes
        -----
        This method requires ``cudf`` and ``libcudf`` to be installed.
        An ``ImportError`` is raised if those aren't available.

        This method requires the Python code to be executed on the same machine
        where OmniSci running.
        """
        try:
            from cudf.comm.gpuarrow import GpuArrowReader  # noqa
            from cudf.dataframe import DataFrame           # noqa
        except ImportError:
            raise ImportError("The 'cudf' package is required for "
                              "`select_ipc_gpu`")

        if parameters is not None:
            operation = str(_bind_parameters(operation, parameters))

        tdf = self._client.sql_execute_gdf(
            self._session, operation.strip(), device_id=device_id,
            first_n=first_n)
        self._tdf = tdf

        df = _parse_tdf_gpu(tdf)

        # Deallocate TDataFrame at OmniSci instance
        if release_memory:
            self.deallocate_ipc_gpu(df)

        return df

    def select_ipc(self, operation, parameters=None, first_n=-1,
                   release_memory=True):
        """Execute a ``SELECT`` operation using CPU shared memory

        Parameters
        ----------
        operation: str
            A SQL select statement
        parameters: dict, optional
            Parameters to insert for a parametrized query
        first_n: int, optional
            Number of records to return
        release_memory: bool, optional
            Call ``self.deallocate_ipc(df)`` after DataFrame created

        Returns
        -------
        df: pandas.DataFrame

        Notes
        -----
        This method requires the Python code to be executed on the same machine
        where OmniSci running.
        """

        if parameters is not None:
            operation = str(_bind_parameters(operation, parameters))

        tdf = self._client.sql_execute_df(
            self._session, operation.strip(), device_type=0, device_id=0,
            first_n=first_n
        )
        self._tdf = tdf

        sm_buf = load_buffer(tdf.sm_handle, tdf.sm_size)
        df_buf = load_buffer(tdf.df_handle, tdf.df_size)

        schema = _load_schema(sm_buf[0])
        df = _load_data(df_buf[0], schema, tdf)

        # free shared memory from Python
        # https://github.com/omnisci/pymapd/issues/46
        # https://github.com/omnisci/pymapd/issues/31
        free_sm = shmdt(ctypes.cast(sm_buf[1], ctypes.c_void_p))  # noqa
        free_df = shmdt(ctypes.cast(df_buf[1], ctypes.c_void_p))  # noqa

        # Deallocate TDataFrame at OmniSci instance
        if release_memory:
            self.deallocate_ipc(df)

        return df

    def deallocate_ipc_gpu(self, df, device_id=0):
        """Deallocate a DataFrame using GPU memory.

        Parameters
        ----------
        device_ids: int
            GPU which contains TDataFrame
        """

        tdf = df.get_tdf()
        result = self._client.deallocate_df(
            session=self._session,
            df=tdf,
            device_type=TDeviceType.GPU,
            device_id=device_id)
        return result

    def deallocate_ipc(self, df, device_id=0):
        """Deallocate a DataFrame using CPU shared memory.

        Parameters
        ----------
        device_id: int
            GPU which contains TDataFrame
        """
        tdf = df.get_tdf()
        result = self._client.deallocate_df(
            session=self._session,
            df=tdf,
            device_type=TDeviceType.CPU,
            device_id=device_id)
        return result

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
        table_name: str

        Returns
        -------
        details: List[tuples]

        Examples
        --------
        >>> con.get_table_details('stocks')
        [ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                       scale=0, comp_param=32, encoding='DICT'),
         ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                       scale=0, comp_param=32, encoding='DICT'),
         ...
        ]
        """
        details = self._client.get_table_details(self._session, table_name)
        return _extract_column_details(details.row_desc)

    def create_table(self, table_name, data, preserve_index=False):
        """Create a table from a pandas.DataFrame

        Parameters
        ----------
        table_name: str
        data: DataFrame
        preserve_index: bool, default False
            Whether to create a column in the table for the DataFrame index
        """

        row_desc = build_row_desc(data, preserve_index=preserve_index)
        self._client.create_table(self._session, table_name, row_desc,
                                  TFileType.DELIMITED, TCreateParams(False))

    def load_table(self, table_name, data, method='infer',
                   preserve_index=False,
                   create='infer'):
        """Load data into a table

        Parameters
        ----------
        table_name: str
        data: pyarrow.Table, pandas.DataFrame, or iterable of tuples
        method: {'infer', 'columnar', 'rows', 'arrow'}
            Method to use for loading the data. Three options are available

            1. ``pyarrow`` and Apache Arrow loader
            2. columnar loader
            3. row-wise loader

            The Arrow loader is typically the fastest, followed by the
            columnar loader, followed by the row-wise loader. If a DataFrame
            or ``pyarrow.Table`` is passed and ``pyarrow`` is installed, the
            Arrow-based loader will be used. If arrow isn't available, the
            columnar loader is used. Finally, ``data`` is an iterable of tuples
            the row-wise loader is used.

        preserve_index: bool, default False
            Whether to keep the index when loading a pandas DataFrame

        create: {"infer", True, False}
            Whether to issue a CREATE TABLE before inserting the data.

            * infer: check to see if the table already exists, and create
              a table if it does not
            * True: attempt to create the table, without checking if it exists
            * False: do not attempt to create the table

        See Also
        --------
        load_table_arrow
        load_table_columnar
        """

        if create not in ['infer', True, False]:
            raise ValueError(f"Unexpected value for create: '{create}'. "
                             "Expected one of {'infer', True, False}")

        if create == 'infer':
            # ask the database if we already exist, creating if not
            create = table_name not in set(
                self._client.get_tables(self._session))

        if create:
            self.create_table(table_name, data)

        if method == 'infer':
            if (isinstance(data, pd.DataFrame)
                or isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch)): # noqa
                return self.load_table_arrow(table_name, data)

            elif (isinstance(data, pd.DataFrame)):
                return self.load_table_columnar(table_name, data)

        elif method == 'arrow':
            return self.load_table_arrow(table_name, data)

        elif method == 'columnar':
            return self.load_table_columnar(table_name, data)

        elif method != 'rows':
            raise TypeError("Method must be one of {{'infer', 'arrow', "
                            "'columnar', 'rows'}}. Got {} instead"
                            .format(method))

        if isinstance(data, pd.DataFrame):
            # We need to convert a Pandas dataframe to a list of tuples before
            # loading row wise
            data = data.itertuples(index=preserve_index, name=None)

        input_data = _build_input_rows(data)
        self._client.load_table(self._session, table_name, input_data)

    def load_table_rowwise(self, table_name, data):
        """Load data into a table row-wise

        Parameters
        ----------
        table_name: str
        data: Iterable of tuples
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

    def load_table_columnar(
            self,
            table_name,
            data,
            preserve_index=False,
            chunk_size_bytes=0,
            col_names_from_schema=False
    ):
        """Load a pandas DataFrame to the database using OmniSci's Thrift-based
        columnar format

        Parameters
        ----------
        table_name: str
        data: DataFrame
        preserve_index: bool, default False
            Whether to include the index of a pandas DataFrame when writing.
        chunk_size_bytes: integer, default 0
            Chunk the loading of columns to prevent large Thrift requests. A
            value of 0 means do not chunk and send the dataframe as a single
            request
        col_names_from_schema: bool, default False
            Read the existing table schema to determine the column names. This
            will read the schema of an existing table in OmniSci and match
            those names to the column names of the dataframe. This is for
            user convenience when loading from data that is unordered,
            especially handy when a table has a large number of columns.

        Examples
        --------
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": ['d', 'e', 'f']})
        >>> con.load_table_columnar('foo', df, preserve_index=False)

        See Also
        --------
        load_table
        load_table_arrow
        load_table_rowwise

        Notes
        -----
        Use ``pymapd >= 0.11.0`` while running with ``omnisci >= 4.6.0`` in
        order to avoid loading inconsistent values into DATE column.
        """

        if isinstance(data, pd.DataFrame):
            table_details = self.get_table_details(table_name)
            # Validate that there are the same number of columns in the table
            # as there are in the dataframe. No point trying to load the data
            # if this is not the case
            if len(table_details) != len(data.columns):
                raise ValueError('Number of columns in dataframe ({}) does not \
                                  match number of columns in OmniSci table \
                                  ({})'.format(len(data.columns),
                                               len(table_details)))

            col_names = [i[0] for i in table_details] if \
                col_names_from_schema \
                else list(data)

            col_types = [(i[1], i[4]) for i in table_details]

            input_cols = _pandas_loaders.build_input_columnar(
                data,
                preserve_index=preserve_index,
                chunk_size_bytes=chunk_size_bytes,
                col_types=col_types,
                col_names=col_names
            )
        else:
            raise TypeError("Unknown type {}".format(type(data)))
        for cols in input_cols:
            self._client.load_table_binary_columnar(self._session, table_name,
                                                    cols)

    def load_table_arrow(self, table_name, data, preserve_index=False):
        """Load a pandas.DataFrame or a pyarrow Table or RecordBatch to the
        database using Arrow columnar format for interchange

        Parameters
        ----------
        table_name: str
        data: pandas.DataFrame, pyarrow.RecordBatch, pyarrow.Table
        preserve_index: bool, default False
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
        payload = _serialize_arrow_payload(data, metadata,
                                           preserve_index=preserve_index)
        self._client.load_table_binary_arrow(self._session, table_name,
                                             payload.to_pybytes())

    def render_vega(self, vega, compression_level=1):
        """Render vega data on the database backend,
        returning the image as a PNG.

        Parameters
        ----------

        vega: dict
            The vega specification to render.
        compression_level: int
            The level of compression for the rendered PNG. Ranges from
            0 (low compression, faster) to 9 (high compression, slower).
        """
        result = self._client.render_vega(
            self._session,
            widget_id=None,
            vega_json=vega,
            compression_level=compression_level,
            nonce=None
        )
        rendered_vega = RenderedVega(result)
        return rendered_vega

    def get_dashboards(self):
        """List all the dashboards in the database

        Examples
        --------
        >>> con.get_dashboards()
        """
        dashboards = self._client.get_dashboards(
            session=self._session
        )
        return dashboards

    def duplicate_dashboard(self, dashboard_id, new_name=None,
                            source_remap=None):
        """
        Duplicate an existing dashboard, returning the new dashboard id.

        Parameters
        ----------

        dashboard_id: int
            The id of the dashboard to duplicate
        new_name: str
            The name for the new dashboard
        source_remap: dict
            EXPERIMENTAL
            A dictionary remapping table names. The old table name(s)
            should be keys of the dict, with each value being another
            dict with a 'name' key holding the new table value. This
            structure can be used later to support changing column
            names.

        Examples
        --------
        >>> source_remap = {'oldtablename1': {'name': 'newtablename1'}, \
'oldtablename2': {'name': 'newtablename2'}}
        >>> newdash = con.duplicate_dashboard(12345, "new dash", source_remap)
        """
        source_remap = source_remap or {}
        d = self._client.get_dashboard(
            session=self._session,
            dashboard_id=dashboard_id
        )

        newdashname = new_name or '{0} (Copy)'.format(d.dashboard_name)
        d = change_dashboard_sources(d, source_remap) if source_remap else d

        new_dashboard_id = self._client.create_dashboard(
            session=self._session,
            dashboard_name=newdashname,
            dashboard_state=d.dashboard_state,
            image_hash='',
            dashboard_metadata=d.dashboard_metadata,
        )

        return new_dashboard_id


class RenderedVega:
    def __init__(self, render_result):
        self._render_result = render_result
        self.image_data = base64.b64encode(render_result.image).decode()

    def _repr_mimebundle_(self, include=None, exclude=None):
        return {
            'image/png': self.image_data,
            'text/html':
                '<img src="data:image/png;base64,{}" alt="OmniSci Vega">'
                .format(self.image_data)
        }
