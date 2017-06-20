from collections import namedtuple
from typing import List, Tuple, Any, Union  # noqa

from thrift.transport import TSocket, THttpClient
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TJSONProtocol

from mapd import MapD
import mapd.ttypes as T  # noqa


def connect(user=None, password=None, host=None, port=None,
            dbname=None, protocol='http'):
    """
    Crate a new Connection.

    Parameters
    ----------
    user : str
    password : str
    host : str
    port : int
    dbname : str
    protocol : {'http', 'binary'}

    Returns
    -------
    conn : Connection

    See Also
    --------
    Connect
    """
    # TODO: accept a dsn URI like sqlalchemy.Engine
    return Connection(user=user, password=password, host=host, port=port,
                      dbname=dbname, protocol=protocol)


class Connection(object):
    """Connect to your mapd database.
    """

    def __init__(self, user=None, password=None, host=None, port=9091,
                 dbname=None, protocol="http"):
        if protocol == "http":
            if not host.startswith(protocol):
                # the THttpClient expects http[s]://localhost
                host = protocol + '://' + host
            transport = THttpClient.THttpClient(host)
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
        self._transport.open()
        self._client = MapD.Client(proto)
        self._session = self._client.connect(user, password, dbname)

    def __repr__(self):
        tpl = ('Connection(mapd://{user}:***@{host}:{port}/{dbname}?protocol'
               '={protocol})')
        return tpl.format(user=self._user, host=self._host, port=self._port,
                          dbname=self._dbname, protocol=self._protocol)

    def __del__(self):
        self.close()

    def close(self):
        try:
            self._client.disconnect(self._session)
        except MapD.TMapDException:
            pass

    def commit(self):
        """This is a noop, as mapd does not provide transactions.

        Implementing to comply with the specification.
        """
        return None

    def cursor(self):
        return Cursor(self)


class Cursor(object):

    def __init__(self, connection, columnar=True):
        # XXX: supposed to share state between cursors of the same connection
        self.connection = connection
        self.columnar = columnar
        self.rowcount = -1
        self._description = None
        self._arraysize = 1
        self._result_set = None

    def __iter__(self):
        return self.result_set

    @property
    def description(self):
        return self._description

    @property
    def result_set(self):
        return self._result_set

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if not isinstance(value, int):
            raise TypeError("Value must be an integer, got {} instead".format(
                type(value)))
        self._arraysize = value

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        # column_format = is_columnar
        if parameters is not None:
            raise NotImplementedError
        self.rowcount = -1
        result = self.connection._client.sql_execute(
            self.connection._session, operation, column_format=self.columnar,
            nonce=None, first_n=-1)
        self._description = _extract_description(result.row_set.row_desc)
        if self.columnar:
            try:
                self.rowcount = len(result.row_set.columns[0].nulls)
            except IndexError:
                pass
        else:
            self.rowcount = len(result.row_set.rows)
        self._result_set = make_row_results_set(result)
        return self

    def executemany(self, operation, parameters=None):
        pass

    def fetchone(self):
        try:
            return next(self.result_set)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        results = [self.fetchone() for _ in range(size)]
        return [x for x in results if x is not None]

    def fetchall(self):
        return list(self)

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, size, column=None):
        pass


Description = namedtuple("Description", ["name", "type_code", "display_size",
                                         "internal_size", "precision", "scale",
                                         "null_ok"])


def make_row_results_set(data):
    # type: (T.QueryResultSet) -> List[Tuple]
    if is_columnar(data):
        nrows = len(data.row_set.columns[0].nulls)
        ncols = len(data.row_set.row_desc)
        columns = [_extract_col_vals(desc, col)
                   for desc, col in zip(data.row_set.row_desc,
                                        data.row_set.columns)]
        for i in range(nrows):
            yield tuple(columns[j][i] for j in range(ncols))
    else:
        for row in data.row_set.rows:
            yield tuple(_extract_row_val(desc, val)
                        for desc, val in zip(data.row_set.row_desc, row.cols))


def is_columnar(data):
    # type: (T.TQueryResult) -> bool
    return data.row_set.is_columnar


_typeattr = {
    'SMALLINT': 'int',
    'INT': 'int',
    'BIGINT': 'int',
    'TIME': 'int',
    'TIMESTAMP': 'int',
    'DATE': 'int',
    'BOOL': 'int',
    'FLOAT': 'real',
    'DECIMAL': 'real',
    'DOUBLE': 'real',
    'STR': 'str',
}


def _extract_row_val(desc, val):
    # type: (T.TColumnType, T.TDatum) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.val, _typeattr[typename] + '_val')


def _extract_col_vals(desc, val):
    # type: (T.TColumnType, T.TColumn) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.data, _typeattr[typename] + '_col')


def _extract_description(row_desc):
    """
    Return a tuple of (name, type_code, display_size, internal_size,
                       precision, scale, null_ok)

    https://www.python.org/dev/peps/pep-0249/#description
    """
    return [Description(col.col_name, col.col_type.type,
                        None, None, None, None,
                        col.col_type.nullable)
            for col in row_desc]
