import sys

from thrift.transport import TSocket, THttpClient
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TJSONProtocol

# XXX: distribute with package, or place in site-packages at install
sys.path.insert(0, "../mapd-core/gen-py")
import mapd            # noqa
from mapd import MapD  # noqa


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
    # TODO: accept a URI like sqlalchemy.Engine
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
        # TODO: single cursors per connection
        self.connection = connection
        self.columnar = columnar
        self.rowcount = -1
        self._description = None
        self._arraysize = 1
        self._result_set = None

    def __iter__(self):
        # TODO
        return iter(self.fetchall())

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
        self._result_set = result
        return self

    def executemany(self, operation, parameters=None):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, size, column=None):
        pass
