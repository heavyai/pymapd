from typing import List, Tuple, Any, Union  # noqa

from thrift.transport import TSocket, THttpClient
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TJSONProtocol

from mapd import MapD

from .cursor import Cursor


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
        except (MapD.TMapDException, AttributeError):
            pass

    def commit(self):
        """This is a noop, as mapd does not provide transactions.

        Implementing to comply with the specification.
        """
        return None

    def cursor(self):
        return Cursor(self)
