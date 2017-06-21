"""
Tests that rely on a server running
"""
import pytest
from thrift.transport import TSocket, TTransport
from thrift.transport.TSocket import TTransportException
from mapd.ttypes import TMapDException

from pymapd import connect, ProgrammingError, DatabaseError

socket = TSocket.TSocket("localhost", 9091)
transport = TTransport.TBufferedTransport(socket)

try:
    transport.open()
except TTransportException:
    pytestmark = pytest.mark.skip("mapd not running on localhost:9091. "
                                  "Skipping integration tests")

# XXX: Make it hashable to silence warnings; see if this can be done upstream
# This isn't a huge deal, but our testing context mangers for asserting
# exceptions need hashability
TMapDException.__hash__ = lambda x: id(x)


@pytest.fixture(scope='module')
def con():
    return connect(user="mapd", password='HyperInteractive', host='localhost',
                   port=9091, protocol='binary', dbname='mapd')


@pytest.mark.parametrize('protocol', [
    pytest.mark.skip(reason="Hangs waiting to hear back")('http'),
    'binary'])
def test_conenct(protocol):
    con = connect(user="mapd", password='HyperInteractive', host='localhost',
                  port=9091, protocol=protocol, dbname='mapd')
    assert con is not None
    assert protocol in repr(con)


class TestExceptions:

    def test_invalid_sql(self, con):
        with pytest.raises(ProgrammingError) as r:
            con.cursor().execute("select it;")
        r.match("Column 'it' not found in any table")

    def test_nonexistant_table(self, con):
        with pytest.raises(DatabaseError) as r:
            con.cursor().execute("select it from fake_table;")
        r.match("Table 'FAKE_TABLE' does not exist")
