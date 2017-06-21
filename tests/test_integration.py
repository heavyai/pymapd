"""
Tests that rely on a server running
"""
import pytest
from thrift.transport import TSocket, TTransport
from thrift.transport.TSocket import TTransportException

from pymapd import connect

socket = TSocket.TSocket("localhost", 9091)
transport = TTransport.TBufferedTransport(socket)

try:
    transport.open()
except TTransportException:
    pytestmark = pytest.mark.skip("mapd not running on localhost:9091. "
                                  "Skipping integration tests")


@pytest.mark.parametrize('protocol', ['http', 'binary'])
def test_conenct(protocol):
    con = connect(user="mapd", password='HyperInteractive', host='localhost',
                  port=9091, protocol=protocol, dbname='mapd')
    assert con is not None
    assert protocol in repr(con)
