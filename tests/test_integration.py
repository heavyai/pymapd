"""
Tests that rely on a server running
"""
import pytest
from pymapd import connect


@pytest.mark.parametrize('protocol', ['http', 'binary'])
def test_conenct(protocol):
    con = connect(user="mapd", password='HyperInteractive', host='localhost',
                  port=9091, protocol=protocol, dbname='mapd')
    assert con is not None
    assert protocol in repr(con)
