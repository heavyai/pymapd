"""
Tests that rely on a server running
"""
import pytest
from mapd.ttypes import TMapDException

from pymapd import connect, ProgrammingError, DatabaseError

# XXX: Make it hashable to silence warnings; see if this can be done upstream
# This isn't a huge deal, but our testing context mangers for asserting
# exceptions need hashability
TMapDException.__hash__ = lambda x: id(x)


@pytest.mark.usefixtures("mapd_server")
class TestIntegration:

    @pytest.mark.parametrize('protocol', [
        pytest.mark.skip(reason="Hangs waiting to hear back")('http'),
        'binary'])
    def test_conenct(self, protocol):
        con = connect(user="mapd", password='HyperInteractive',
                      host='localhost', port=9091, protocol=protocol,
                      dbname='mapd')
        assert con is not None
        assert protocol in repr(con)

    def test_invalid_sql(self, con):
        with pytest.raises(ProgrammingError) as r:
            con.cursor().execute("select it;")
        r.match("Column 'it' not found in any table")

    def test_nonexistant_table(self, con):
        with pytest.raises(DatabaseError) as r:
            con.cursor().execute("select it from fake_table;")
        r.match("Table 'FAKE_TABLE' does not exist")
