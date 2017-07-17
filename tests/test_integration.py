"""
Tests that rely on a server running
"""
import pytest

from pymapd import connect, ProgrammingError, DatabaseError
from pymapd.cursor import Cursor
from pymapd.compat import TMapDException, TDataFrame

from .utils import no_gpu

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

    def test_connect_uri(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:9091/mapd?protocol='
               'binary')
        con = connect(uri=uri)
        assert con._user == 'mapd'
        assert con._password == 'HyperInteractive'
        assert con._host == 'localhost'
        assert con._port == 9091
        assert con._dbname == 'mapd'
        assert con._protocol == 'binary'

    def test_connect_uri_and_others_raises(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:9091/mapd?protocol='
               'binary')
        with pytest.raises(TypeError):
            connect(username='mapd', uri=uri)

    def test_invalid_sql(self, con):
        with pytest.raises(ProgrammingError) as r:
            con.cursor().execute("select it;")
        r.match("Column 'it' not found in any table")

    def test_nonexistant_table(self, con):
        with pytest.raises(DatabaseError) as r:
            con.cursor().execute("select it from fake_table;")
        r.match("Table 'FAKE_TABLE' does not exist")

    def test_connection_execute(self, con):
        result = con.execute("drop table if exists FOO;")
        result = con.execute("create table FOO (a int);")
        assert isinstance(result, Cursor)

    @pytest.mark.skipif(no_gpu, reason="No GPU available")
    def test_select_ipc(self, con, stocks):
        result = con.select_ipc("select qty, price from stocks")
        assert isinstance(result, TDataFrame)

    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_ipc_gpu(self, con, stocks):
        import pandas as pd
        import numpy as np
        from pygdf.dataframe import DataFrame

        result = con.select_ipc_gpu("select qty, price from stocks")
        assert isinstance(result, DataFrame)

        dtypes = dict(qty=np.int32, price=np.float32)
        expected = pd.DataFrame([[100, 35.14], [100, 12.14]],
                                columns=['qty', 'price']).astype(dtypes)

        result = result.to_pandas()[['qty', 'price']]  # column order
        pd.testing.assert_frame_equal(result, expected)
