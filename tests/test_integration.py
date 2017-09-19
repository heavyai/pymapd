"""
Tests that rely on a server running
"""
import datetime

import pytest

from pymapd import connect, ProgrammingError, DatabaseError
from pymapd.cursor import Cursor
from pymapd._parsers import Description, ColumnDetails
from pymapd.compat import TMapDException

from .utils import no_gpu, mock

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
            con.cursor().execute("this is invalid;")
        r.match("Exception: Parse failed:")

    def test_nonexistant_table(self, con):
        with pytest.raises(DatabaseError) as r:
            con.cursor().execute("select it from fake_table;")
        r.match("Table 'FAKE_TABLE' does not exist|Object 'fake_table' not")

    def test_connection_execute(self, con):
        result = con.execute("drop table if exists FOO;")
        result = con.execute("create table FOO (a int);")
        assert isinstance(result, Cursor)

    def test_select_sets_description(self, con, stocks):
        c = con.cursor()
        c.execute("select * from stocks")
        expected = [
            Description('date_', 6, None, None, None, None, True),
            Description('trans', 6, None, None, None, None, True),
            Description('symbol', 6, None, None, None, None, True),
            Description('qty', 1, None, None, None, None, True),
            Description('price', 3, None, None, None, None, True),
            Description('vol', 3, None, None, None, None, True),
        ]
        assert c.description == expected

    def test_select_parametrized(self, con, stocks):
        c = con.cursor()
        c.execute('select symbol, qty from stocks where symbol = :symbol',
                  {'symbol': 'GOOG'})
        result = list(c)
        expected = [('GOOG', 100),]  # noqa
        assert result == expected

    def test_executemany_parametrized(self, con, stocks):
        parameters = [{'symbol': 'GOOG'}, {'symbol': "RHAT"}]
        expected = [[('GOOG', 100)], [('RHAT', 100)]]
        query = 'select symbol, qty from stocks where symbol = :symbol'
        c = con.cursor()
        result = c.executemany(query, parameters)
        assert result == expected

    def test_executemany_parametrized_insert(self, con):
        c = con.cursor()
        c.execute("drop table if exists stocks2;")
        # Create table
        c.execute('CREATE TABLE stocks2 (symbol text, qty int);')
        params = [{"symbol": "GOOG", "qty": 10},
                  {"symbol": "AAPL", "qty": 20}]
        query = "INSERT INTO stocks2 VALUES (:symbol, :qty);"
        result = c.executemany(query, params)
        assert result == [[], []]  # TODO: not sure if this is standard
        c.execute("drop table stocks2;")

    @pytest.mark.parametrize('query, parameters', [
        ('select qty, price from stocks', None),
        ('select qty, price from stocks where qty=:qty', {'qty': 100}),
    ])
    def test_select_ipc_parametrized(self, con, stocks, query, parameters):
        pd = pytest.importorskip("pandas")
        import numpy as np
        import pandas.util.testing as tm

        result = con.select_ipc(query, parameters=parameters)
        expected = pd.DataFrame({
            "qty": np.array([100, 100], dtype=np.int32),
            "price": np.array([35.13999938964844, 12.140000343322754],
                              dtype=np.float32)
        })[['qty', 'price']]
        tm.assert_frame_equal(result, expected)

    @pytest.mark.xfail
    def test_select_ipc_first_n(self, con, stocks):
        pytest.importorskip("pandas")
        result = con.select_ipc("select * from stocks", first_n=1)
        assert len(result) == 1

    @pytest.mark.parametrize('query, parameters', [
        ('select qty, price from stocks', None),
        ('select qty, price from stocks where qty=:qty', {'qty': 100}),
    ])
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_ipc_gpu(self, con, stocks, query, parameters):
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

    @pytest.mark.xfail
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_gpu_first_n(self, con, stocks):
        result = con.select_ipc("select * from stocks", first_n=1)
        assert len(result) == 1

    def test_select_rowwise(self, con, stocks):
        c = con.cursor()
        c.columnar = False
        c.execute("select * from stocks;")
        assert c.rowcount == 2
        assert c.description is not None

    def test_fetchone(self, con, stocks):
        c = con.cursor()
        c.execute("select symbol, qty from stocks")
        result = c.fetchone()
        expected = ('RHAT', 100)
        assert result == expected

    def test_fetchmany(self, con, stocks):
        c = con.cursor()
        c.execute("select symbol, qty from stocks")
        result = c.fetchmany()
        expected = [('RHAT', 100)]
        assert result == expected

        c.execute("select symbol, qty from stocks")
        result = c.fetchmany(size=10)
        expected = [('RHAT', 100), ('GOOG', 100)]
        assert result == expected

    @pytest.mark.parametrize('columnar', [True, False])
    def test_select_dates(self, columnar, con, date_table):
        c = con.cursor()
        c.columnar = columnar
        result = list(c.execute("select * from {}".format(date_table)))
        expected = [
            (datetime.date(2006, 1, 5),
             datetime.datetime(2006, 1, 1, 12),
             datetime.time(12)),
            (datetime.date(1901, 12, 14),
             datetime.datetime(1901, 12, 13, 20, 45, 53),
             datetime.time(23, 59)),
        ]
        assert result == expected


class TestOptionalImports(object):

    def test_select_ipc_pyarrow(self, con):
        with mock.patch.dict('sys.modules', {'pyarrow': None}):
            with pytest.raises(ImportError) as m:
                con.select_ipc("select * from foo;")

        assert m.match("pyarrow is required for `select_ipc`")

    def test_select_ipc_pandas(self, con):
        pytest.importorskip("pyarrow")
        with mock.patch.dict('sys.modules', {'pandas': None}):
            with pytest.raises(ImportError) as m:
                con.select_ipc("select * from foo;")

        assert m.match("pandas is required for `select_ipc`")

    def test_select_gpu(self, con):
        with mock.patch.dict("sys.modules", {"pygdf": None}):
            with pytest.raises(ImportError) as m:
                con.select_ipc_gpu("select * from foo;")
        assert m.match("The 'pygdf' package is required")


class TestExtras(object):

    def test_get_tables(self, con, stocks):
        result = con.get_tables()
        assert isinstance(result, list)
        assert 'stocks' in result

    def test_get_table_details(self, con, stocks):
        result = con.get_table_details('stocks')
        expected = [
            ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32),
            ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32),
            ColumnDetails(name='symbol', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32),
            ColumnDetails(name='qty', type='INT', nullable=True, precision=0,
                          scale=0, comp_param=0),
            ColumnDetails(name='price', type='FLOAT', nullable=True,
                          precision=0, scale=0, comp_param=0),
            ColumnDetails(name='vol', type='FLOAT', nullable=True, precision=0,
                          scale=0, comp_param=0)
        ]
        assert result == expected

    def test_load_table(self, con, empty_table):
        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]
        con.load_table(empty_table, data)
        result = sorted(con.execute("select * from {}".format(empty_table)))
        assert len(result) == 3
        assert data[0][0] == result[0][0]
        assert data[0][2] == result[0][2]
        assert abs(data[0][1] - result[0][1]) < 1e-7  # floating point

    def test_load_table_binary(self, con, empty_table):
        pd = pytest.importorskip("pandas")
        from mapd.ttypes import TColumn, TColumnData

        nulls = [False] * 3
        data = [
            TColumn(TColumnData(int_col=[1, 2, 3]), nulls=nulls),
            TColumn(TColumnData(real_col=[1.1, 2.2, 3.3]), nulls=nulls),
            TColumn(TColumnData(str_col=['a', '2', '3']), nulls=nulls)
        ]
        con.load_table_columnar(empty_table, data)
