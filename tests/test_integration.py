"""
Tests that rely on a server running
"""
import datetime
from distutils.version import LooseVersion
from unittest import mock

import pytest

from pymapd import connect, ProgrammingError, DatabaseError
from pymapd.cursor import Cursor
from pymapd._parsers import Description, ColumnDetails
from mapd.ttypes import TMapDException

from .utils import no_gpu

# XXX: Make it hashable to silence warnings; see if this can be done upstream
# This isn't a huge deal, but our testing context mangers for asserting
# exceptions need hashability
TMapDException.__hash__ = lambda x: id(x)


def skip_if_no_arrow_loader(con):
    mapd_version = LooseVersion(con._client.get_version())
    if mapd_version <= '3.3.1':
        pytest.skip("Arrow loader requires mapd > 3.3.1")


@pytest.mark.usefixtures("mapd_server")
class TestIntegration:

    @pytest.mark.parametrize('protocol',
                             [pytest.param('http',
                                           marks=pytest.mark.skip("Hangs \
                                           waiting to hear back")
                                           ), 'binary'])
    def test_connect(self, protocol):
        con = connect(user="mapd", password='HyperInteractive',
                      host='localhost', port=6274, protocol=protocol,
                      dbname='mapd')
        assert con is not None
        assert protocol in repr(con)

    def test_connect_uri(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:6274/mapd?protocol='
               'binary')
        con = connect(uri=uri)
        assert con._user == 'mapd'
        assert con._password == 'HyperInteractive'
        assert con._host == 'localhost'
        assert con._port == 6274
        assert con._dbname == 'mapd'
        assert con._protocol == 'binary'

    def test_connect_uri_and_others_raises(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:6274/mapd?protocol='
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
        expected = [('GOOG', 100), ]  # noqa
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
        from cudf.dataframe import DataFrame

        result = con.select_ipc_gpu("select qty, price from stocks")
        assert isinstance(result, DataFrame)

        dtypes = dict(qty=np.int32, price=np.float32)
        expected = pd.DataFrame([[100, 35.14], [100, 12.14]],
                                columns=['qty', 'price']).astype(dtypes)

        result = result.to_pandas()[['qty', 'price']]  # column order
        pd.testing.assert_frame_equal(result, expected)

    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_gpu_first_n(self, con, stocks):
        result = con.select_ipc_gpu("select * from stocks", first_n=1)
        assert len(result) == 1

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

    def test_select_dates(self, con, date_table):
        c = con.cursor()
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


class TestOptionalImports:

    def test_select_gpu(self, con):
        with mock.patch.dict("sys.modules",
                             {"cudf": None, "cudf.dataframe": None}):
            with pytest.raises(ImportError) as m:
                con.select_ipc_gpu("select * from foo;")
        assert m.match("The 'cudf' package is required")


class TestExtras:

    def test_get_tables(self, con, stocks):
        result = con.get_tables()
        assert isinstance(result, list)
        assert 'stocks' in result

    def test_get_table_details(self, con, stocks):
        result = con.get_table_details('stocks')
        expected = [
            ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT'),
            ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT'),
            ColumnDetails(name='symbol', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32,
                          encoding='DICT'),
            ColumnDetails(name='qty', type='INT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE'),
            ColumnDetails(name='price', type='FLOAT', nullable=True,
                          precision=0, scale=0, comp_param=0, encoding='NONE'),
            ColumnDetails(name='vol', type='FLOAT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE')
        ]
        assert result == expected


class TestLoaders:

    @staticmethod
    def check_empty_insert(result, expected):
        assert len(result) == 3
        assert expected[0][0] == result[0][0]
        assert expected[0][2] == result[0][2]
        assert abs(expected[0][1] - result[0][1]) < 1e-7  # floating point

    def test_load_empty_table(self, con, empty_table):
        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]
        con.load_table(empty_table, data)
        result = sorted(con.execute("select * from {}".format(empty_table)))
        self.check_empty_insert(result, data)

    def test_load_empty_table_pandas(self, con, empty_table):
        # TODO: just require arrow and pandas for tests
        pd = pytest.importorskip("pandas")

        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]
        df = pd.DataFrame(data, columns=list('abc'))
        con.load_table(empty_table, df, method='columnar')
        result = sorted(con.execute("select * from {}".format(empty_table)))
        self.check_empty_insert(result, data)

    def test_load_empty_table_arrow(self, con, empty_table):
        pd = pytest.importorskip("pandas")
        pa = pytest.importorskip("pyarrow")
        skip_if_no_arrow_loader(con)

        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]

        df = pd.DataFrame(data, columns=list('abc')).astype({
            'a': 'int32',
            'b': 'float32'
        })

        table = pa.Table.from_pandas(df, preserve_index=False)
        con.load_table(empty_table, table, method='arrow')
        result = sorted(con.execute("select * from {}".format(empty_table)))
        self.check_empty_insert(result, data)

    def test_load_table_columnar(self, con, empty_table):
        pd = pytest.importorskip("pandas")
        skip_if_no_arrow_loader(con)

        df = pd.DataFrame({"a": [1, 2, 3],
                           "b": [1.1, 2.2, 3.3],
                           "c": ['a', '2', '3']}, columns=['a', 'b', 'c'])
        con.load_table_columnar(empty_table, df)

    def test_load_infer(self, con, empty_table):
        pd = pytest.importorskip("pandas")
        skip_if_no_arrow_loader(con)
        import numpy as np

        data = pd.DataFrame(
            {'a': np.array([0, 1], dtype=np.int32),
             'b': np.array([1.1, 2.2], dtype=np.float32),
             'c': ['a', 'b']}
        )
        con.load_table(empty_table, data)

    def test_load_infer_bad(self, con, empty_table):
        with pytest.raises(TypeError):
            con.load_table(empty_table, [], method='thing')

    def test_infer_non_pandas(self, con, empty_table):
        with pytest.raises(TypeError):
            con.load_table(empty_table, [], method='columnar')

    def test_load_columnar_pandas_all(self, con, all_types_table):
        pd = pytest.importorskip("pandas")
        import numpy as np

        data = pd.DataFrame({
            "boolean_": [True, False],
            "smallint_": np.array([0, 1], dtype=np.int16),
            "int_": np.array([0, 1], dtype=np.int32),
            "bigint_": np.array([0, 1], dtype=np.int64),
            "float_": np.array([0, 1], dtype=np.float32),
            "double_": np.array([0, 1], dtype=np.float64),
            "varchar_": ["a", "b"],
            "text_": ['a', 'b'],
            "time_": [datetime.time(0, 11, 59), datetime.time(13)],
            "timestamp_": [pd.Timestamp("2016"), pd.Timestamp("2017")],
            "date_": [datetime.date(2016, 1, 1), datetime.date(2017, 1, 1)],
        }, columns=['boolean_', 'smallint_', 'int_', 'bigint_', 'float_',
                    'double_', 'varchar_', 'text_', 'time_', 'timestamp_',
                    'date_'])
        con.load_table_columnar(all_types_table, data, preserve_index=False)
        c = con.cursor()
        result = list(c.execute("select * from all_types"))
        expected = [(1, 0, 0, 0, 0.0, 0.0, 'a', 'a',
                    datetime.time(0, 11, 59),
                    datetime.datetime(2016, 1, 1, 0, 0),
                    datetime.date(2016, 1, 1)),
                    (0, 1, 1, 1, 1.0, 1.0, 'b', 'b',
                    datetime.time(13, 0),
                    datetime.datetime(2017, 1, 1, 0, 0),
                    datetime.date(2017, 1, 1))]

        assert result == expected

    def test_load_table_columnar_arrow_all(self, con, all_types_table):
        pa = pytest.importorskip("pyarrow")
        skip_if_no_arrow_loader(con)

        names = ['boolean_', 'smallint_', 'int_', 'bigint_',
                 'float_', 'double_', 'varchar_', 'text_',
                 'time_', 'timestamp_', 'date_']

        columns = [pa.array([True, False, None], type=pa.bool_()),
                   pa.array([1, 0, None]).cast(pa.int16()),
                   pa.array([1, 0, None]).cast(pa.int32()),
                   pa.array([1, 0, None]),
                   pa.array([1.0, 1.1, None]).cast(pa.float32()),
                   pa.array([1.0, 1.1, None]),
                   # no fixed-width string
                   pa.array(['a', 'b', None]),
                   pa.array(['a', 'b', None]),
                   (pa.array([1, 2, None]).cast(pa.int32())
                    .cast(pa.time32('s'))),
                   pa.array([datetime.datetime(2016, 1, 1, 12, 12, 12),
                             datetime.datetime(2017, 1, 1), None]),
                   pa.array([datetime.date(2016, 1, 1),
                             datetime.date(2017, 1, 1), None])]
        table = pa.Table.from_arrays(columns, names=names)
        con.load_table_arrow(all_types_table, table)

    def test_select_null(self, con):
        con.execute("drop table if exists pymapd_test_table;")
        con.execute("create table pymapd_test_table (a int);")
        con.execute("insert into pymapd_test_table VALUES (1);")
        con.execute("insert into pymapd_test_table VALUES (null);")
        # the test

        c = con.cursor()
        result = c.execute("select * from pymapd_test_table")
        expected = [(1,), (None,)]
        assert result.fetchall() == expected

        # cleanup
        con.execute("drop table if exists pymapd_test_table;")

    def test_create_table(self, con, not_a_table):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"A": [1, 2], "B": [1., 2.]})
        con.create_table(not_a_table, df)

    def test_load_table_creates(self, con, not_a_table):
        pd = pytest.importorskip("pandas")
        import numpy as np

        data = pd.DataFrame({
            "boolean_": [True, False],
            "smallint_cast": np.array([0, 1], dtype=np.int8),
            "smallint_": np.array([0, 1], dtype=np.int16),
            "int_": np.array([0, 1], dtype=np.int32),
            "bigint_": np.array([0, 1], dtype=np.int64),
            "float_": np.array([0, 1], dtype=np.float32),
            "double_": np.array([0, 1], dtype=np.float64),
            "varchar_": ["a", "b"],
            "text_": ['a', 'b'],
            "time_": [datetime.time(0, 11, 59), datetime.time(13)],
            "timestamp_": [pd.Timestamp("2016"), pd.Timestamp("2017")],
            "date_": [datetime.date(2016, 1, 1), datetime.date(2017, 1, 1)],
        }, columns=['boolean_', 'smallint_', 'int_', 'bigint_', 'float_',
                    'double_', 'varchar_', 'text_', 'time_', 'timestamp_',
                    'date_'])
        con.load_table(not_a_table, data, create=True)

    def test_array_in_result_set(self, con):

        # text
        con.execute("DROP TABLE IF EXISTS test_lists;")
        con.execute("CREATE TABLE IF NOT EXISTS test_lists \
                    (col1 TEXT, col2 TEXT[]);")

        row = [("row1", "{hello,goodbye,aloha}"),
               ("row2", "{hello2,goodbye2,aloha2}")]

        con.load_table_rowwise("test_lists", row)
        ans = con.execute("select * from test_lists").fetchall()

        expected = [('row1', ['hello', 'goodbye', 'aloha']),
                    ('row2', ['hello2', 'goodbye2', 'aloha2'])]

        assert ans == expected

        # int
        con.execute("DROP TABLE IF EXISTS test_lists;")
        con.execute("CREATE TABLE IF NOT EXISTS test_lists \
                    (col1 TEXT, col2 INT[]);")

        row = [("row1", "{10,20,30}"), ("row2", "{40,50,60}")]

        con.load_table_rowwise("test_lists", row)
        ans = con.execute("select * from test_lists").fetchall()

        expected = [('row1', [10, 20, 30]), ('row2', [40, 50, 60])]

        assert ans == expected

        # timestamp
        con.execute("DROP TABLE IF EXISTS test_lists;")
        con.execute("CREATE TABLE IF NOT EXISTS test_lists \
                    (col1 TEXT, col2 TIMESTAMP[]);")

        row = [("row1",
                "{2019-03-02 00:00:00,2019-03-02 00:00:00,2019-03-02 00:00:00}"),  # noqa
               ("row2",
                "{2019-03-02 00:00:00,2019-03-02 00:00:00,2019-03-02 00:00:00}")]  # noqa

        con.load_table_rowwise("test_lists", row)
        ans = con.execute("select * from test_lists").fetchall()

        expected = [('row1',
                    [datetime.datetime(2019, 3, 2, 0, 0),
                     datetime.datetime(2019, 3, 2, 0, 0),
                     datetime.datetime(2019, 3, 2, 0, 0)]),
                    ('row2',
                    [datetime.datetime(2019, 3, 2, 0, 0),
                     datetime.datetime(2019, 3, 2, 0, 0),
                     datetime.datetime(2019, 3, 2, 0, 0)])]

        assert ans == expected

        # date
        con.execute("DROP TABLE IF EXISTS test_lists;")
        con.execute("CREATE TABLE IF NOT EXISTS test_lists \
                    (col1 TEXT, col2 DATE[]);")

        row = [("row1", "{2019-03-02,2019-03-02,2019-03-02}"),
               ("row2", "{2019-03-02,2019-03-02,2019-03-02}")]

        con.load_table_rowwise("test_lists", row)
        ans = con.execute("select * from test_lists").fetchall()

        expected = [('row1',
                     [datetime.date(2019, 3, 2),
                      datetime.date(2019, 3, 2),
                      datetime.date(2019, 3, 2)]),
                    ('row2',
                     [datetime.date(2019, 3, 2),
                      datetime.date(2019, 3, 2),
                      datetime.date(2019, 3, 2)])]

        assert ans == expected

        # time
        con.execute("DROP TABLE IF EXISTS test_lists;")
        con.execute("CREATE TABLE IF NOT EXISTS test_lists \
                    (col1 TEXT, col2 TIME[]);")

        row = [("row1", "{23:59,23:59,23:59}"),
               ("row2", "{23:59,23:59,23:59}")]

        con.load_table_rowwise("test_lists", row)
        ans = con.execute("select * from test_lists").fetchall()

        expected = [('row1',
                     [datetime.time(23, 59), datetime.time(23, 59),
                      datetime.time(23, 59)]),
                    ('row2',
                     [datetime.time(23, 59), datetime.time(23, 59),
                      datetime.time(23, 59)])]

        assert ans == expected
