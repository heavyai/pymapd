"""
Tests that rely on a server running
"""
import datetime
from unittest import mock
import pytest
from pymapd import connect, ProgrammingError, DatabaseError
from pymapd.cursor import Cursor
from pymapd._parsers import Description, ColumnDetails
from omnisci.mapd.ttypes import TMapDException
from .data import dashboard_metadata
import json
import base64
import pandas as pd
import numpy as np
import pyarrow as pa
from pandas.api.types import is_object_dtype, is_categorical_dtype
import pandas.util.testing as tm
import textwrap
from .conftest import no_gpu

# XXX: Make it hashable to silence warnings; see if this can be done upstream
# This isn't a huge deal, but our testing context mangers for asserting
# exceptions need hashability
TMapDException.__hash__ = lambda x: id(x)


@pytest.mark.usefixtures("mapd_server")
class TestIntegration:

    def test_connect_binary(self):
        con = connect(user="admin", password='HyperInteractive',
                      host='localhost', port=6274, protocol='binary',
                      dbname='omnisci')
        assert con is not None

    def test_connect_http(self):
        con = connect(user="admin", password='HyperInteractive',
                      host='localhost', port=6278, protocol='http',
                      dbname='omnisci')
        assert con is not None

    def test_connect_uri(self):
        uri = ('omnisci://admin:HyperInteractive@localhost:6274/omnisci?'
               'protocol=binary')
        con = connect(uri=uri)
        assert con._user == 'admin'
        assert con._password == 'HyperInteractive'
        assert con._host == 'localhost'
        assert con._port == 6274
        assert con._dbname == 'omnisci'
        assert con._protocol == 'binary'

    def test_connect_uri_and_others_raises(self):
        uri = ('omnisci://admin:HyperInteractive@localhost:6274/omnisci?'
               'protocol=binary')
        with pytest.raises(TypeError):
            connect(username='omnisci', uri=uri)

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
        con.execute("drop table if exists FOO;")

    def test_select_sets_description(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

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
        c.execute('drop table if exists stocks;')

    def test_select_parametrized(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        c.execute('select symbol, qty from stocks where symbol = :symbol',
                  {'symbol': 'GOOG'})
        result = list(c)
        expected = [('GOOG', 100), ]  # noqa
        assert result == expected
        c.execute('drop table if exists stocks;')

    def test_executemany_parametrized(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        parameters = [{'symbol': 'GOOG'}, {'symbol': "RHAT"}]
        expected = [[('GOOG', 100)], [('RHAT', 100)]]
        query = 'select symbol, qty from stocks where symbol = :symbol'
        c = con.cursor()
        result = c.executemany(query, parameters)
        assert result == expected
        c.execute('drop table if exists stocks;')

    def test_executemany_parametrized_insert(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

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
        c.execute('drop table if exists stocks;')

    @pytest.mark.parametrize('query, parameters', [
        ('select qty, price from stocks', None),
        ('select qty, price from stocks where qty=:qty', {'qty': 100}),
    ])
    def test_select_ipc_parametrized(self, con, query, parameters):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.select_ipc(query, parameters=parameters)
        expected = pd.DataFrame({
            "qty": np.array([100, 100], dtype=np.int32),
            "price": np.array([35.13999938964844, 12.140000343322754],
                              dtype=np.float32)
        })[['qty', 'price']]
        tm.assert_frame_equal(result, expected)
        c.execute('drop table if exists stocks;')

    def test_select_ipc_first_n(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.select_ipc("select * from stocks", first_n=1)
        assert len(result) == 1
        c.execute('drop table if exists stocks;')

    @pytest.mark.parametrize('query, parameters', [
        ('select qty, price from stocks', None),
        ('select qty, price from stocks where qty=:qty', {'qty': 100}),
    ])
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_ipc_gpu(self, con, query, parameters):

        from cudf.dataframe import DataFrame

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.select_ipc_gpu("select qty, price from stocks")
        assert isinstance(result, DataFrame)

        dtypes = dict(qty=np.int32, price=np.float32)
        expected = pd.DataFrame([[100, 35.14], [100, 12.14]],
                                columns=['qty', 'price']).astype(dtypes)

        result = result.to_pandas()[['qty', 'price']]  # column order
        pd.testing.assert_frame_equal(result, expected)
        c.execute('drop table if exists stocks;')

    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_select_gpu_first_n(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.select_ipc_gpu("select * from stocks", first_n=1)
        assert len(result) == 1
        c.execute('drop table if exists stocks;')

    def test_fetchone(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        c.execute("select symbol, qty from stocks")
        result = c.fetchone()
        expected = ('RHAT', 100)
        assert result == expected
        c.execute('drop table if exists stocks;')

    def test_fetchmany(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        c.execute("select symbol, qty from stocks")
        result = c.fetchmany()
        expected = [('RHAT', 100)]
        assert result == expected

        c.execute("select symbol, qty from stocks")
        result = c.fetchmany(size=10)
        expected = [('RHAT', 100), ('GOOG', 100)]
        assert result == expected
        c.execute('drop table if exists stocks;')

    def test_select_dates(self, con):

        c = con.cursor()
        c.execute('drop table if exists dates;')
        c.execute('create table dates (date_ DATE, datetime_ TIMESTAMP, '
                  'time_ TIME);')
        i1 = ("INSERT INTO dates VALUES ('2006-01-05','2006-01-01T12:00:00',"
              "'12:00');")
        i2 = ("INSERT INTO dates VALUES ('1901-12-14','1901-12-13T20:45:53',"
              "'23:59:00');")
        c.execute(i1)
        c.execute(i2)

        result = list(c.execute("select * from dates"))
        expected = [
            (datetime.date(2006, 1, 5),
             datetime.datetime(2006, 1, 1, 12),
             datetime.time(12)),
            (datetime.date(1901, 12, 14),
             datetime.datetime(1901, 12, 13, 20, 45, 53),
             datetime.time(23, 59)),
        ]
        assert result == expected
        c.execute('drop table if exists dates;')

    def test_dashboard_duplication_remap(self, con):
        # This test relies on the test_data_no_nulls_ipc table
        # Setup our testing variables
        old_dashboard_state = dashboard_metadata.old_dashboard_state
        old_dashboard_name = dashboard_metadata.old_dashboard_name
        new_dashboard_name = "new_test"
        meta_data = {"table": "test_data_no_nulls_ipc", "version": "v2"}
        remap = {
                    "test_data_no_nulls_ipc": {
                        "name": new_dashboard_name,
                        "title": new_dashboard_name
                        }
                }
        dashboards = []

        # Create testing dashboard
        try:
            dashboard_id = con._client.create_dashboard(
                session=con._session,
                dashboard_name=old_dashboard_name,
                dashboard_state=(
                    base64.b64encode(json.dumps(old_dashboard_state).encode(
                        "utf-8"))),
                image_hash="",
                dashboard_metadata=json.dumps(meta_data),
            )
        except TMapDException:
            dashboards = con._client.get_dashboards(con._session)
            for dash in dashboards:
                if dash.dashboard_name == old_dashboard_name:
                    con._client.delete_dashboard(con._session,
                                                 dash.dashboard_id)
                    break
            dashboard_id = con._client.create_dashboard(
                session=con._session,
                dashboard_name=old_dashboard_name,
                dashboard_state=(
                    base64.b64encode(json.dumps(old_dashboard_state).encode(
                        "utf-8"))),
                image_hash="",
                dashboard_metadata=json.dumps(meta_data),
            )

        # Duplicate and remap our dashboard
        try:
            dashboard_id = con.duplicate_dashboard(
                    dashboard_id, new_dashboard_name, remap
                )
        except TMapDException:
            dashboards = con._client.get_dashboards(con._session)
            for dash in dashboards:
                if dash.dashboard_name == new_dashboard_name:
                    con._client.delete_dashboard(con._session,
                                                 dash.dashboard_id)
                    break
            dashboard_id = con.duplicate_dashboard(
                    dashboard_id, new_dashboard_name, remap
                )

        # Get our new dashboard from the database
        d = con._client.get_dashboard(
                                        session=con._session,
                                        dashboard_id=dashboard_id
                                    )
        remapped_dashboard = json.loads(base64.b64decode(
            d.dashboard_state).decode())

        # Assert that the table and title changed
        assert remapped_dashboard['dashboard']['title'] == new_dashboard_name

        # Ensure the datasources change
        for key, val in remapped_dashboard['dashboard']['dataSources'].items():
            for col in val['columnMetadata']:
                assert col['table'] == new_dashboard_name


class TestOptionalImports:

    def test_select_gpu(self, con):
        with mock.patch.dict("sys.modules",
                             {"cudf": None, "cudf.dataframe": None}):
            with pytest.raises(ImportError) as m:
                con.select_ipc_gpu("select * from foo;")
        assert m.match("The 'cudf' package is required")


class TestExtras:

    def test_get_tables(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float);')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.get_tables()
        assert isinstance(result, list)
        assert 'stocks' in result
        c.execute('drop table if exists stocks;')

    def test_get_table_details(self, con):

        c = con.cursor()
        c.execute('drop table if exists stocks;')
        create = ('create table stocks (date_ text, trans text, symbol text, '
                  'qty int, price float, vol float, '
                  'exchanges TEXT [] ENCODING DICT(32));')
        c.execute(create)
        i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1,{'NYSE', 'NASDAQ', 'AMEX'});"  # noqa
        i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2,{'NYSE', 'NASDAQ'});"  # noqa

        c.execute(i1)
        c.execute(i2)

        result = con.get_table_details('stocks')
        expected = [
            ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT',
                          is_array=False),
            ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT',
                          is_array=False),
            ColumnDetails(name='symbol', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32,
                          encoding='DICT', is_array=False),
            ColumnDetails(name='qty', type='INT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE',
                          is_array=False),
            ColumnDetails(name='price', type='FLOAT', nullable=True,
                          precision=0, scale=0, comp_param=0, encoding='NONE',
                          is_array=False),
            ColumnDetails(name='vol', type='FLOAT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE',
                          is_array=False),
            ColumnDetails(name='exchanges', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32, encoding='DICT',
                          is_array=True)
        ]
        assert result == expected
        c.execute('drop table if exists stocks;')


class TestLoaders:

    @staticmethod
    def check_empty_insert(result, expected):
        assert len(result) == 3
        assert expected[0][0] == result[0][0]
        assert expected[0][2] == result[0][2]
        assert abs(expected[0][1] - result[0][1]) < 1e-7  # floating point

    def test_load_empty_table(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]
        con.load_table("baz", data)
        result = sorted(con.execute("select * from baz"))
        self.check_empty_insert(result, data)
        con.execute("drop table if exists baz;")

    def test_load_empty_table_pandas(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]
        df = pd.DataFrame(data, columns=list('abc'))
        con.load_table("baz", df, method='columnar')
        result = sorted(con.execute("select * from baz"))
        self.check_empty_insert(result, data)
        con.execute("drop table if exists baz;")

    def test_load_empty_table_arrow(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        data = [(1, 1.1, 'a'),
                (2, 2.2, '2'),
                (3, 3.3, '3')]

        df = pd.DataFrame(data, columns=list('abc')).astype({
            'a': 'int32',
            'b': 'float32'
        })

        table = pa.Table.from_pandas(df, preserve_index=False)
        con.load_table("baz", table, method='arrow')
        result = sorted(con.execute("select * from baz"))
        self.check_empty_insert(result, data)
        con.execute("drop table if exists baz;")

    def test_load_table_columnar(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        df = pd.DataFrame({"a": [1, 2, 3],
                           "b": [1.1, 2.2, 3.3],
                           "c": ['a', '2', '3']}, columns=['a', 'b', 'c'])
        con.load_table_columnar("baz", df)
        con.execute("drop table if exists baz;")

    def test_load_infer(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        data = pd.DataFrame(
            {'a': np.array([0, 1], dtype=np.int32),
             'b': np.array([1.1, 2.2], dtype=np.float32),
             'c': ['a', 'b']}
        )
        con.load_table("baz", data)
        con.execute("drop table if exists baz;")

    def test_load_infer_bad(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        with pytest.raises(TypeError):
            con.load_table("baz", [], method='thing')

        con.execute("drop table if exists baz;")

    def test_infer_non_pandas(self, con):

        con.execute("drop table if exists baz;")
        con.execute("create table baz (a int, b float, c text);")

        with pytest.raises(TypeError):
            con.load_table("baz", [], method='columnar')

        con.execute("drop table if exists baz;")

    def test_load_columnar_pandas_all(self, con):

        c = con.cursor()
        c.execute('drop table if exists all_types;')
        create = textwrap.dedent('''\
        create table all_types (
            boolean_ BOOLEAN,
            smallint_ SMALLINT,
            int_ INT,
            bigint_ BIGINT,
            float_ FLOAT,
            double_ DOUBLE,
            varchar_ VARCHAR(40),
            text_ TEXT,
            time_ TIME,
            timestamp_ TIMESTAMP,
            date_ DATE
        );''')
        # skipping decimal for now
        c.execute(create)

        data = pd.DataFrame({
            "boolean_": [True, False, True, False],
            "smallint_": np.array([0, 1, 0, 1], dtype=np.int16),
            "int_": np.array([0, 1, 0, 1], dtype=np.int32),
            "bigint_": np.array([0, 1, 0, 1], dtype=np.int64),
            "float_": np.array([0, 1, 0, 1], dtype=np.float32),
            "double_": np.array([0, 1, 0, 1], dtype=np.float64),
            "varchar_": ["a", "b", "a", "b"],
            "text_": ['a', 'b', 'a', 'b'],
            "time_": [datetime.time(0, 11, 59), datetime.time(13),
                      datetime.time(22, 58, 59), datetime.time(7, 13, 43)],
            "timestamp_": [pd.Timestamp("2016"), pd.Timestamp("2017"),
                           pd.Timestamp('2017-11-28 23:55:59.342380',
                           tz='US/Eastern'),
                           pd.Timestamp('2018-11-28 23:55:59.342380',
                           tz='Asia/Calcutta')],
            "date_": [datetime.date(2016, 1, 1), datetime.date(2017, 1, 1),
                      datetime.date(2017, 11, 28),
                      datetime.date(2018, 11, 28)],
        }, columns=['boolean_', 'smallint_', 'int_', 'bigint_', 'float_',
                    'double_', 'varchar_', 'text_', 'time_', 'timestamp_',
                    'date_'])
        con.load_table_columnar("all_types", data, preserve_index=False)

        result = list(c.execute("select * from all_types"))
        expected = [(1, 0, 0, 0, 0.0, 0.0, 'a', 'a',
                    datetime.time(0, 11, 59),
                    datetime.datetime(2016, 1, 1, 0, 0),
                    datetime.date(2016, 1, 1)),
                    (0, 1, 1, 1, 1.0, 1.0, 'b', 'b',
                    datetime.time(13, 0),
                    datetime.datetime(2017, 1, 1, 0, 0),
                    datetime.date(2017, 1, 1)),
                    (1, 0, 0, 0, 0.0, 0.0, 'a', 'a',
                    datetime.time(22, 58, 59),
                    datetime.datetime(2017, 11, 29, 4, 55, 59),
                    datetime.date(2017, 11, 28)),
                    (0, 1, 1, 1, 1.0, 1.0, 'b', 'b',
                    datetime.time(7, 13, 43),
                    datetime.datetime(2018, 11, 28, 18, 25, 59),
                    datetime.date(2018, 11, 28))]

        assert result == expected
        c.execute('drop table if exists all_types;')

    def test_load_table_columnar_arrow_all(self, con):

        c = con.cursor()
        c.execute('drop table if exists all_types;')
        create = textwrap.dedent('''\
        create table all_types (
            boolean_ BOOLEAN,
            smallint_ SMALLINT,
            int_ INT,
            bigint_ BIGINT,
            float_ FLOAT,
            double_ DOUBLE,
            varchar_ VARCHAR(40),
            text_ TEXT,
            time_ TIME,
            timestamp_ TIMESTAMP,
            date_ DATE
        );''')
        # skipping decimal for now
        c.execute(create)

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
        con.load_table_arrow("all_types", table)
        c.execute('drop table if exists all_types;')

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

    def test_create_table(self, con):

        con.execute("drop table if exists test_create_table;")
        df = pd.DataFrame({"A": [1, 2], "B": [1., 2.]})
        con.create_table("test_create_table", df)
        con.execute("drop table if exists test_create_table;")

    def test_load_table_creates(self, con):

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

        con.execute("drop table if exists test_load_table_creates;")
        con.load_table("test_load_table_creates", data, create=True)
        con.execute("drop table if exists test_load_table_creates;")

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
        con.execute("DROP TABLE IF EXISTS test_lists;")

    def test_upload_pandas_categorical(self, con):

        con.execute("DROP TABLE IF EXISTS test_categorical;")

        df = pd.DataFrame({"A": ["a", "b", "c", "a"]})
        df["B"] = df["A"].astype('category')

        # test that table created correctly when it doesn't exist on server
        con.load_table("test_categorical", df)
        ans = con.execute("select * from test_categorical").fetchall()

        assert ans == [('a', 'a'), ('b', 'b'), ('c', 'c'), ('a', 'a')]

        assert con.get_table_details("test_categorical") == \
            [ColumnDetails(name='A', type='STR', nullable=True, precision=0,
                           scale=0, comp_param=32, encoding='DICT',
                           is_array=False),
             ColumnDetails(name='B', type='STR', nullable=True, precision=0,
                           scale=0, comp_param=32, encoding='DICT',
                           is_array=False)]

        # load row-wise
        con.load_table("test_categorical", df, method="rows")

        # load columnar
        con.load_table("test_categorical", df, method="columnar")

        # load arrow
        con.load_table("test_categorical", df, method="arrow")

        # test end result
        df_ipc = con.select_ipc("select * from test_categorical")
        assert df_ipc.shape == (16, 2)

        res = df.append([df, df, df]).reset_index(drop=True)
        res["A"] = res["A"].astype('category')
        res["B"] = res["B"].astype('category')
        assert pd.DataFrame.equals(df_ipc, res)

        # test that input df wasn't mutated
        # original input is object, categorical
        # to load via Arrow, converted internally to object, object
        assert is_object_dtype(df["A"])
        assert is_categorical_dtype(df["B"])
        con.execute("DROP TABLE IF EXISTS test_categorical;")

    def test_insert_unicode(self, con):

        """INSERT Unicode using bind_params"""

        c = con.cursor()
        c.execute('drop table if exists text_holder;')
        create = ('create table text_holder (the_text text);')
        c.execute(create)
        first = {"value": "我和我的姐姐吃米饭鸡肉"}
        second = {"value": "El camina a case en bicicleta es relajante"}

        i1 = "INSERT INTO text_holder VALUES ( :value );"

        c.execute(i1, parameters=first)
        c.execute(i1, parameters=second)

        c.execute('drop table if exists text_holder;')
