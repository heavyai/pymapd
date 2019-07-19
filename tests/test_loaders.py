import pytest
import datetime
from pymapd._loaders import _build_input_rows
from pymapd import _pandas_loaders
from omnisci.mapd.MapD import TStringRow, TStringValue, TColumn, TColumnData
import pandas as pd
import numpy as np
from omnisci.mapd.ttypes import TColumnType
from omnisci.common.ttypes import TTypeInfo


def assert_columnar_equal(result, expected):

    for i, (a, b) in enumerate(zip(result, expected)):
        np.testing.assert_array_equal(a.nulls, b.nulls)
        np.testing.assert_array_equal(a.data.int_col, b.data.int_col)
        np.testing.assert_array_equal(a.data.real_col, b.data.real_col)
        np.testing.assert_array_equal(a.data.str_col, b.data.str_col)


class TestLoaders:

    def test_build_input_rows(self):
        data = [(1, 'a'), (2, 'b')]
        result = _build_input_rows(data)
        expected = [TStringRow(cols=[TStringValue(str_val='1', is_null=None),
                                     TStringValue(str_val='a', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='2', is_null=None),
                                     TStringValue(str_val='b', is_null=None)])]

        assert result == expected

    def test_build_input_rows_with_array(self):
        data = [(1, 'a'), (2, 'b'), (3, ['c', 'd', 'e'])]
        result = _build_input_rows(data)
        expected = [TStringRow(cols=[TStringValue(str_val='1', is_null=None),
                                     TStringValue(str_val='a', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='2', is_null=None),
                                     TStringValue(str_val='b', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='3', is_null=None),
                                     TStringValue(str_val='{c,d,e}',
                                                  is_null=None)])]

        assert result == expected

    def test_build_table_columnar(self):

        from pymapd._pandas_loaders import build_input_columnar

        data = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        nulls = [False] * 3
        result = build_input_columnar(data, preserve_index=False)
        expected = [
            TColumn(TColumnData(int_col=[1, 2, 3]), nulls=nulls),
            TColumn(TColumnData(real_col=[1.1, 2.2, 3.3]), nulls=nulls)
        ]
        assert_columnar_equal(result[0], expected)

    def test_build_table_columnar_pandas(self):

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
        result = _pandas_loaders.build_input_columnar(data,
                                                      preserve_index=False)

        nulls = [False, False]
        expected = [
            TColumn(TColumnData(int_col=[True, False]), nulls=nulls),
            TColumn(TColumnData(int_col=np.array([0, 1], dtype=np.int16)), nulls=nulls),  # noqa
            TColumn(TColumnData(int_col=np.array([0, 1], dtype=np.int32)), nulls=nulls),  # noqa
            TColumn(TColumnData(int_col=np.array([0, 1], dtype=np.int64)), nulls=nulls),  # noqa
            TColumn(TColumnData(real_col=np.array([0, 1], dtype=np.float32)), nulls=nulls),  # noqa
            TColumn(TColumnData(real_col=np.array([0, 1], dtype=np.float64)), nulls=nulls),  # noqa
            TColumn(TColumnData(str_col=['a', 'b']), nulls=nulls),
            TColumn(TColumnData(str_col=['a', 'b']), nulls=nulls),
            TColumn(TColumnData(int_col=[719, 46800]), nulls=nulls),
            TColumn(TColumnData(int_col=[1451606400, 1483228800]), nulls=nulls),  # noqa
            TColumn(TColumnData(int_col=[1451606400, 1483228800]), nulls=nulls)
        ]
        assert_columnar_equal(result[0], expected)

    def test_build_table_columnar_nulls(self):

        data = pd.DataFrame({
            "boolean_": [True, False, None],
            # Currently Pandas does not support storing None or NaN
            # in integer columns, so int cols with null
            # need to be objects. This means our type detection will be
            # unreliable since if there is no number outside the int32
            # bounds in a column with nulls then we will be assuming int
            "int_": np.array([0, 1, None], dtype=np.object),
            "bigint_": np.array([0, 9223372036854775807, None],
                                dtype=np.object),
            "double_": np.array([0, 1, None], dtype=np.float64),
            "varchar_": ["a", "b", None],
            "text_": ['a', 'b', None],
            "time_": [datetime.time(0, 11, 59), datetime.time(13), None],
            "timestamp_": [pd.Timestamp("2016"), pd.Timestamp("2017"), None],
            "date_": [datetime.date(1001, 1, 1), datetime.date(2017, 1, 1),
                      None],
        }, columns=['boolean_', 'int_', 'bigint_',
                    'double_', 'varchar_', 'text_', 'time_', 'timestamp_',
                    'date_'])
        result = _pandas_loaders.build_input_columnar(data,
                                                      preserve_index=False)

        nulls = [False, False, True]
        bool_na = -128
        int_na = -2147483648
        bigint_na = -9223372036854775808
        ns_na = -9223372037
        double_na = 0

        expected = [
            TColumn(TColumnData(int_col=[1, 0, bool_na]), nulls=nulls),
            TColumn(TColumnData(int_col=np.array([0, 1, int_na], dtype=np.int32)), nulls=nulls),  # noqa
            TColumn(TColumnData(int_col=np.array([0, 9223372036854775807, bigint_na], dtype=np.int64)), nulls=nulls),  # noqa
            TColumn(TColumnData(real_col=np.array([0, 1, double_na], dtype=np.float64)), nulls=nulls),  # noqa
            TColumn(TColumnData(str_col=['a', 'b', '']), nulls=nulls),
            TColumn(TColumnData(str_col=['a', 'b', '']), nulls=nulls),
            TColumn(TColumnData(int_col=[719, 46800, bigint_na]), nulls=nulls),
            TColumn(TColumnData(int_col=[1451606400, 1483228800, ns_na]), nulls=nulls),  # noqa
            TColumn(TColumnData(int_col=[-30578688000, 1483228800, bigint_na]), nulls=nulls)  # noqa
        ]
        assert_columnar_equal(result[0], expected)

    def test_build_row_desc(self):

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
        result = _pandas_loaders.build_row_desc(data)
        expected = [
            TColumnType(col_name='boolean_',
                        col_type=TTypeInfo(type=10),
                        is_reserved_keyword=None),
            TColumnType(col_name='smallint_',
                        col_type=TTypeInfo(type=0),
                        is_reserved_keyword=None),
            TColumnType(col_name='int_',
                        col_type=TTypeInfo(type=1),
                        is_reserved_keyword=None),
            TColumnType(col_name='bigint_',
                        col_type=TTypeInfo(type=2)),
            TColumnType(col_name='float_',
                        col_type=TTypeInfo(type=3)),
            TColumnType(col_name='double_',
                        col_type=TTypeInfo(type=5)),
            TColumnType(col_name='varchar_',
                        col_type=TTypeInfo(type=6, encoding=4)),
            TColumnType(col_name='text_',
                        col_type=TTypeInfo(type=6, encoding=4)),
            TColumnType(col_name='time_',
                        col_type=TTypeInfo(type=7)),
            TColumnType(col_name='timestamp_',
                        col_type=TTypeInfo(type=8)),
            TColumnType(col_name='date_',
                        col_type=TTypeInfo(type=9))
        ]

        assert result == expected

        data.index.name = 'idx'
        result = _pandas_loaders.build_row_desc(data, preserve_index=True)
        expected.insert(0, TColumnType(col_name='idx',
                                       col_type=TTypeInfo(type=2)))

        assert result == expected

    def test_create_non_pandas_raises(self):
        with pytest.raises(TypeError) as m:
            _pandas_loaders.build_row_desc([(1, 'a'), (2, 'b')])

        assert m.match('is not supported for type ')
