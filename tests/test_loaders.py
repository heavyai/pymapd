import pytest
import datetime

from pymapd._loaders import _build_input_rows
from pymapd import _pandas_loaders
from mapd.MapD import TStringRow, TStringValue, TColumn, TColumnData

from .utils import assert_columnar_equal


class TestLoaders(object):

    def test_build_input_rows(self):
        data = [(1, 'a'), (2, 'b')]
        result = _build_input_rows(data)
        expected = [TStringRow(cols=[TStringValue(str_val='1', is_null=None),
                                     TStringValue(str_val='a', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='2', is_null=None),
                                     TStringValue(str_val='b', is_null=None)])]

        assert result == expected

    def test_build_table_columnar(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        from pymapd._pandas_loaders import build_input_columnar

        data = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        nulls = [False] * 3
        result = build_input_columnar(data, preserve_index=False)
        expected = [
            TColumn(TColumnData(int_col=[1, 2, 3]), nulls=nulls),
            TColumn(TColumnData(real_col=[1.1, 2.2, 3.3]), nulls=nulls)
        ]
        assert_columnar_equal(result, expected)

    def test_build_table_columnar_pandas(self):
        import pandas as pd
        import numpy as np

        data = pd.DataFrame({
            "boolean_": [True, False],
            "smallint_": np.array([0, 1], dtype=np.int8),
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
            TColumn(TColumnData(int_col=np.array([0, 1], dtype=np.int8)), nulls=nulls),  # noqa
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
        assert_columnar_equal(result, expected)
