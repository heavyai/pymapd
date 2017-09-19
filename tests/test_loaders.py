import pytest

from pymapd._loaders import _build_input_rows, _build_table_columnar
from mapd.MapD import TStringRow, TStringValue, TColumn, TColumnData


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
        data = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        nulls = [False] * 3
        result = _build_table_columnar(data, preserve_index=False)
        expected = [
            TColumn(TColumnData(int_col=[1, 2, 3]), nulls=nulls),
            TColumn(TColumnData(real_col=[1.1, 2.2, 3.3]), nulls=nulls)
        ]
        assert result == expected
