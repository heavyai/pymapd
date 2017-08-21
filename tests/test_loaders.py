from pymapd._loaders import _build_input_rows
from mapd.MapD import TStringRow, TStringValue


class TestLoaders(object):

    def test_build_input_rows(self):
        data = [(1, 'a'), (2, 'b')]
        result = _build_input_rows(data)
        expected = [TStringRow(cols=[TStringValue(str_val='1', is_null=None),
                                     TStringValue(str_val='a', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='2', is_null=None),
                                     TStringValue(str_val='b', is_null=None)])]

        assert result == expected
