from pymapd._loaders import _build_input_rows, _build_input_rows_binary
from mapd.MapD import TStringRow, TStringValue, TRow, TDatum, TDatumVal


class TestLoaders(object):

    def test_build_input_rows(self):
        data = [(1, 'a'), (2, 'b')]
        result = _build_input_rows(data)
        expected = [TStringRow(cols=[TStringValue(str_val='1', is_null=None),
                                     TStringValue(str_val='a', is_null=None)]),
                    TStringRow(cols=[TStringValue(str_val='2', is_null=None),
                                     TStringValue(str_val='b', is_null=None)])]

        assert result == expected

    def test_build_input_rows_binary(self):
        types = ['INT', 'FLOAT', 'STR']
        data = [(1, 1.1, '1'), (2, 2.2, '2')]
        result = _build_input_rows_binary(data, types)

        expected = [TRow(cols=[TDatum(val=TDatumVal(int_val=1), is_null=None),
                               TDatum(val=TDatumVal(real_val=1.1), is_null=None),   # noqa
                               TDatum(val=TDatumVal(str_val='1'), is_null=None)]),  # noqa
                    TRow(cols=[TDatum(val=TDatumVal(int_val=2), is_null=None),
                               TDatum(val=TDatumVal(real_val=2.2), is_null=None),   # noqa
                               TDatum(val=TDatumVal(str_val='2'), is_null=None)])]  # noqa

        assert result == expected
