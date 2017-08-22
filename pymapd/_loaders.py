"""
Internal helpers for loading data
"""
from itertools import cycle
from mapd.ttypes import TStringRow, TStringValue, TRow, TDatumVal, TDatum


def _build_input_rows(data):
    input_data = []
    for row in data:
        input_row = TStringRow()
        input_row.cols = [TStringValue(str(x)) for x in row]
        input_data.append(input_row)
    return input_data


def _build_input_rows_binary(data, types):
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        if isinstance(data, pd.DataFrame):
            data = data.itertuples(index=False)

    types = cycle(types)

    binary_rows = []
    for row in data:
        trow = []
        for col, t in zip(row, types):
            if t == 'INT':
                trow.append(TDatum(TDatumVal(int_val=col)))
            elif t == 'FLOAT':
                trow.append(TDatum(TDatumVal(real_val=col)))
            elif t == 'STR':
                trow.append(TDatum(TDatumVal(str_val=col)))
        binary_rows.append(TRow(trow))
    return binary_rows


def _infer_type(column):
    from pandas.api.types import (is_any_int_dtype,
                                  is_float_dtype,
                                  is_string_dtype)

    if is_any_int_dtype(column):
        return 'int'
    elif is_float_dtype(column):
        return 'float'
    elif is_string_dtype(column):
        return 'string'
    else:
        raise TypeError("Unsupported type {}".format(type(column)))
