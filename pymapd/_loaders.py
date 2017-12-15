"""
Internal helpers for loading data
"""
from mapd.ttypes import TStringRow, TStringValue


def _build_input_rows(data):
    input_data = []
    for row in data:
        input_row = TStringRow()
        input_row.cols = ["{"+",".join(str(y) for y in x)+"}" if type(x)==list else TStringValue(str(x)) for x in row]
        input_data.append(input_row)
    return input_data
