"""
Internal helpers for loading data
"""
from omnisci.thrift.ttypes import TStringRow, TStringValue
import collections


def _build_input_rows(data):
    input_data = []
    for row in data:
        input_row = TStringRow()
        input_row.cols = [
            TStringValue("{" + ",".join(str(y) for y in x) + "}")
            if isinstance(x, collections.abc.Sequence)
            and not isinstance(x, str)
            else TStringValue(str(x))
            for x in row
        ]
        input_data.append(input_row)
    return input_data
