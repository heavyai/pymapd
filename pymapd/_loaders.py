"""
Internal helpers for loading data
"""
from mapd.ttypes import TStringRow, TStringValue, TColumnData, TColumn
from ._parsers import _typeattr
import pyarrow as pa
import numpy as np

_pa_typemap = {
    pa.int64().id: 'BIGINT',
    pa.bool_().id: 'BOOL',
    pa.date64().id: 'DATE',
    # 'DECIMAL': pa.decimal,
    pa.float64().id: 'DOUBLE',
    pa.float32().id: 'FLOAT',
    pa.int32().id: 'INT',
    # 'INTERVAL_DAY_TIME': 11,
    # 'INTERVAL_YEAR_MONTH': 12,
    pa.int16().id: 'SMALLINT',
    pa.string().id: 'STR',
    # 'TIME': 7,
    # 'TIMESTAMP': 8}
}

_np_typemap = {
    np.dtype("int64"): 'BIGINT',
    np.dtype("bool"): 'BOOL',
    # pa.date64: 'DATE',
    # 'DECIMAL': pa.decimal,
    np.dtype("float64"): 'DOUBLE',
    np.dtype('float32'): 'FLOAT',
    np.dtype("int32"): 'INT',
    # 'INTERVAL_DAY_TIME': 11,
    # 'INTERVAL_YEAR_MONTH': 12,
    np.dtype("int16"): 'SMALLINT',
    np.dtype('str'): 'STR',
    # 'TIME': 7,
    # 'TIMESTAMP': 8}
}


def _build_input_rows(data):
    input_data = []
    for row in data:
        input_row = TStringRow()
        input_row.cols = [TStringValue(str(x)) for x in row]
        input_data.append(input_row)
    return input_data


def _build_table_columnar(df):
    input_cols = []

    for name in df.columns:
        col = df[name]
        kw = _typeattr[_np_typemap[col.dtype]] + "_col"
        input_cols.append(
            TColumn(data=TColumnData(**{kw: col.values.tolist()}),
                    nulls=col.isnull().values.tolist())
        )
    return input_cols
