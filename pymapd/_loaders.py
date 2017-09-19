"""
Internal helpers for loading data
"""
import datetime

from mapd.ttypes import TStringRow, TStringValue, TColumnData, TColumn
import pyarrow as pa
import numpy as np
import pandas as pd

from ._parsers import _typeattr, _time_to_seconds
from . import _arrow_utils as au


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


def _build_table_columnar(df, preserve_index=True):
    input_cols = []

    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=preserve_index)

    # a reference to a list of False for the common case of no missing values
    all_nulls = None

    for col in df.itercolumns():
        data = col.to_pylist()  # TODO: avoid this allocation!
        if col.null_count:
            nulls = pd.isnull(data)
        else:
            # Avoid allocating a list if possible
            if all_nulls is None:
                nulls = all_nulls = [False] * len(col)
            else:
                nulls = all_nulls

        # TODO: consider {id -> {'real', 'int', 'str'}}
        t = col.type

        if au.is_int_type(t):
            if col.null_count:
                data = col.to_pandas().fillna(-1).astype(t.to_pandas_dtype())
            col = TColumn(data=TColumnData(int_col=data), nulls=nulls)

        elif au.is_float_type(t):
            if col.null_count:
                data = col.to_pandas().fillna(-1)
            col = TColumn(data=TColumnData(real_col=data), nulls=nulls)

        elif au.is_string_type(t):
            if col.null_count:
                data = col.to_pandas().fillna('')
            col = TColumn(data=TColumnData(str_col=data), nulls=nulls)

        elif au.is_datetime_type(t):
            # TODO: unused to_pylist above for dates
            data = col.to_pandas().view('i8') // 10**9  # ns -> s since epoch
            col = TColumn(data=TColumnData(int_col=data), nulls=nulls)

        elif au.is_bool_type(t):
            if col.null_count:
                data = col.to_pandas().fillna(-1).astype(t.to_pandas_dtype())
            col = TColumn(data=TColumnData(int_col=data), nulls=nulls)

        elif au.is_decimal_type(t):
            if col.null_count:
                data = col.to_pandas().fillna(-1).astype(t.to_pandas_dtype())
            col = TColumn(data=TColumnData(real_col=data), nulls=nulls)

        elif au.is_time_type(t):
            data = [_time_to_seconds(time) if time is not None else -1
                    for time in data]
            col = TColumn(data=TColumnData(int_col=data), nulls=nulls)

        elif au.is_date_type(t):
            # TODO: verify this conversion
            data = ((col.to_pandas() -
                     datetime.datetime(1970, 1, 1))
                    .dt.total_seconds().fillna(-1)  # fillna should be OK...
                    .astype(int))
            col = TColumn(data=TColumnData(int_col=data), nulls=nulls)
        else:
            raise TypeError("Unhandled type {}".format(t))

        input_cols.append(col)

    return input_cols
