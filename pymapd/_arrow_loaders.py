import datetime
import pandas as pd
import pyarrow as pa

from mapd.ttypes import TColumnData, TColumn

from ._parsers import _time_to_seconds
from . import _arrow_utils as au


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
