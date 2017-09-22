import six
import datetime

from pandas.api.types import (
    is_bool_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_object_dtype,
    is_datetime64_any_dtype,
)

from mapd.ttypes import TColumn, TColumnData
from ._utils import (
    date_to_seconds, time_to_seconds, datetime_to_seconds,
    mapd_to_na, mapd_to_slot
)


def get_mapd_dtype(data):
    "Get the mapd type"
    if is_object_dtype(data):
        return get_mapd_type_from_object(data)
    else:
        return get_mapd_type_from_known(data.dtype)


def get_mapd_type_from_known(dtype):
    """For cases where pandas type system matches"""
    if is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif is_integer_dtype(dtype):
        if dtype.itemsize <= 2:
            return 'SMALLINT'
        elif dtype.itemsize == 4:
            return 'INTEGER'
        else:
            return 'BIGINT'
    elif is_float_dtype(dtype):
        if dtype.itemsize <= 4:
            return 'FLOAT'
        else:
            return 'DOUBLE'
    elif is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        raise TypeError("Unhandled type {}".format(dtype))


def get_mapd_type_from_object(data):
    """For cases where the type system mismatches"""
    try:
        val = data.dropna()[0]
    except IndexError:
        raise IndexError("Not any valid values to infer the type")
    if isinstance(val, six.string_types):
        return 'TEXT'
    elif isinstance(val, datetime.date):
        return 'DATE'
    elif isinstance(val, datetime.time):
        return 'TIME'
    elif isinstance(val, int):
        return 'INTEGER'
    else:
        raise TypeError("Unhandled type {}".format(data.dtype))


def thrift_cast(data, mapd_type):
    """Cast data type to the expected thrift types"""
    import pandas as pd

    if mapd_type == 'TIMESTAMP':
        return datetime_to_seconds(data)
    elif mapd_type == 'TIME':
        return pd.Series(time_to_seconds(x) for x in data)
    elif mapd_type == 'DATE':
        return date_to_seconds(data)


def build_input_columnar(df, preserve_index=True):
    if preserve_index:
        df = df.reset_index()

    input_cols = []
    all_nulls = None

    for col in df.columns:
        data = df[col]
        mapd_type = get_mapd_dtype(data)
        has_nulls = data.hasnans

        if has_nulls:
            nulls = data.isnull().values
        elif all_nulls is None:
            nulls = all_nulls = [False] * len(df)

        if mapd_type in {'TIME', 'TIMESTAMP', 'DATE'}:
            # requires a cast to integer
            data = thrift_cast(data, mapd_type)

        if has_nulls:
            data = data.fillna(mapd_to_na[mapd_type])

            if mapd_type not in {'FLOAT', 'DOUBLE', 'VARCHAR', 'TEXT'}:
                data = data.astype('int64')
        # use .values so that indexes don't have to be serialized too
        kwargs = {mapd_to_slot[mapd_type]: data.values}

        input_cols.append(
            TColumn(data=TColumnData(**kwargs), nulls=nulls)
        )

    return input_cols
