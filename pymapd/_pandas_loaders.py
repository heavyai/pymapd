import datetime
import math

import numpy as np
import pandas as pd
import pyarrow as pa
import shapely

from pandas.api.types import (
    is_bool_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_object_dtype,
    is_datetime64_any_dtype,
)

from omnisci.thrift.ttypes import TColumn, TColumnData, TColumnType

from omnisci.common.ttypes import TTypeInfo, TDatumType

from ._utils import (
    date_to_seconds,
    time_to_seconds,
    datetime_to_seconds,
    mapd_to_na,
    mapd_to_slot,
)

try:
    import geopandas as gpd
except ImportError:
    gpd = None


GEO_TYPE_NAMES = ['POINT', 'LINESTRING', 'POLYGON', 'MULTIPOLYGON']
GEO_TYPE_ID = [
    v[1] for v in TDatumType._NAMES_TO_VALUES.items() if v[0] in GEO_TYPE_NAMES
]


def get_mapd_dtype(data):
    """Get the OmniSci type"""
    if is_object_dtype(data):
        return get_mapd_type_from_object(data)
    else:
        return get_mapd_type_from_known(data.dtype)


def get_mapd_type_from_known(dtype):
    """For cases where pandas type system matches"""
    if is_bool_dtype(dtype):
        return 'BOOL'
    elif is_integer_dtype(dtype):
        if dtype.itemsize <= 1:
            return 'TINYINT'
        elif dtype.itemsize == 2:
            return 'SMALLINT'
        elif dtype.itemsize == 4:
            return 'INT'
        else:
            return 'BIGINT'
    elif is_float_dtype(dtype):
        if dtype.itemsize <= 4:
            return 'FLOAT'
        else:
            return 'DOUBLE'
    elif is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif isinstance(dtype, pd.CategoricalDtype):
        return 'STR'
    else:
        raise TypeError("Unhandled type {}".format(dtype))


def get_mapd_type_from_object(data):
    """For cases where the type system mismatches"""
    try:
        val = data.dropna().iloc[0]
    except IndexError:
        raise IndexError("Not any valid values to infer the type")

    if isinstance(val, str):
        return 'STR'
    elif isinstance(val, np.datetime64):
        return 'TIMESTAMP'
    elif isinstance(val, datetime.date):
        return 'DATE'
    elif isinstance(val, datetime.time):
        return 'TIME'
    elif isinstance(val, bool):
        return 'BOOL'
    elif isinstance(val, int):
        if data.max() >= 2147483648 or data.min() <= -2147483648:
            return 'BIGINT'
        return 'INT'
    elif isinstance(val, list):
        return 'ARRAY/{}'.format(get_mapd_dtype(pd.Series(val)))
    elif isinstance(val, shapely.geometry.Point):
        return 'POINT'
    elif isinstance(val, shapely.geometry.LineString):
        return 'LINESTRING'
    elif isinstance(val, shapely.geometry.Polygon):
        return 'POLYGON'
    elif isinstance(val, shapely.geometry.MultiPolygon):
        return 'MULTIPOLYGON'
    else:
        raise TypeError("Unhandled type {}".format(data.dtype))


def thrift_cast(data, mapd_type, scale=0, precision=0):
    """Cast data type to the expected thrift types"""

    if mapd_type == 'TIMESTAMP':
        return datetime_to_seconds(data, precision)
    elif mapd_type == 'TIME':
        return pd.Series(time_to_seconds(x) for x in data)
    elif mapd_type == 'DATE':
        data = date_to_seconds(data)
        data = data.fillna(mapd_to_na[mapd_type])
        return data.astype(int)
    elif mapd_type == 'BOOL':
        # fillna before converting to int, since int cols
        # in Pandas do not support None or NaN
        data = data.fillna(mapd_to_na[mapd_type])
        return data.astype(int)
    elif mapd_type == 'DECIMAL':
        # Multiply by 10^scale
        data = data * 10 ** scale
        # fillna and convert to int
        data = data.fillna(mapd_to_na[mapd_type])
        return data.astype(int)


def build_input_columnar(
    df, preserve_index=True, chunk_size_bytes=0, col_types=[], col_names=[]
):
    if preserve_index:
        df = df.reset_index()

    dfsize = df.memory_usage().sum()
    if chunk_size_bytes > 0:
        chunks = math.ceil(dfsize / chunk_size_bytes)
    else:
        chunks = 1

    dfs = np.array_split(df, chunks)
    cols_array = []

    for df in dfs:
        input_cols = []

        colindex = 0
        for col in col_names:
            data = df.loc[:, [col]]

            mapd_type = col_types[colindex].type
            is_array = col_types[colindex].is_array
            scale = col_types[colindex].scale
            precision = col_types[colindex].precision
            has_nulls = data[col].hasnans

            if has_nulls:
                nulls = data[col].isnull().values.tolist()
            else:
                nulls = [False] * len(df)

            if is_array:
                # Expand the dataframe so each array item has
                # its own field in the dataframe.
                data = data.iloc[:, 0].apply(pd.Series)

            if mapd_type in {'TIME', 'TIMESTAMP', 'DATE', 'BOOL'}:
                # requires a cast to integer
                for c in data:
                    data.loc[:, c] = thrift_cast(
                        data=data[c], mapd_type=mapd_type, precision=precision
                    )

            if mapd_type in ['DECIMAL']:
                # requires a calculation be done using the scale
                # then cast to int
                for c in data:
                    data.loc[:, c] = thrift_cast(
                        data=data[c], mapd_type=mapd_type, scale=scale,
                    )

            if has_nulls:
                if not is_array:
                    for c in data:
                        data.loc[:, c] = data[c].fillna(mapd_to_na[mapd_type])

            if is_array:
                data = data.apply(lambda x: [i for i in x.dropna()], axis=1)
                if has_nulls:
                    data[nulls] = mapd_to_na[mapd_type]

            if mapd_type in GEO_TYPE_NAMES:
                for c in data:
                    data.loc[:, c] = data.loc[:, c].apply(lambda g: g.wkt)
            elif mapd_type not in ['FLOAT', 'DOUBLE', 'VARCHAR', 'STR']:
                if is_array:
                    data = data.apply(
                        lambda _array: [int(item) for item in _array]
                        if isinstance(_array, list)
                        else None
                    )
                else:
                    for c in data:
                        data.loc[:, c] = data.loc[:, c].astype('int64')

            # If this is an array column, we need the data to be a series
            # of TColumn objects of type mapd_type.
            if is_array:
                data = data.apply(
                    lambda x: TColumn(
                        data=TColumnData(**{mapd_to_slot[mapd_type]: x})
                    )
                )
                kwargs = {'arr_col': data}
            else:
                kwargs = {mapd_to_slot[mapd_type]: data.iloc[:, 0].values}

            input_cols.append(TColumn(data=TColumnData(**kwargs), nulls=nulls))
            colindex += 1
        cols_array.append(input_cols)

    return cols_array


def _serialize_arrow_payload(data, table_metadata, preserve_index=True):

    if isinstance(data, pd.DataFrame):

        # detect if there are categorical columns in dataframe
        cols = data.select_dtypes(include=['category']).columns

        # if there are categorical columns, make a copy before casting
        # to avoid mutating input data
        # https://github.com/omnisci/pymapd/issues/169
        if cols.size > 0:
            data_ = data.copy()
            data_[cols] = data_[cols].astype('object')
        else:
            data_ = data

        data = pa.RecordBatch.from_pandas(data_, preserve_index=preserve_index)

    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, data.schema)

    if isinstance(data, pa.RecordBatch):
        writer.write_batch(data)
    elif isinstance(data, pa.Table):
        writer.write_table(data)

    writer.close()
    return stream.getvalue()


def build_row_desc(data, preserve_index=False):

    if not (
        isinstance(data, pd.DataFrame)
        or (gpd is not None and isinstance(data, gpd.GeoDataFrame))
    ):
        # Once https://issues.apache.org/jira/browse/ARROW-1576 is complete
        # we can support pa.Table here too
        raise TypeError(
            "Create table is not supported for type {}. "
            "Use a pandas DataFrame or a GeoPandas DataFrame, "
            "or perform the create separately".format(type(data))
        )

    if preserve_index:
        data = data.reset_index()

    dtypes = []
    is_array = {}
    for col in data.columns:
        _dtype = get_mapd_dtype(data[col])
        is_array[col] = True if _dtype.startswith('ARRAY') else None
        dtypes.append((col, _dtype.replace('ARRAY/', '')))
    # row_desc :: List<TColumnType>
    row_desc = [
        TColumnType(
            name,
            TTypeInfo(getattr(TDatumType, mapd_type), is_array=is_array[name]),
        )
        for name, mapd_type in dtypes
    ]

    # force text encoding dict for all string columns
    # default is TEXT ENCODING DICT(32) when only tct.col_type.encoding = 4 set
    # https://github.com/omnisci/pymapd/issues/140#issuecomment-477353420
    for tct in row_desc:
        if tct.col_type.type == 6:
            tct.col_type.encoding = 4
        elif tct.col_type.type in GEO_TYPE_ID:
            tct.col_type.precision = 23
        elif tct.col_type.type == 8:
            # force precision for timestamp with nanoseconds
            if data[tct.col_name].dt.nanosecond.sum():
                tct.col_type.precision = 9
    return row_desc
