"""
Utility methods for parsing data returned from MapD
"""
import datetime
import pyarrow as pa
from collections import namedtuple
from sqlalchemy import text
import omnisci.common.ttypes as T
import ctypes
from types import MethodType
from ._mutators import set_tdf, get_tdf
from ._utils import seconds_to_time, datetime_in_precisions
import numpy as np
from .ipc import load_buffer, shmdt


Description = namedtuple("Description", ["name", "type_code", "display_size",
                                         "internal_size", "precision", "scale",
                                         "null_ok"])
ColumnDetails = namedtuple("ColumnDetails", ["name", "type", "nullable",
                                             "precision", "scale",
                                             "comp_param", "encoding",
                                             "is_array"])

_typeattr = {
    'SMALLINT': 'int',
    'INT': 'int',
    'BIGINT': 'int',
    'TIME': 'int',
    'TIMESTAMP': 'int',
    'DATE': 'int',
    'BOOL': 'int',
    'FLOAT': 'real',
    'DECIMAL': 'real',
    'DOUBLE': 'real',
    'STR': 'str',
    'POINT': 'str',
    'LINESTRING': 'str',
    'POLYGON': 'str',
    'MULTIPOLYGON': 'str',
    'TINYINT': 'int',
    'GEOMETRY': 'str',
    'GEOGRAPHY': 'str',
}
_thrift_types_to_values = T.TDatumType._NAMES_TO_VALUES
_thrift_values_to_types = T.TDatumType._VALUES_TO_NAMES
_thrift_encodings_to_values = T.TEncodingType._NAMES_TO_VALUES
_thrift_values_to_encodings = T.TEncodingType._VALUES_TO_NAMES


def _format_result_timestamp(desc, arr):

    return [None if v is None else
            datetime_in_precisions(v, desc.col_type.precision)
            for v in arr]


def _format_result_date(arr):

    base = datetime.datetime(1970, 1, 1)
    return [None if v is None else
            (base + datetime.timedelta(seconds=v)).date()
            for v in arr]


def _format_result_time(arr):

    return [None if v is None else seconds_to_time(v) for v in arr]


def _extract_col_vals(desc, val):

    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    nulls = val.nulls

    # arr_col has multiple levels to parse, not accounted for in original code
    # https://github.com/omnisci/pymapd/issues/68
    if hasattr(val.data, 'arr_col') and val.data.arr_col:
        vals = [None if null else getattr(v.data, _typeattr[typename] + '_col')
                for null, v in zip(nulls, val.data.arr_col)]

        if typename == 'TIMESTAMP':
            vals = [_format_result_timestamp(desc, v) for v in vals]
        elif typename == 'DATE':
            vals = [_format_result_date(v) for v in vals]
        elif typename == 'TIME':
            vals = [_format_result_time(v) for v in vals]

    # else clause original code path
    else:
        vals = getattr(val.data, _typeattr[typename] + '_col')
        vals = [None if null else v for null, v in zip(nulls, vals)]

        if typename == 'TIMESTAMP':
            vals = _format_result_timestamp(desc, vals)
        elif typename == 'DATE':
            vals = _format_result_date(vals)
        elif typename == 'TIME':
            vals = _format_result_time(vals)

    return vals


def _extract_description(row_desc):
    """
    Return a tuple of (name, type_code, display_size, internal_size,
                       precision, scale, null_ok)

    https://www.python.org/dev/peps/pep-0249/#description
    """
    return [Description(col.col_name, col.col_type.type,
                        None, None, None, None,
                        col.col_type.nullable)
            for col in row_desc]


def _extract_column_details(row_desc):
    # For Connection.get_table_details
    return [
        ColumnDetails(x.col_name, _thrift_values_to_types[x.col_type.type],
                      x.col_type.nullable, x.col_type.precision,
                      x.col_type.scale, x.col_type.comp_param,
                      _thrift_values_to_encodings[x.col_type.encoding],
                      x.col_type.is_array)
        for x in row_desc
    ]


def _load_schema(buf):
    """
    Load a `pyarrow.Schema` from a buffer written to shared memory

    Parameters
    ----------
    buf : pyarrow.Buffer

    Returns
    -------
    schema : pyarrow.Schema
    """
    reader = pa.RecordBatchStreamReader(buf)
    return reader.schema


def _load_data(buf, schema, tdf=None):
    """
    Load a `pandas.DataFrame` from a buffer written to shared memory

    Parameters
    ----------
    buf : pyarrow.Buffer
    shcema : pyarrow.Schema
    tdf(optional) : TDataFrame

    Returns
    -------
    df : pandas.DataFrame
    """
    message = pa.read_message(buf)
    rb = pa.read_record_batch(message, schema)
    df = rb.to_pandas()
    df.set_tdf = MethodType(set_tdf, df)
    df.get_tdf = MethodType(get_tdf, df)
    df.set_tdf(tdf)
    return df


def _parse_tdf_gpu(tdf):
    """
    Parse the results of a select ipc_gpu into a GpuDataFrame

    Parameters
    ----------
    tdf : TDataFrame

    Returns
    -------
    gdf : GpuDataFrame
    """

    from cudf.comm.gpuarrow import GpuArrowReader
    from cudf.dataframe import DataFrame
    from numba import cuda
    from numba.cuda.cudadrv import drvapi

    ipc_handle = drvapi.cu_ipc_mem_handle(*tdf.df_handle)
    ipch = cuda.driver.IpcHandle(None, ipc_handle, size=tdf.df_size)
    ctx = cuda.current_context()
    dptr = ipch.open(ctx)

    schema_buffer = load_buffer(tdf.sm_handle, tdf.sm_size)

    # save ptr value before overwritten below copy with np.frombuffer()
    ptr = schema_buffer[1]

    # TODO: extra copy.
    schema_buffer = np.frombuffer(schema_buffer[0].to_pybytes(),
                                  dtype=np.uint8)

    dtype = np.dtype(np.byte)
    darr = cuda.devicearray.DeviceNDArray(shape=dptr.size,
                                          strides=dtype.itemsize,
                                          dtype=dtype,
                                          gpu_data=dptr)
    reader = GpuArrowReader(schema_buffer, darr)
    df = DataFrame()
    df.set_tdf = MethodType(set_tdf, df)
    df.get_tdf = MethodType(get_tdf, df)

    for k, v in reader.to_dict().items():
        df[k] = v

    df.set_tdf(tdf)

    # free shared memory from Python
    # https://github.com/omnisci/pymapd/issues/46
    # https://github.com/omnisci/pymapd/issues/31
    free_sm = shmdt(ctypes.cast(ptr, ctypes.c_void_p))  # noqa

    return df


def _bind_parameters(operation, parameters):
    return (text(operation)
            .bindparams(**parameters)
            .compile(compile_kwargs={"literal_binds": True}))
