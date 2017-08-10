"""
Utility methods for parsing data returned from MapD
"""
from collections import namedtuple
import mapd.ttypes as T


Description = namedtuple("Description", ["name", "type_code", "display_size",
                                         "internal_size", "precision", "scale",
                                         "null_ok"])

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
}


def _extract_row_val(desc, val):
    # type: (T.TColumnType, T.TDatum) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.val, _typeattr[typename] + '_val')


def _extract_col_vals(desc, val):
    # type: (T.TColumnType, T.TColumn) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.data, _typeattr[typename] + '_col')


def _extract_description(row_desc):
    # type: (List[T.TColumnType]) -> List[Description]
    """
    Return a tuple of (name, type_code, display_size, internal_size,
                       precision, scale, null_ok)

    https://www.python.org/dev/peps/pep-0249/#description
    """
    return [Description(col.col_name, col.col_type.type,
                        None, None, None, None,
                        col.col_type.nullable)
            for col in row_desc]


def _is_columnar(data):
    # type: (T.TQueryResult) -> bool
    return data.row_set.is_columnar


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
    import pyarrow as pa

    reader = pa.RecordBatchStreamReader(buf)
    return reader.schema


def _load_data(buf, schema):
    """
    Load a `pandas.DataFrame` from a buffer written to shared memory

    Parameters
    ----------
    buf : pyarrow.Buffer
    shcema : pyarrow.Schema

    Returns
    -------
    df : pandas.DataFrame
    """
    import pyarrow as pa

    message = pa.read_message(buf)
    rb = pa.read_record_batch(message, schema)
    return rb.to_pandas()


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
    import numpy as np
    from pygdf.gpuarrow import GpuArrowReader
    from pygdf.dataframe import DataFrame
    from numba import cuda
    from numba.cuda.cudadrv import drvapi

    from .shm import load_buffer

    ipc_handle = drvapi.cu_ipc_mem_handle(*tdf.df_handle)
    ipch = cuda.driver.IpcHandle(None, ipc_handle, size=tdf.df_size)
    ctx = cuda.current_context()
    dptr = ipch.open(ctx)

    schema_buffer = load_buffer(tdf.sm_handle, tdf.sm_size)
    # TODO: extra copy.
    schema_buffer = np.frombuffer(schema_buffer.to_pybytes(), dtype=np.uint8)

    dtype = np.dtype(np.byte)
    darr = cuda.devicearray.DeviceNDArray(shape=dptr.size,
                                          strides=dtype.itemsize,
                                          dtype=dtype,
                                          gpu_data=dptr)
    reader = GpuArrowReader(schema_buffer, darr)
    df = DataFrame()
    for k, v in reader.to_dict().items():
        df[k] = v

    return df
