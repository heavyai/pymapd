"""
Utility methods for parsing data returned from MapD
"""


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
