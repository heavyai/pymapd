def no_gpu():
    """Detect if don't have numba and a GPU available"""
    try:
        from numba import cuda

        try:
            cuda.select_device(0)
        except cuda.cudadrv.error.CudaDriverError:
            return True
    except ImportError:
        return True
    return False


def assert_columnar_equal(result, expected):
    import numpy as np

    for i, (a, b) in enumerate(zip(result, expected)):
        np.testing.assert_array_equal(a.nulls, b.nulls)
        np.testing.assert_array_equal(a.data.int_col, b.data.int_col)
        np.testing.assert_array_equal(a.data.real_col, b.data.real_col)
        np.testing.assert_array_equal(a.data.str_col, b.data.str_col)
