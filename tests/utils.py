try:
    from unittest import mock
except ImportError:
    import mock  # noqa

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
