try:
    # version ...
    from mapd.ttypes import TGpuDataFrame as TDataFrame
except ImportError:
    from mapd.ttypes import TDataFrame  # noqa

from mapd.ttypes import TMapDException  # noqa
