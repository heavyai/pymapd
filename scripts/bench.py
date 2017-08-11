"""
Cases to cover:

#### Select

1. Thrift
2. GPU
3. CPU

#### Data Types

1. Numeric
2. Text
3. Mix

#### Table Shapes

1. Long, narrow
2. Long, wide
3. Short, narrow
4. Short, wide
"""
from timeit import default_timer as timer
from functools import wraps

import logging

import pymapd
from numba import cuda

try:
    cuda.select_device(0)
    has_gpu = True
except cuda.cudadrv.error.CudaDriverError:
    has_gpu = False

ch = logging.StreamHandler()
fh = logging.FileHandler('timing.csv')
formatter = logging.Formatter(
    '%(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(ch)
logger.addHandler(fh)
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Benchmark Setup
# -----------------------------------------------------------------------------

_benchmarks = []
selects = {
    'cpu': lambda con, op: con.select_ipc(op),
    'thrift': lambda con, op: list(con.execute(op)),
}
if has_gpu:
    selects['gpu'] = lambda con, op: con.select_ipc_gpu(op)
# -----------------------------------------------------------------------------


def benchmark(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        kind = args[0]
        t0 = timer()
        try:
            result = func(*args, **kwargs)
        except Exception:
            logger.warning("finished,%s,%s,%s", func.__name__, kind,
                           float('nan'))
        else:
            t1 = timer()
            logger.info("finished,%s,%s,%s", func.__name__, kind, t1 - t0)
            return result

    _benchmarks.append(wrapper)
    return wrapper


@benchmark
def select_text_small(kind, con):
    q = "select uniquecarrier from flights_2008_10k limit 10;"
    selects[kind](con, q)


@benchmark
def select_text_large(kind, con):
    q = "select uniquecarrier from flights_2008_10k;"
    selects[kind](con, q)


@benchmark
def select_numeric_small(kind, con):
    q = "select depdelay, arrdelay from flights_2008_10k limit 10;"
    selects[kind](con, q)


@benchmark
def select_numeric_large(kind, con):
    q = "select depdelay, arrdelay from flights_2008_10k;"
    selects[kind](con, q)


@benchmark
def select_wide_small(kind, con):
    q = '''
    select flight_year, flight_month, flight_dayofmonth, flight_dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay, carrier_name, plane_type, plane_manufacturer, plane_model, plane_status, plane_aircraft_type, plane_engine_type, plane_year, origin_name, origin_city, origin_state, origin_country, origin_lat, origin_lon, dest_name, dest_city, dest_state, dest_country, dest_lat, dest_lon, origin_merc_x, origin_merc_y, dest_merc_x, dest_merc_y
    from flights_2008_10k
    limit 10;'''
    selects[kind](con, q)


@benchmark
def select_wide_large(kind, con):
    q = '''
    select flight_year, flight_month, flight_dayofmonth, flight_dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay, carrier_name, plane_type, plane_manufacturer, plane_model, plane_status, plane_aircraft_type, plane_engine_type, plane_year, origin_name, origin_city, origin_state, origin_country, origin_lat, origin_lon, dest_name, dest_city, dest_state, dest_country, dest_lat, dest_lon, origin_merc_x, origin_merc_y, dest_merc_x, dest_merc_y
    from flights_2008_10k;'''
    selects[kind](con, q)


def main():
    n = 5
    con = pymapd.connect(
        user='mapd',
        password='HyperInteractive',
        dbname='mapd',
        host='localhost')

    for kind in selects.keys():
        for bench in _benchmarks:
            for i in range(n):
                bench(kind, con)


if __name__ == '__main__':
    main()
