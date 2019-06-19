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
import argparse
import datetime
import logging
import re
import sys
from timeit import default_timer as timer
from functools import wraps
from itertools import product

import coloredlogs
from numba import cuda

import pymapd

try:
    cuda.select_device(0)
    has_gpu = True
except cuda.cudadrv.error.CudaDriverError:
    has_gpu = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
coloredlogs.install(level='DEBUG')


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Run some benchmarks')
    parser.add_argument("-c", '--count', type=int, default=3,
                        help="Number of trials per benchmark")
    parser.add_argument("-b", "--benchmarks", default=None,
                        help='Regex to match benchmark names')
    parser.add_argument("-o", '--output',
                        default="timing-{:%Y-%m-%d-%H-%M-%S}.csv".format(
                            datetime.datetime.now()),
                        help="Output CSV")
    return parser.parse_args(args)


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


def benchmark(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        warmup = kwargs.pop('warmup', False)

        if warmup:
            func(*args, **kwargs)

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


numeric_cols = ('flight_year, flight_month, flight_dayofmonth, '
                'flight_dayofweek, deptime, crsdeptime, arrtime, crsarrtime, '
                'flightnum, actualelapsedtime, crselapsedtime, airtime, '
                'arrdelay, depdelay, distance, taxiin, taxiout, cancelled, '
                'diverted, carrierdelay, weatherdelay, nasdelay, '
                'securitydelay, lateaircraftdelay, plane_year, origin_lat, '
                'origin_lon, dest_lat, dest_lon, origin_merc_x, origin_merc_y, '
                'dest_merc_x, dest_merc_y')
text_cols = (
    'uniquecarrier, tailnum, origin, dest, cancellationcode, carrier_name, '
    'plane_type, plane_manufacturer, plane_model, plane_status, '
    'plane_aircraft_type, plane_engine_type, origin_name, origin_city, '
    'origin_state, origin_country, dest_name, dest_city, dest_state, '
    'dest_country'
)
mixed_cols = numeric_cols + ', ' + text_cols

# -----------------------------------------------------------------------------


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
def select_wide_numeric(kind, con):
    q = 'select {} from flights_2008_10k'.format(numeric_cols)
    selects[kind](con, q)


@benchmark
def select_wide_text(kind, con):
    q = 'select {} from flights_2008_10k'.format(text_cols)
    selects[kind](con, q)


@benchmark
def select_wide_mixed(kind, con):
    q = 'select {} from flights_2008_10k'.format(mixed_cols)
    selects[kind](con, q)


@benchmark
def select_reduction(kind, con):
    q = '''select avg(flight_year), count(*) from flights_2008_10k;'''
    return selects[kind](con, q)


@benchmark
def select_groupby(kind, con):
    q = '''\
    SELECT uniquecarrier, avg(depdelay) as delay
    from flights_2008_10k
    group by uniquecarrier
    '''
    return selects[kind](con, q)


@benchmark
def select_distinct_single(kind, con):
    q = 'select distinct uniquecarrier from flights_2008_10k'
    return selects[kind](con, q)


@benchmark
def select_distinct_multiple(kind, con):
    q = ('select distinct uniquecarrier, flight_year, flight_month, '
         'flight_dayofmonth, flight_dayofweek from flights_2008_10k')
    return selects[kind](con, q)


@benchmark
def select_filter(kind, con):
    q = ('select uniquecarrier, depdelay, arrdelay from flights_2008_10k '
         'where depdelay > 10')
    return selects[kind](con, q)


@benchmark
def select_complex(kind, con):
    q = '''
    SELECT origin, dest, carrier_name, count(*), avg(depdelay), avg(arrdelay)
    FROM flights_2008_10k
    WHERE arrdelay between -30 and 100
    GROUP BY origin, dest, carrier_name
    ORDER BY origin, dest, carrier_name'''
    return selects[kind](con, q)


skips = {
    (),
}


def main(args=None):
    args = parse_args(args)

    fh = logging.FileHandler(args.output)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    con = pymapd.connect(
        user='admin',
        password='HyperInteractive',
        dbname='omnisci',
        host='localhost')

    grid = product(selects.keys(), _benchmarks)
    if args.benchmarks:
        xpr = re.compile(args.benchmarks)
    else:
        xpr = None

    for kind, bench in grid:
        if xpr:
            if not xpr.search(bench.__name__):
                continue
        logger.debug("Starting %s[%s]", bench.__name__, kind)
        bench(kind, con, warmup=True)

        for i in range(args.count):
            logger.debug("%s[%s] %d/%d", bench.__name__, kind, i + 1,
                         args.count)
            bench(kind, con)


if __name__ == '__main__':
    sys.exit(main(None))
