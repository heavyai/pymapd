import time
from timeit import default_timer as timer
from functools import wraps

import structlog

import pymapd


log = structlog.get_logger()

selects = {
    'gpu': lambda con, op: con.select_ipc_gpu(op),
    # 'cpu': lambda con, op: con.select_ipc(op),
    'thrift': lambda con, op: list(con.execute(op)),
}

_benchmarks = []

def benchmark(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        kind = args[0]
        t0 = timer()
        result = func(*args, **kwargs)
        t1 = timer()
        log.info("Finished", name=func.__name__, kind=kind, time=t1 - t0)
        return result
    _benchmarks.append(wrapper)
    return wrapper


@benchmark
def select_text_small(kind, con):
    q = "select uniquecarrier from flights_2008_10k limit 10;"
    result = selects[kind](con, q)


@benchmark
def select_text_large(kind, con):
    q = "select uniquecarrier from flights_2008_10k;"
    result = selects[kind](con, q)


def main():
    con = pymapd.connect(user='mapd', password='HyperInteractive',
                         dbname='mapd', host='localhost')

    for kind in selects.keys():
        for bench in _benchmarks:
            bench(kind, con)


if __name__ == '__main__':
    main()
