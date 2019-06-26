import subprocess
import time
import pytest
from thrift.transport import TSocket, TTransport
from thrift.transport.TSocket import TTransportException
from pymapd import connect
import datetime
import random
import string
import numpy as np
import pandas as pd


def _check_open():
    """
    Test to see if OmniSci running on localhost and socket open
    """
    socket = TSocket.TSocket("localhost", 6274)
    transport = TTransport.TBufferedTransport(socket)

    try:
        transport.open()
        return True
    except TTransportException:
        return False


@pytest.fixture(scope='session')
def mapd_server():
    """Ensure a mapd server is running, optionally starting one if none"""
    if _check_open():
        # already running before pytest started
        pass
    else:
        # not yet running...
        subprocess.check_output(['docker', 'run', '-d',
                                 '--ipc=host',
                                 '-v', '/dev:/dev',
                                 '-p', '6274:6274',
                                 '-p', '9092:9092',
                                 '--name=mapd', 'mapd/core-os-cpu:latest'])
        # yield and stop afterwards?
        assert _check_open()
        # Takes some time to start up. Unfortunately even trying to connect
        # will cause it to hang.
        time.sleep(5)


@pytest.fixture(scope='session')
def con(mapd_server):
    """
    Fixture to provide Connection for tests run against live OmniSci instance
    """
    return connect(user="admin", password='HyperInteractive', host='localhost',
                   port=6274, protocol='binary', dbname='omnisci')


@pytest.fixture
def mock_client(mocker):
    """A magicmock for pymapd.connection.Client"""
    return mocker.patch("pymapd.connection.Client")


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


def gen_string():
    """Generate a random string sequence for use in _tests_table_no_nulls"""
    return ''.join([random.choice(string.ascii_letters + string.digits)
                   for n in range(10)])


def _tests_table_no_nulls(n_samples):
    """
    Generates a dataframe with all OmniSci types in it for use in integration
    testing
    """

    np.random.seed(12345)

    tinyint_ = np.random.randint(low=-127,
                                 high=127,
                                 size=n_samples,
                                 dtype='int8')

    smallint_ = np.random.randint(low=-32767,
                                  high=32767,
                                  size=n_samples,
                                  dtype='int16')

    int_ = np.random.randint(low=-2147483647,
                             high=2147483647,
                             size=n_samples,
                             dtype='int32')

    bigint_ = np.random.randint(low=-9223372036854775807,
                                high=9223372036854775807,
                                size=n_samples,
                                dtype='int64')

    # float and double ranges slightly lower than we support, full width
    # causes an error in np.linspace that's not worth tracking down
    float_ = np.linspace(-3.4e37, 3.4e37, n_samples, dtype='float32')
    double_ = np.linspace(-1.79e307, 1.79e307, n_samples, dtype='float64')

    bool_ = np.random.randint(low=0, high=2, size=n_samples, dtype='bool')

    # effective date range of 1904 to 2035
    # TODO: validate if this is an Arrow limitation, outside this range fails
    date_ = [datetime.date(1970, 1, 1) + datetime.timedelta(days=int(x))
             for x in np.random.randint(-24000, 24000, size=n_samples)]

    datetime_ = [datetime.datetime(1970, 1, 1) +
                 datetime.timedelta(days=int(x), minutes=int(x))
                 for x in np.random.randint(-24000, 24000, size=n_samples)]

    time_h = np.random.randint(0, 24, size=n_samples)
    time_m = np.random.randint(0, 60, size=n_samples)
    time_s = np.random.randint(0, 60, size=n_samples)
    time_ = [datetime.time(h, m, s) for h, m, s in zip(time_h, time_m, time_s)]

    # generate random text strings
    text_ = [gen_string() for x in range(n_samples)]

    # read geo data from files
    point_ = pd.read_csv("tests/data/points_10000.zip", header=None).values
    point_ = np.squeeze(point_)

    line_ = pd.read_csv("tests/data/lines_10000.zip", header=None).values
    line_ = np.squeeze(line_)

    mpoly_ = pd.read_csv("tests/data/mpoly_10000.zip", header=None).values
    mpoly_ = np.squeeze(mpoly_)

    poly_ = pd.read_csv("tests/data/polys_10000.zip", header=None).values
    poly_ = np.squeeze(poly_)

    d = {'tinyint_': tinyint_,
         'smallint_': smallint_,
         'int_': int_,
         'bigint_': bigint_,
         'float_': float_,
         'double_': double_,
         'bool_': bool_,
         'date_': date_,
         'datetime_': datetime_,
         'time_': time_,
         'text_': text_,
         'point_': point_,
         'line_': line_,
         'mpoly_': mpoly_,
         'poly_': poly_
         }

    return pd.DataFrame(d)
