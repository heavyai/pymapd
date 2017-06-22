import os
import pickle
import subprocess
import time

import pytest
import importlib
from thrift.transport import TSocket, TTransport
from thrift.transport.TSocket import TTransportException

from pymapd import connect

ttypes = importlib.import_module("mapd.ttypes")
HERE = os.path.dirname(__file__)

socket = TSocket.TSocket("localhost", 9091)
transport = TTransport.TBufferedTransport(socket)


def _check_open():
    try:
        transport.open()
        return True
    except TTransportException:
        return False


@pytest.fixture(scope='session')
def mapd_server():
    """
    Ensure a mapd server is running.
    """
    if _check_open():
        # already running before pytest started
        pass
    else:
        # not yet running...
        subprocess.check_output(['docker', 'run', '-d',
                                 '-p', '9092:9092', '-p', '9091:9091',
                                 '--name=mapd', 'mapd/mapd:v3.0.0'])
        # yield and stop afterwards?
        assert _check_open()
        # Takes some time to start up. Unfortunately even trying to connect
        # will cause it to hang.
        time.sleep(5)


@pytest.fixture(scope='session')
def con(mapd_server):
    return connect(user="mapd", password='HyperInteractive', host='localhost',
                   port=9091, protocol='binary', dbname='mapd')


def _load_pickle(filepath):
    with open(filepath, 'rb') as f:
        return pickle.load(f)


@pytest.fixture
def colwise():
    return _load_pickle(os.path.join(HERE, "data", "colwise.pkl"))


@pytest.fixture
def rowwise():
    return _load_pickle(os.path.join(HERE, "data", "rowwise.pkl"))


@pytest.fixture
def invalid_sql():
    return _load_pickle(os.path.join(HERE, "data", "invalid_sql.pkl"))


@pytest.fixture
def nonexistant_table():
    return _load_pickle(os.path.join(HERE, "data", "nonexistant_table.pkl"))
