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


@pytest.fixture(scope="session")
def stocks(con):
    drop = 'drop table if exists stocks;'
    c = con.cursor()
    c.execute(drop)
    create = ('create table stocks (date_ text, trans text, symbol text, '
              'qty int, price float, vol float);')
    c.execute(create)
    i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"
    i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"

    c.execute(i1)
    c.execute(i2)


@pytest.fixture
def mock_transport(mocker):
    """A magicmock for pymapd.connection.TTrapnsport.TBufferedTransport"""
    return mocker.patch("pymapd.connection.TTransport.TBufferedTransport")


@pytest.fixture
def mock_client(mocker):
    """A magicmock for pymapd.connection.Client"""
    return mocker.patch("pymapd.connection.Client")


@pytest.fixture
def mock_connection(mock_transport, mock_client):
    """Connection with mocked transport layer, and

    - username='user'
    - password='password'
    - host='localhost'
    - dbname='dbname'
    """
    return connect(user='user', password='password',
                   host='localhost', dbname='dbname')
