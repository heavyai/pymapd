# -*- coding: utf-8 -*-
import os
import subprocess
import textwrap
import time

import pytest
import importlib
from thrift.transport import TSocket, TTransport
from thrift.transport.TSocket import TTransportException

from pymapd import connect


def _check_open():

    socket = TSocket.TSocket("localhost", 6274)
    transport = TTransport.TBufferedTransport(socket)

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
    return connect(user="mapd", password='HyperInteractive', host='localhost',
                   port=6274, protocol='binary', dbname='mapd')


@pytest.fixture(scope="session")
def stocks(con):
    """A sample table `stocks` populated with two rows. The
    table is dropped at the start of the session.

    - date_ : text
    - trans : text
    - symbol : text
    - qty : int
    - price : float
    - vol : float
    """
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
    yield "stocks"
    c.execute(drop)


@pytest.fixture
def empty_table(con):
    """
    An empty table named `baz`. The spec is

        - a : int
        - b : float
        - c : text
    """
    name = 'baz'
    con.execute("drop table if exists {};".format(name))
    con.execute("create table {} (a int, b float, c text);".format(name))
    yield name
    con.execute("drop table if exists {};".format(name))


@pytest.fixture(scope="session")
def date_table(con):
    """A sample with date and time columns

    - date_ : DATE
    - datetime_ : TIMESTAMP
    - time_ : TIME
    """
    name = 'dates'
    drop = 'drop table if exists {};'.format(name)
    c = con.cursor()
    c.execute(drop)
    create = ('create table {} (date_ DATE, datetime_ TIMESTAMP, '
              'time_ TIME);'.format(name))
    c.execute(create)
    i1 = ("INSERT INTO {} VALUES ('2006-01-05','2006-01-01T12:00:00',"
          "'12:00');".format(name))
    i2 = ("INSERT INTO {} VALUES ('1901-12-14','1901-12-13T20:45:53',"
          "'23:59:00');".format(name))
    c.execute(i1)
    c.execute(i2)
    yield name
    c.execute(drop)


@pytest.fixture
def all_types_table(con):
    """A table with all the supported types

    - boolean_
    - smallint_
    - int_
    - bigint_
    - decimal_
    - float_
    - double_
    - varchar_
    - text_
    - time_
    - timestamp_
    - date_

    https://www.mapd.com/docs/latest/mapd-core-guide/tables/#create-table
    """
    name = 'all_types'
    drop = 'drop table if exists {};'.format(name)
    c = con.cursor()
    c.execute(drop)
    create = textwrap.dedent('''\
    create table {name} (
        boolean_ BOOLEAN,
        smallint_ SMALLINT,
        int_ INT,
        bigint_ BIGINT,
        float_ FLOAT,
        double_ DOUBLE,
        varchar_ VARCHAR(40),
        text_ TEXT,
        time_ TIME,
        timestamp_ TIMESTAMP,
        date_ DATE
    );'''.format(name=name))
    # skipping decimal for now
    c.execute(create)
    return name


@pytest.fixture
def mock_transport(mocker):
    """A magicmock for pymapd.connection.TTransport.TBufferedTransport"""
    return mocker.patch("pymapd.connection.TTransport.TBufferedTransport")


@pytest.fixture
def mock_client(mocker):
    """A magicmock for pymapd.connection.Client"""
    return mocker.patch("pymapd.connection.Client")
