# -*- coding: utf-8 -*-
import os
import subprocess
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


@pytest.fixture
def mock_transport(mocker):
    """A magicmock for pymapd.connection.TTransport.TBufferedTransport"""
    return mocker.patch("pymapd.connection.TTransport.TBufferedTransport")


@pytest.fixture
def mock_client(mocker):
    """A magicmock for pymapd.connection.Client"""
    return mocker.patch("pymapd.connection.Client")
