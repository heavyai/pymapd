import os

import pytest
import importlib

ttypes = importlib.import_module("mapd.ttypes")
HERE = os.path.dirname(__file__)


def _load_thrift_object(filepath):
    d = {}
    with open(filepath) as f:
        val = f.read()
    code = "result = {}".format(val)
    # should probably find an alternative to exec, but this works for now
    exec(code, vars(ttypes), d)
    return d['result']


@pytest.fixture
def colwise():
    return _load_thrift_object(os.path.join(HERE, "data", "colwise.py"))


@pytest.fixture
def rowwise():
    return _load_thrift_object(os.path.join(HERE, "data", "rowwise.py"))


@pytest.fixture
def invalid_sql():
    return _load_thrift_object(os.path.join(HERE, "data", "invalid_sql.py"))


@pytest.fixture
def nonexistant_table():
    return _load_thrift_object(os.path.join(HERE, "data",
                                            "nonexistant_table.py"))
