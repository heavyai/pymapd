import os
import pickle

import pytest
import importlib

ttypes = importlib.import_module("mapd.ttypes")
HERE = os.path.dirname(__file__)


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
