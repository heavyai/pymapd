"""
https://www.python.org/dev/peps/pep-0249/#type-objects
"""
# XXX: packaging
import datetime
import six
import sys
import time
sys.path.insert(0, "../mapd-core/gen-py")
from mapd import MapD  # noqa

if six.PY2:
    memoryview = buffer  # noqa


T = MapD.TDatumType


class DataType(object):

    def __init__(self, matches):
        self._matches = set(matches)

    def __eq__(self, other):
        return other in self._matches

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self, other):
        return hash(tuple(self._matches))


Binary = memoryview
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetimte

BINARY = DataType([])
STRING = DataType([T.STR])
NUMBER = DataType([T.SMALLINT, T.INT, T.BIGINT, T.FLOAT, T.DECIMAL, T.DOUBLE,
                   T.BOOL])
DATETIME = DataType([T.DATE, T.TIME, T.TIMESTAMP])
ROWID = DataType([])


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])
