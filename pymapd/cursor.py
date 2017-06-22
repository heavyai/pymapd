from collections import namedtuple

import six
import mapd.ttypes as T  # noqa

from .exceptions import _translate_exception


Description = namedtuple("Description", ["name", "type_code", "display_size",
                                         "internal_size", "precision", "scale",
                                         "null_ok"])


class Cursor(object):

    def __init__(self, connection, columnar=True):
        # XXX: supposed to share state between cursors of the same connection
        self.connection = connection
        self.columnar = columnar
        self.rowcount = -1
        self._description = None
        self._arraysize = 1
        self._result = None
        self._result_set = None

    def __iter__(self):
        return self.result_set

    @property
    def description(self):
        return self._description

    @property
    def result_set(self):
        return self._result_set

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if not isinstance(value, int):
            raise TypeError("Value must be an integer, got {} instead".format(
                type(value)))
        self._arraysize = value

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        if parameters is not None:
            raise NotImplementedError
        self.rowcount = -1
        try:
            result = self.connection._client.sql_execute(
                self.connection._session, operation,
                column_format=self.columnar,
                nonce=None, first_n=-1)
        except T.TMapDException as e:
            six.raise_from(_translate_exception(e), e)
        self._description = _extract_description(result.row_set.row_desc)
        if self.columnar:
            try:
                self.rowcount = len(result.row_set.columns[0].nulls)
            except IndexError:
                pass
        else:
            self.rowcount = len(result.row_set.rows)
        self._result_set = make_row_results_set(result)
        self._result = result
        return self

    def executemany(self, operation, parameters=None):
        pass

    def fetchone(self):
        try:
            return next(self.result_set)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        results = [self.fetchone() for _ in range(size)]
        return [x for x in results if x is not None]

    def fetchall(self):
        return list(self)

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, size, column=None):
        pass


# -----------------------------------------------------------------------------
# Result Sets
# -----------------------------------------------------------------------------

def make_row_results_set(data):
    # type: (T.QueryResultSet) -> List[Tuple]
    if is_columnar(data):
        nrows = len(data.row_set.columns[0].nulls)
        ncols = len(data.row_set.row_desc)
        columns = [_extract_col_vals(desc, col)
                   for desc, col in zip(data.row_set.row_desc,
                                        data.row_set.columns)]
        for i in range(nrows):
            yield tuple(columns[j][i] for j in range(ncols))
    else:
        for row in data.row_set.rows:
            yield tuple(_extract_row_val(desc, val)
                        for desc, val in zip(data.row_set.row_desc, row.cols))


def is_columnar(data):
    # type: (T.TQueryResult) -> bool
    return data.row_set.is_columnar


_typeattr = {
    'SMALLINT': 'int',
    'INT': 'int',
    'BIGINT': 'int',
    'TIME': 'int',
    'TIMESTAMP': 'int',
    'DATE': 'int',
    'BOOL': 'int',
    'FLOAT': 'real',
    'DECIMAL': 'real',
    'DOUBLE': 'real',
    'STR': 'str',
}


def _extract_row_val(desc, val):
    # type: (T.TColumnType, T.TDatum) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.val, _typeattr[typename] + '_val')


def _extract_col_vals(desc, val):
    # type: (T.TColumnType, T.TColumn) -> Any
    typename = T.TDatumType._VALUES_TO_NAMES[desc.col_type.type]
    return getattr(val.data, _typeattr[typename] + '_col')


def _extract_description(row_desc):
    """
    Return a tuple of (name, type_code, display_size, internal_size,
                       precision, scale, null_ok)

    https://www.python.org/dev/peps/pep-0249/#description
    """
    return [Description(col.col_name, col.col_type.type,
                        None, None, None, None,
                        col.col_type.nullable)
            for col in row_desc]
