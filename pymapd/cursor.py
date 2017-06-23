from collections import namedtuple
from typing import (Iterator, Iterable, List, Optional, Tuple, Any, Union)  # noqa

import six
import mapd.ttypes as T  # noqa

from .exceptions import _translate_exception


Description = namedtuple("Description", ["name", "type_code", "display_size",
                                         "internal_size", "precision", "scale",
                                         "null_ok"])


class Cursor(object):
    """A database cursor."""

    def __init__(self, connection, columnar=True):
        # type: (Any, bool) -> None
        # XXX: supposed to share state between cursors of the same connection
        self.connection = connection
        self.columnar = columnar
        self.rowcount = -1
        self._description = None  # type: Optional[List[Description]]
        self._arraysize = 1
        self._result = None
        self._result_set = None  # type: Optional[Iterator[Any]]

    def __iter__(self):
        # type: () -> Union[List, Iterator]
        if self.result_set is None:
            return iter([])
        return self.result_set

    @property
    def description(self):
        # type: () -> Optional[List[Description]]
        """
        Read-only sequence describing columns of the result set.
        Each column is an instance of `Description` describing

        - name
        - type_code
        - display_size
        - internal_size
        - precision
        - scale
        - null_ok

        We only use name, type_code, and null_ok; The rest are always ``None``
        """
        return self._description

    @property
    def result_set(self):
        # type: () -> Optional[Iterator]
        return self._result_set

    @property
    def arraysize(self):
        # type: () -> int
        """The number of rows to fetch at a time with `fetchmany`. Default 1.

        See Also
        --------
        fetchmany
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        # type: (int) -> None
        """Number of items to fetch with :func:`fetchmany`."""
        if not isinstance(value, int):
            raise TypeError("Value must be an integer, got {} instead".format(
                type(value)))
        self._arraysize = value

    def close(self):
        # type: () -> None
        """Close this cursor."""
        pass

    def execute(self, operation, parameters=None):
        # type: (str, tuple) -> Cursor
        """Execute a SQL statement.

        Parameters
        ----------
        operation : str
            A SQL query
        parameters : tuple
            Parameters to substitute into ``operation``.

        Returns
        -------
        self : Cursor
        """
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
        # type: (str, Iterable) -> None
        pass

    def fetchone(self):
        # type: () -> Optional[Any]
        """Fetch a single row from the results set"""
        try:
            return next(self.result_set)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        # type: (Optional[int]) -> Iterable[Any]
        """Fetch ``size`` rows from the results set."""
        if size is None:
            size = self.arraysize
        results = [self.fetchone() for _ in range(size)]
        return [x for x in results if x is not None]

    def fetchall(self):
        # type: () -> Any
        return list(self)

    def setinputsizes(self, sizes):
        # type: (int) -> None
        pass

    def setoutputsizes(self, size, column=None):
        # type: (int, Optional[Any]) -> None
        pass


# -----------------------------------------------------------------------------
# Result Sets
# -----------------------------------------------------------------------------

def make_row_results_set(data):
    # type: (T.QueryResultSet) -> Iterator[Tuple]
    """
    Build a results set of python objects.

    Parameters
    ----------
    data : QueryResultSet

    Returns
    -------
    results : Iterator[tuple]
    """
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
    # type: (List[T.TColumnType]) -> List[Description]
    """
    Return a tuple of (name, type_code, display_size, internal_size,
                       precision, scale, null_ok)

    https://www.python.org/dev/peps/pep-0249/#description
    """
    return [Description(col.col_name, col.col_type.type,
                        None, None, None, None,
                        col.col_type.nullable)
            for col in row_desc]
