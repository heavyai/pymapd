import datetime
from pymapd.cursor import make_row_results_set


class TestRowResults:

    def test_basic_colwise(self, colwise):
        expected = [
            ('2006-01-05', 'BUY', 'RHAT', 100, 35.13999938964844,
             1.100000023841858, datetime.datetime(2010, 1, 1, 12, 1, 1)),
            ('2006-01-05', 'BUY', 'GOOG', 100, 12.140000343322754,
             1.2000000476837158, datetime.datetime(2010, 1, 1, 12, 2, 2))
        ]
        result = list(make_row_results_set(colwise))
        assert result == expected

    def test_nulls_handled(self):
        from mapd.ttypes import (TRowSet, TColumnType, TTypeInfo, TColumn,
                                 TColumnData, TQueryResult, TDatum, TRow,
                                 TDatumVal)

        rs = TQueryResult(TRowSet(
            row_desc=[
                TColumnType(col_name='a',
                            col_type=TTypeInfo(type=0, nullable=True)),
                TColumnType(col_name='b',
                            col_type=TTypeInfo(type=1, nullable=True)),
                TColumnType(col_name='c',
                            col_type=TTypeInfo(type=2, nullable=True)),
                TColumnType(col_name='d',
                            col_type=TTypeInfo(type=3, nullable=True)),
                TColumnType(col_name='e',
                            col_type=TTypeInfo(type=4, nullable=True)),
                TColumnType(col_name='f',
                            col_type=TTypeInfo(type=5, nullable=True)),
                TColumnType(col_name='g',
                            col_type=TTypeInfo(type=6, nullable=True)),
                TColumnType(col_name='h',
                            col_type=TTypeInfo(type=7, nullable=True)),
                TColumnType(col_name='i',
                            col_type=TTypeInfo(type=8, nullable=True)),
                TColumnType(col_name='j',
                            col_type=TTypeInfo(type=9, nullable=True)),
                TColumnType(col_name='k',
                            col_type=TTypeInfo(type=10, nullable=True)),
            ],
            rows=[],
            columns=[
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(real_col=[-2147483648]), nulls=[True]),  # noqa
                TColumn(data=TColumnData(real_col=[-2147483648]), nulls=[True]),  # noqa
                TColumn(data=TColumnData(real_col=[-2147483648]), nulls=[True]),  # noqa
                TColumn(data=TColumnData(str_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
                TColumn(data=TColumnData(int_col=[-2147483648]), nulls=[True]),
            ],
            is_columnar=True))

        result = list(make_row_results_set(rs))
        assert result == [(None,) * 11]
