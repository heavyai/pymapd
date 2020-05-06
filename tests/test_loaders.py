import pytest
import datetime
from pymapd._loaders import _build_input_rows
from pymapd import _pandas_loaders
from pymapd._parsers import ColumnDetails
from omnisci.thrift.OmniSci import (
    TStringRow,
    TStringValue,
    TColumn,
    TColumnData,
)
import pandas as pd
import numpy as np
from omnisci.thrift.ttypes import TColumnType
from omnisci.common.ttypes import TTypeInfo


def assert_columnar_equal(result, expected):

    assert len(result) == len(expected)

    for i, (a, b) in enumerate(zip(result, expected)):
        np.testing.assert_array_equal(a.nulls, b.nulls)
        np.testing.assert_array_equal(a.data.int_col, b.data.int_col)
        np.testing.assert_array_equal(a.data.real_col, b.data.real_col)
        np.testing.assert_array_equal(a.data.str_col, b.data.str_col)
        np.testing.assert_array_equal(a.data.arr_col, b.data.arr_col)


def get_col_types(col_properties: dict):
    common_col_params = dict(
        nullable=True,
        precision=0,
        scale=0,
        comp_param=0,
        encoding='NONE',
        # is_array=True,
    )

    return [
        ColumnDetails(**properties, **common_col_params)
        for properties in col_properties
    ]


def get_expected(data, col_properties):
    expected = []
    _map_col_types = {'INT': 'int_col', 'DOUBLE': 'real_col', 'STR': 'str_col'}
    isnull = data.isnull()
    for prop in col_properties:
        nulls = isnull[prop['name']].tolist()
        if prop['is_array']:
            arr_col = []
            for v in data[prop['name']]:
                arr_col.append(
                    TColumn(
                        data=TColumnData(**{_map_col_types[prop['type']]: v})
                    ),
                )
            col = TColumn(data=TColumnData(arr_col=arr_col), nulls=nulls)
        else:
            col = TColumn(
                data=TColumnData(
                    **{_map_col_types[prop['type']]: data[prop['name']]}
                ),
                nulls=nulls,
            )
        expected.append(col)
    return expected


class TestLoaders:
    def test_build_input_rows(self):
        data = [(1, 'a'), (2, 'b')]
        result = _build_input_rows(data)
        expected = [
            TStringRow(
                cols=[
                    TStringValue(str_val='1', is_null=None),
                    TStringValue(str_val='a', is_null=None),
                ]
            ),
            TStringRow(
                cols=[
                    TStringValue(str_val='2', is_null=None),
                    TStringValue(str_val='b', is_null=None),
                ]
            ),
        ]

        assert result == expected

    def test_build_input_rows_with_array(self):
        data = [(1, 'a'), (2, 'b'), (3, ['c', 'd', 'e'])]
        result = _build_input_rows(data)
        expected = [
            TStringRow(
                cols=[
                    TStringValue(str_val='1', is_null=None),
                    TStringValue(str_val='a', is_null=None),
                ]
            ),
            TStringRow(
                cols=[
                    TStringValue(str_val='2', is_null=None),
                    TStringValue(str_val='b', is_null=None),
                ]
            ),
            TStringRow(
                cols=[
                    TStringValue(str_val='3', is_null=None),
                    TStringValue(str_val='{c,d,e}', is_null=None),
                ]
            ),
        ]

        assert result == expected

    @pytest.mark.parametrize(
        'data, col_properties',
        [
            pytest.param(
                pd.DataFrame(
                    {
                        'a': [[1, 1], [2, 2], [3, 3]],
                        'b': [[1.1, 1.1], [2.2, 2.2], [3.3, 3.3]],
                        'c': [1, 2, 3],
                    }
                ),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': True},
                    {'name': 'b', 'type': 'DOUBLE', 'is_array': True},
                    {'name': 'c', 'type': 'INT', 'is_array': False},
                ],
                id='mult-cols-mix-array-not-null',
            ),
            pytest.param(
                pd.DataFrame(
                    {
                        'a': [[1, 1], [2, 2], [3, 3]],
                        'b': [[1.1, 1.1], [2.2, 2.2], [3.3, 3.3]],
                    }
                ),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': True},
                    {'name': 'b', 'type': 'DOUBLE', 'is_array': True},
                ],
                id='mult-cols-array-not-null',
            ),
            pytest.param(
                pd.DataFrame({'a': [1, 2, 3], 'b': [1.1, 2.2, 3.3]}),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': False},
                    {'name': 'b', 'type': 'DOUBLE', 'is_array': False},
                ],
                id='mult-cols-not-null',
            ),
            pytest.param(
                pd.DataFrame(
                    [
                        {'a': [2, 3, 4]},
                        {'a': [4444]},
                        {'a': []},
                        {'a': []},
                        {'a': [2, 3, 4]},
                    ]
                ),
                [{'name': 'a', 'type': 'INT', 'is_array': True}],
                id='one-col-array-not-null',
            ),
            pytest.param(
                pd.DataFrame(
                    data=[
                        {'a': [2, 3, 4], 'b': 'teststr'},
                        {'a': [2, 3], 'b': 'teststr'},
                        {'a': [4444], 'b': 'teststr'},
                        {'a': [], 'b': 'teststr'},
                        {'a': [2, 3, 4], 'b': 'teststr'},
                    ]
                ),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': True},
                    {'name': 'b', 'type': 'STR', 'is_array': False},
                ],
                id='mult-cols-mix-array-not-null',
            ),
            pytest.param(
                pd.DataFrame(
                    data=[
                        {'a': [2, 3, 4], 'b': 'teststr'},
                        {'a': [2, 3], 'b': 'teststr'},
                        {'a': [4444], 'b': 'teststr'},
                        {'a': None, 'b': 'teststr'},
                        {'a': [2, 3, 4], 'b': 'teststr'},
                    ]
                ),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': True},
                    {'name': 'b', 'type': 'STR', 'is_array': False},
                ],
                id='mult-cols-mix-array-nullable',
            ),
            pytest.param(
                pd.DataFrame(
                    [
                        {'a': [2, 3, 4]},
                        {'a': [4444]},
                        {'a': None},
                        {'a': []},
                        {'a': [2, 3, 4]},
                    ]
                ),
                [{'name': 'a', 'type': 'INT', 'is_array': True}],
                id='one-col-array-nullable',
            ),
        ],
    )
    def test_build_table_columnar(self, data, col_properties):

        from pymapd._pandas_loaders import build_input_columnar

        col_types = get_col_types(col_properties)

        result = build_input_columnar(
            data,
            preserve_index=False,
            col_names=data.columns,
            col_types=col_types,
        )
        expected = get_expected(data, col_properties)

        assert data.shape[1] == len(expected)
        assert_columnar_equal(result[0], expected)

    def test_build_table_columnar_pandas(self):
        common_col_params = dict(
            nullable=True,
            precision=0,
            scale=0,
            comp_param=0,
            encoding='NONE',
            is_array=False,
        )

        col_types = [
            ColumnDetails(name='boolean_', type='BOOL', **common_col_params),
            ColumnDetails(
                name='smallint_', type='SMALLINT', **common_col_params
            ),
            ColumnDetails(name='int_', type='INT', **common_col_params),
            ColumnDetails(name='bigint_', type='BIGINT', **common_col_params),
            ColumnDetails(name='float_', type='FLOAT', **common_col_params),
            ColumnDetails(name='double_', type='DOUBLE', **common_col_params),
            ColumnDetails(name='varchar_', type='STR', **common_col_params),
            ColumnDetails(name='text_', type='STR', **common_col_params),
            ColumnDetails(name='time_', type='TIME', **common_col_params),
            ColumnDetails(
                name='timestamp_', type='TIMESTAMP', **common_col_params
            ),
            ColumnDetails(name='date_', type='DATE', **common_col_params),
        ]

        data = pd.DataFrame(
            {
                'boolean_': [True, False],
                'smallint_': np.array([0, 1], dtype=np.int16),
                'int_': np.array([0, 1], dtype=np.int32),
                'bigint_': np.array([0, 1], dtype=np.int64),
                'float_': np.array([0, 1], dtype=np.float32),
                'double_': np.array([0, 1], dtype=np.float64),
                'varchar_': ['a', 'b'],
                'text_': ['a', 'b'],
                'time_': [datetime.time(0, 11, 59), datetime.time(13)],
                'timestamp_': [pd.Timestamp('2016'), pd.Timestamp('2017')],
                'date_': [
                    datetime.date(2016, 1, 1),
                    datetime.date(2017, 1, 1),
                ],
            }
        )
        result = _pandas_loaders.build_input_columnar(
            data,
            preserve_index=False,
            col_names=data.columns,
            col_types=col_types,
        )

        nulls = [False, False]
        expected = [
            TColumn(TColumnData(int_col=[True, False]), nulls=nulls),
            TColumn(
                TColumnData(int_col=np.array([0, 1], dtype=np.int16)),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(int_col=np.array([0, 1], dtype=np.int32)),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(int_col=np.array([0, 1], dtype=np.int64)),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(real_col=np.array([0, 1], dtype=np.float32)),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(real_col=np.array([0, 1], dtype=np.float64)),
                nulls=nulls,
            ),  # noqa
            TColumn(TColumnData(str_col=['a', 'b']), nulls=nulls),
            TColumn(TColumnData(str_col=['a', 'b']), nulls=nulls),
            TColumn(TColumnData(int_col=[719, 46800]), nulls=nulls),
            TColumn(
                TColumnData(int_col=[1451606400, 1483228800]), nulls=nulls
            ),  # noqa
            TColumn(
                TColumnData(int_col=[1451606400, 1483228800]), nulls=nulls
            ),
        ]
        assert_columnar_equal(result[0], expected)

    def test_build_table_columnar_nulls(self):
        common_col_params = dict(
            nullable=True,
            precision=0,
            scale=0,
            comp_param=0,
            encoding='NONE',
            is_array=False,
        )

        col_types = [
            ColumnDetails(name='boolean_', type='BOOL', **common_col_params),
            ColumnDetails(name='int_', type='INT', **common_col_params),
            ColumnDetails(name='bigint_', type='BIGINT', **common_col_params),
            ColumnDetails(name='double_', type='DOUBLE', **common_col_params),
            ColumnDetails(name='varchar_', type='STR', **common_col_params),
            ColumnDetails(name='text_', type='STR', **common_col_params),
            ColumnDetails(name='time_', type='TIME', **common_col_params),
            ColumnDetails(
                name='timestamp_', type='TIMESTAMP', **common_col_params
            ),
            ColumnDetails(name='date_', type='DATE', **common_col_params),
        ]

        data = pd.DataFrame(
            {
                'boolean_': [True, False, None],
                # Currently Pandas does not support storing None or NaN
                # in integer columns, so int cols with null
                # need to be objects. This means our type detection will be
                # unreliable since if there is no number outside the int32
                # bounds in a column with nulls then we will be assuming int
                'int_': np.array([0, 1, None], dtype=np.object),
                'bigint_': np.array(
                    [0, 9223372036854775807, None], dtype=np.object
                ),
                'double_': np.array([0, 1, None], dtype=np.float64),
                'varchar_': ['a', 'b', None],
                'text_': ['a', 'b', None],
                'time_': [datetime.time(0, 11, 59), datetime.time(13), None],
                'timestamp_': [
                    pd.Timestamp('2016'),
                    pd.Timestamp('2017'),
                    None,
                ],
                'date_': [
                    datetime.date(1001, 1, 1),
                    datetime.date(2017, 1, 1),
                    None,
                ],
            }
        )

        result = _pandas_loaders.build_input_columnar(
            data,
            preserve_index=False,
            col_names=data.columns,
            col_types=col_types,
        )

        nulls = [False, False, True]
        bool_na = -128
        int_na = -2147483648
        bigint_na = -9223372036854775808
        ns_na = -9223372037
        double_na = 0

        expected = [
            TColumn(TColumnData(int_col=[1, 0, bool_na]), nulls=nulls),
            TColumn(
                TColumnData(int_col=np.array([0, 1, int_na], dtype=np.int32)),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(
                    int_col=np.array(
                        [0, 9223372036854775807, bigint_na], dtype=np.int64
                    )
                ),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(
                    real_col=np.array([0, 1, double_na], dtype=np.float64)
                ),
                nulls=nulls,
            ),  # noqa
            TColumn(TColumnData(str_col=['a', 'b', '']), nulls=nulls),
            TColumn(TColumnData(str_col=['a', 'b', '']), nulls=nulls),
            TColumn(TColumnData(int_col=[719, 46800, bigint_na]), nulls=nulls),
            TColumn(
                TColumnData(int_col=[1451606400, 1483228800, ns_na]),
                nulls=nulls,
            ),  # noqa
            TColumn(
                TColumnData(int_col=[-30578688000, 1483228800, bigint_na]),
                nulls=nulls,
            ),  # noqa
        ]
        assert_columnar_equal(result[0], expected)

    def test_build_row_desc(self):

        data = pd.DataFrame(
            {
                'boolean_': [True, False],
                'smallint_': np.array([0, 1], dtype=np.int16),
                'int_': np.array([0, 1], dtype=np.int32),
                'bigint_': np.array([0, 1], dtype=np.int64),
                'float_': np.array([0, 1], dtype=np.float32),
                'double_': np.array([0, 1], dtype=np.float64),
                'varchar_': ['a', 'b'],
                'text_': ['a', 'b'],
                'time_': [datetime.time(0, 11, 59), datetime.time(13)],
                'timestamp_': [pd.Timestamp('2016'), pd.Timestamp('2017')],
                'date_': [
                    datetime.date(2016, 1, 1),
                    datetime.date(2017, 1, 1),
                ],
            },
            columns=[
                'boolean_',
                'smallint_',
                'int_',
                'bigint_',
                'float_',
                'double_',
                'varchar_',
                'text_',
                'time_',
                'timestamp_',
                'date_',
            ],
        )
        result = _pandas_loaders.build_row_desc(data)
        expected = [
            TColumnType(
                col_name='boolean_',
                col_type=TTypeInfo(type=10),
                is_reserved_keyword=None,
            ),
            TColumnType(
                col_name='smallint_',
                col_type=TTypeInfo(type=0),
                is_reserved_keyword=None,
            ),
            TColumnType(
                col_name='int_',
                col_type=TTypeInfo(type=1),
                is_reserved_keyword=None,
            ),
            TColumnType(col_name='bigint_', col_type=TTypeInfo(type=2)),
            TColumnType(col_name='float_', col_type=TTypeInfo(type=3)),
            TColumnType(col_name='double_', col_type=TTypeInfo(type=5)),
            TColumnType(
                col_name='varchar_', col_type=TTypeInfo(type=6, encoding=4)
            ),
            TColumnType(
                col_name='text_', col_type=TTypeInfo(type=6, encoding=4)
            ),
            TColumnType(col_name='time_', col_type=TTypeInfo(type=7)),
            TColumnType(col_name='timestamp_', col_type=TTypeInfo(type=8)),
            TColumnType(col_name='date_', col_type=TTypeInfo(type=9)),
        ]

        assert result == expected

        data.index.name = 'idx'
        result = _pandas_loaders.build_row_desc(data, preserve_index=True)
        expected.insert(
            0, TColumnType(col_name='idx', col_type=TTypeInfo(type=2))
        )

        assert result == expected

    def test_create_non_pandas_raises(self):
        with pytest.raises(TypeError) as m:
            _pandas_loaders.build_row_desc([(1, 'a'), (2, 'b')])

        assert m.match('is not supported for type ')
