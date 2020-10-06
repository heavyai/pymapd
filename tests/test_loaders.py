from decimal import Decimal
import datetime

import pytest

from pymapd._loaders import _build_input_rows
from pymapd import _pandas_loaders
from pymapd._parsers import ColumnDetails
from omnisci.thrift.OmniSci import (
    TStringRow,
    TStringValue,
    TColumn,
    TColumnData,
)
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, LineString, Polygon, MultiPolygon
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
        ColumnDetails(**{**common_col_params, **properties})
        for properties in col_properties
    ]


def get_expected(data, col_properties):
    expected = []
    _map_col_types = {
        'INT': 'int_col',
        'DOUBLE': 'real_col',
        'STR': 'str_col',
        'TIMESTAMP': 'int_col',
        'DECIMAL': 'int_col',
    }
    _map_col_types.update(
        {k: 'str_col' for k in _pandas_loaders.GEO_TYPE_NAMES}
    )
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
        elif prop['type'] in _pandas_loaders.GEO_TYPE_NAMES:
            col = TColumn(
                data=TColumnData(
                    **{
                        _map_col_types[prop['type']]: data[prop['name']].apply(
                            lambda g: g.wkt
                        )
                    }
                ),
                nulls=nulls,
            )
        else:
            if prop['type'] == 'TIMESTAMP':
                # convert datetime to epoch
                if data[prop['name']].dt.nanosecond.sum():
                    data[prop['name']] = data[prop['name']].astype(int)
                else:
                    data[prop['name']] = (
                        data[prop['name']].astype(int) // 10 ** 9
                    )
            elif prop['type'] == 'DECIMAL':
                # data = (data * 10 ** precision).astype(int) \
                #   * 10 ** (scale - precision)
                data[prop['name']] = (
                    data[prop['name']] * 10 ** prop['precision']
                ).astype(int) * 10 ** (prop['scale'] - prop['precision'])

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
        dt_microsecond_format = '%Y-%m-%d %H:%M:%S.%f'

        def get_dt_nanosecond(v):
            return np.datetime64('201{}-01-01 01:01:01.001001001'.format(v))

        def get_dt_microsecond(v):
            return datetime.datetime.strptime(
                '201{}-01-01 01:01:01.001001'.format(v), dt_microsecond_format
            )

        data = [
            (1, 'a', get_dt_nanosecond(1), get_dt_microsecond(1)),
            (2, 'b', get_dt_nanosecond(2), get_dt_microsecond(2)),
        ]
        result = _build_input_rows(data)
        # breakpoint
        expected = [
            TStringRow(
                cols=[
                    TStringValue(str_val='1', is_null=None),
                    TStringValue(str_val='a', is_null=None),
                    TStringValue(
                        str_val=get_dt_nanosecond(1).astype(str), is_null=None
                    ),
                    TStringValue(
                        str_val=get_dt_microsecond(1).strftime(
                            dt_microsecond_format
                        ),
                        is_null=None,
                    ),
                ]
            ),
            TStringRow(
                cols=[
                    TStringValue(str_val='2', is_null=None),
                    TStringValue(str_val='b', is_null=None),
                    TStringValue(
                        str_val=get_dt_nanosecond(2).astype(str), is_null=None
                    ),
                    TStringValue(
                        str_val=get_dt_microsecond(2).strftime(
                            dt_microsecond_format
                        ),
                        is_null=None,
                    ),
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
                        'd': [
                            np.datetime64('2010-01-01 01:01:01.001001001'),
                            np.datetime64('2011-01-01 01:01:01.001001001'),
                            np.datetime64('2012-01-01 01:01:01.001001001'),
                        ],
                        'e': [
                            datetime.datetime.strptime(
                                '2010-01-01 01:01:01.001001',
                                '%Y-%m-%d %H:%M:%S.%f',
                            ),
                            datetime.datetime.strptime(
                                '2011-01-01 01:01:01.001001',
                                '%Y-%m-%d %H:%M:%S.%f',
                            ),
                            datetime.datetime.strptime(
                                '2012-01-01 01:01:01.001001',
                                '%Y-%m-%d %H:%M:%S.%f',
                            ),
                        ],
                        'f': [
                            Decimal('1.1234'),
                            Decimal('2.2345'),
                            Decimal('3.3456'),
                        ],
                    }
                ),
                [
                    {'name': 'a', 'type': 'INT', 'is_array': True},
                    {'name': 'b', 'type': 'DOUBLE', 'is_array': True},
                    {'name': 'c', 'type': 'INT', 'is_array': False},
                    {
                        'name': 'd',
                        'type': 'TIMESTAMP',
                        'is_array': False,
                        'precision': 9,
                    },
                    {
                        'name': 'e',
                        'type': 'TIMESTAMP',
                        'is_array': False,
                        'precision': 0,
                    },
                    {
                        'name': 'f',
                        'type': 'DECIMAL',
                        'is_array': False,
                        'scale': 10,
                        'precision': 4,
                    },
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
                id='one-col-array-nullable-and-empty-list',
            ),
            pytest.param(
                gpd.GeoDataFrame(
                    {
                        'a': [Point(0, 0), Point(1, 1)],
                        'b': [
                            LineString([(2, 0), (2, 4), (3, 4)]),
                            LineString([(0, 0), (1, 1)]),
                        ],
                        'c': [
                            Polygon([(0, 0), (1, 1), (1, 0)]),
                            Polygon([[0, 0], [0, 4], [4, 4], [4, 0]]),
                        ],
                        'd': [
                            MultiPolygon([Polygon([(0, 0), (1, 1), (1, 0)])]),
                            MultiPolygon(
                                [Polygon([[0, 0], [0, 4], [4, 4], [4, 0]])]
                            ),
                        ],
                    }
                ),
                [
                    {'name': 'a', 'type': 'POINT', 'is_array': False},
                    {'name': 'b', 'type': 'LINESTRING', 'is_array': False},
                    {'name': 'c', 'type': 'POLYGON', 'is_array': False},
                    {'name': 'd', 'type': 'MULTIPOLYGON', 'is_array': False},
                ],
                id='multi-col-geo-nullable',
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
                name='timestamp_',
                type='TIMESTAMP',
                nullable=True,
                precision=0,
                scale=0,
                comp_param=0,
                encoding='NONE',
                is_array=False,
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
            scale=0,
            comp_param=0,
            encoding='NONE',
            is_array=False,
        )

        col_types = [
            ColumnDetails(
                name='boolean_', type='BOOL', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='int_', type='INT', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='bigint_', type='BIGINT', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='double_', type='DOUBLE', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='varchar_', type='STR', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='text_', type='STR', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='time_', type='TIME', precision=0, **common_col_params
            ),
            ColumnDetails(
                name='timestamp_',
                type='TIMESTAMP',
                **common_col_params,
                precision=0,
            ),
            ColumnDetails(
                name='date_', type='DATE', precision=0, **common_col_params
            ),
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
                'timestamp1_': [pd.Timestamp('2016'), pd.Timestamp('2017')],
                'timestamp2_': [
                    np.datetime64('2016-01-01 01:01:01.001001001'),
                    np.datetime64('2017-01-01 01:01:01.001001001'),
                ],
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
                'timestamp1_',
                'timestamp2_',
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
            TColumnType(col_name='timestamp1_', col_type=TTypeInfo(type=8)),
            TColumnType(
                col_name='timestamp2_', col_type=TTypeInfo(type=8, precision=9)
            ),
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
