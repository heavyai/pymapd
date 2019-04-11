import pytest
import pandas as pd
import numpy as np


def _tests_table_no_nulls(n_samples):

    np.random.seed(12345)

    tinyint_ = np.random.randint(low=-127, high=127, size=n_samples,
                                 dtype='int8')

    smallint_ = np.random.randint(low=-32767, high=32767, size=n_samples,
                                  dtype='int16')

    int_ = np.random.randint(low=-2147483647, high=2147483647, size=n_samples,
                             dtype='int32')

    bigint_ = np.random.randint(low=-9223372036854775807,
                                high=9223372036854775807,
                                size=n_samples,
                                dtype='int64')

    float_ = np.linspace(-3.4e37, 3.4e37, n_samples, dtype='float32')

    double_ = np.linspace(-1.79e307, 1.79e307, n_samples, dtype='float64')

    d = {'tinyint_': tinyint_,
         'smallint_': smallint_,
         'int_': int_,
         'bigint_': bigint_,
         'float_': float_,
         'double_': double_
         }

    return pd.DataFrame(d)


@pytest.mark.usefixtures("mapd_server")
class TestDataNoNulls:

    @pytest.mark.parametrize('method', ["rows", "columnar", "arrow", "infer"])
    def test_create_load_table_no_nulls(self, con, method):

        df_in = _tests_table_no_nulls(10000)
        con.execute("drop table if exists test_data_no_nulls;")
        con.load_table("test_data_no_nulls", df_in, method=method)

        # read_sql() uses execute() under the hood
        df_out = pd.read_sql("select * from test_data_no_nulls", con)

        # test size and table definition
        assert df_in.shape == df_out.shape
        gtd = con.get_table_details("test_data_no_nulls")
        name_types = [(x.name, x.type) for x in gtd]
        assert name_types == [('tinyint_', 'TINYINT'),
                              ('smallint_', 'SMALLINT'),
                              ('int_', 'INT'),
                              ('bigint_', 'BIGINT'),
                              ('float_', 'FLOAT'),
                              ('double_', 'DOUBLE'),
                              ]

        # sort tables to ensure data in same order before compare
        # need to sort by all the columns in case of ties
        df_in.sort_values(by=['tinyint_',
                              'smallint_',
                              'int_',
                              'bigint_'], inplace=True)
        df_in.reset_index(drop=True, inplace=True)

        df_out.sort_values(by=['tinyint_',
                               'smallint_',
                               'int_',
                               'bigint_'], inplace=True)
        df_out.reset_index(drop=True, inplace=True)

        # pymapd won't necessarily return exact dtype as input using execute()
        # and pd.read_sql() since transport is rows of tuples
        # test that results arethe same when dtypes aligned
        assert pd.DataFrame.equals(df_in["tinyint_"],
                                   df_out["tinyint_"].astype('int8'))
        assert pd.DataFrame.equals(df_in["smallint_"],
                                   df_out["smallint_"].astype('int16'))
        assert pd.DataFrame.equals(df_in["int_"],
                                   df_out["int_"].astype('int32'))
        assert pd.DataFrame.equals(df_in["bigint_"], df_out["bigint_"])
        assert all(np.isclose(df_in["float_"], df_out["float_"]))
        assert all(np.isclose(df_in["double_"], df_out["double_"]))

        # select_ipc uses Arrow, so expect exact df dtypes back
        df_out_arrow = con.select_ipc("select * from test_data_no_nulls")
        df_out_arrow.sort_values(by=['tinyint_',
                                     'smallint_',
                                     'int_',
                                     'bigint_'], inplace=True)
        df_out_arrow.reset_index(drop=True, inplace=True)
        assert pd.DataFrame.equals(df_in, df_out_arrow)

        con.execute("drop table if exists test_data_no_nulls;")
