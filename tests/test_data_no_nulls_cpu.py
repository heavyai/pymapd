"""
The intent of this file is to be a full integration test. Whenever possible,
add a datatype to the main _tests_table_no_nulls function, so that the tests
will evaluate not only that a data type works, but that it works in the
presence of the other data types as well in the same dataframe/database table
"""
import pytest
import pandas as pd
import numpy as np
from datetime import date, time, datetime, timedelta
import random
import string


def gen_string():
    return ''.join([random.choice(string.ascii_letters + string.digits)
                   for n in range(10)])


def _tests_table_no_nulls(n_samples):

    np.random.seed(12345)

    tinyint_ = np.random.randint(low=-127,
                                 high=127,
                                 size=n_samples,
                                 dtype='int8')

    smallint_ = np.random.randint(low=-32767,
                                  high=32767,
                                  size=n_samples,
                                  dtype='int16')

    int_ = np.random.randint(low=-2147483647,
                             high=2147483647,
                             size=n_samples,
                             dtype='int32')

    bigint_ = np.random.randint(low=-9223372036854775807,
                                high=9223372036854775807,
                                size=n_samples,
                                dtype='int64')

    # float and double ranges slightly lower than we support, full width
    # causes an error in np.linspace that's not worth tracking down
    float_ = np.linspace(-3.4e37, 3.4e37, n_samples, dtype='float32')
    double_ = np.linspace(-1.79e307, 1.79e307, n_samples, dtype='float64')

    bool_ = np.random.randint(low=0, high=2, size=n_samples, dtype='bool')

    # effective date range of 1904 to 2035
    # TODO: validate if this is an Arrow limitation, outside this range fails
    date_ = [date(1970, 1, 1) + timedelta(days=int(x))
             for x in np.random.randint(-24000, 24000, size=n_samples)]

    datetime_ = [datetime(1970, 1, 1) + timedelta(days=int(x), minutes=int(x))
                 for x in np.random.randint(-24000, 24000, size=n_samples)]

    time_h = np.random.randint(0, 24, size=n_samples)
    time_m = np.random.randint(0, 60, size=n_samples)
    time_s = np.random.randint(0, 60, size=n_samples)
    time_ = [time(h, m, s) for h, m, s in zip(time_h, time_m, time_s)]

    text_ = [gen_string() for x in range(n_samples)]

    d = {'tinyint_': tinyint_,
         'smallint_': smallint_,
         'int_': int_,
         'bigint_': bigint_,
         'float_': float_,
         'double_': double_,
         'bool_': bool_,
         'date_': date_,
         'datetime_': datetime_,
         'time_': time_,
         'text_': text_
         }

    return pd.DataFrame(d)


@pytest.mark.usefixtures("mapd_server")
class TestDataNoNulls:

    @pytest.mark.parametrize('method', ["rows", "columnar", "arrow", "infer"])
    def test_create_load_table_no_nulls_sql_execute(self, con, method):

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
                              ('bool_', 'BOOL'),
                              ('date_', 'DATE'),
                              ('datetime_', 'TIMESTAMP'),
                              ('time_', 'TIME'),
                              ('text_', 'STR'),
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
        # test that results are the same when dtypes aligned
        assert pd.DataFrame.equals(df_in["tinyint_"],
                                   df_out["tinyint_"].astype('int8'))

        assert pd.DataFrame.equals(df_in["smallint_"],
                                   df_out["smallint_"].astype('int16'))

        assert pd.DataFrame.equals(df_in["int_"],
                                   df_out["int_"].astype('int32'))

        assert pd.DataFrame.equals(df_in["bigint_"], df_out["bigint_"])

        assert all(np.isclose(df_in["float_"], df_out["float_"]))

        assert all(np.isclose(df_in["double_"], df_out["double_"]))

        assert pd.DataFrame.equals(df_in["bool_"], df_out["bool_"].astype('bool'))  # noqa

        assert pd.DataFrame.equals(df_in["date_"], df_out["date_"])

        assert pd.DataFrame.equals(df_in["datetime_"], df_out["datetime_"])

        assert pd.DataFrame.equals(df_in["time_"], df_out["time_"])

        assert pd.DataFrame.equals(df_in["text_"], df_out["text_"])

        con.execute("drop table if exists test_data_no_nulls;")

    @pytest.mark.parametrize('method', ["rows", "columnar", "arrow", "infer"])
    def test_create_load_table_no_nulls_select_ipc(self, con, method):

        df_in = _tests_table_no_nulls(10000)
        con.execute("drop table if exists test_data_no_nulls_ipc;")
        con.load_table("test_data_no_nulls_ipc", df_in, method=method)

        # need to manually specify columns since some don't currently work
        # need to drop unsupported columns from df_in
        # (BOOL) https://github.com/omnisci/pymapd/issues/211
        df_in.drop(columns=["bool_"], inplace=True)
        df_out = con.select_ipc("""select
                                tinyint_,
                                smallint_,
                                int_,
                                bigint_,
                                float_,
                                double_,
                                date_,
                                datetime_,
                                time_,
                                text_
                                from test_data_no_nulls_ipc""")

        # test size and table definition
        assert df_in.shape == df_out.shape

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

        # When Arrow result converted to pandas, dict comes back as category
        # This providies extra functionality above base 'object' type
        df_out["text_"] = df_out["text_"].astype('object')

        # select_ipc uses Arrow, so expect exact df dtypes back
        assert pd.DataFrame.equals(df_in, df_out)

        con.execute("drop table if exists test_data_no_nulls_ipc;")
