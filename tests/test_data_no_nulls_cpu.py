import pytest
import pandas as pd
import numpy as np


def _tests_table_no_nulls(n_samples):

    tinyint_ = np.random.randint(low=-127, high=127,
                                 size=n_samples, dtype='int8')

    smallint_ = np.random.randint(low=-32767, high=32767,
                                  size=n_samples, dtype='int16')

    int_ = np.random.randint(low=-2147483647, high=2147483647,
                             size=n_samples, dtype='int32')

    bigint_ = np.random.randint(low=-9223372036854775807,
                                high=9223372036854775807,
                                size=n_samples,
                                dtype='int64')

    d = {'tinyint_': tinyint_,
         'smallint_': smallint_,
         'int_': int_,
         'bigint_': bigint_
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
                              ('bigint_', 'BIGINT')]

        # pymapd won't necessarily return exact dtype as input using execute()
        # and pd.read_sql() since transport is rows of tuples
        # test that results are functionally the same
        assert df_in["tinyint_"].sum() == df_out["tinyint_"].sum()
        assert df_in["smallint_"].sum() == df_out["smallint_"].sum()
        assert df_in["int_"].sum() == df_out["int_"].sum()
        assert df_in["bigint_"].sum() == df_out["bigint_"].sum()

        # select_ipc uses Arrow, so expect exact df back
        df_out_arrow = con.select_ipc("select * from test_data_no_nulls")
        assert pd.DataFrame.equals(df_in, df_out_arrow)

        con.execute("drop table if exists test_data_no_nulls;")
