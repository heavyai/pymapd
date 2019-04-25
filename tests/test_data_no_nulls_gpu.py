"""
The intent of this file is to be a full integration test. Whenever possible,
evaluate tests not only that a data type works, but that it works in the
presence of the other data types as well in the same dataframe/database table
"""
import pytest
import pandas as pd

from .conftest import _tests_table_no_nulls, no_gpu


@pytest.mark.usefixtures("mapd_server")
class TestGPUDataNoNulls:

    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    @pytest.mark.parametrize('method', ["rows", "columnar", "arrow", "infer"])
    def test_select_ipc_gpu(self, con, method):

        df_in = _tests_table_no_nulls(10000)
        # keep columns that pass, make issues for ones that don't
        df_in_limited = df_in[["smallint_",
                               "int_",
                               "bigint_",
                               "float_",
                               "double_"]].copy()

        con.execute("drop table if exists test_data_no_nulls_gpu;")
        con.load_table("test_data_no_nulls_gpu", df_in_limited, method=method)
        df_gdf = con.select_ipc_gpu("select * from test_data_no_nulls_gpu")

        # validate dtypes are exactly the same
        assert pd.DataFrame.equals(df_in_limited.dtypes, df_gdf.dtypes)

        # bring gdf local to CPU, do comparison
        df_gdf_cpu_copy = df_gdf.to_pandas()

        df_in_limited.sort_values(by=['smallint_',
                                      'int_',
                                      'bigint_'], inplace=True)
        df_in_limited.reset_index(drop=True, inplace=True)

        df_gdf_cpu_copy.sort_values(by=['smallint_',
                                        'int_',
                                        'bigint_'], inplace=True)
        df_gdf_cpu_copy.reset_index(drop=True, inplace=True)

        assert pd.DataFrame.equals(df_in_limited, df_gdf_cpu_copy)

        con.execute("drop table if exists test_data_no_nulls_gpu;")
