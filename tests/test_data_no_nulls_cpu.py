"""
The intent of this file is to be a full integration test. Whenever possible,
add a datatype to the main _tests_table_no_nulls function, so that the tests
will evaluate not only that a data type works, but that it works in the
presence of the other data types as well in the same dataframe/database table
"""
import pytest
import pandas as pd
import numpy as np
from shapely import wkt

from .conftest import _tests_table_no_nulls


@pytest.mark.usefixtures("mapd_server")
class TestCPUDataNoNulls:

    @pytest.mark.parametrize('method', ["rows", "columnar", "arrow", "infer"])
    def test_create_load_table_no_nulls_sql_execute(self, con, method):
        """
        Demonstrate that regardless of how data loaded, roundtrip answers
        are the same when con.execute()/pd.read_sql called for row-wise
        data retrieval
        """
        df_in = _tests_table_no_nulls(10000)
        df_in.drop(columns=["point_",
                            "line_",
                            "mpoly_",
                            "poly_"], inplace=True)
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
        """
        Demonstrate that regardless of how data loaded, roundtrip answer
        is same when con.select_ipc() called to retrieve data using Arrow
        """
        # need to manually specify columns since some don't currently work
        # need to drop unsupported columns from df_in
        df_in = _tests_table_no_nulls(10000)
        df_in.drop(columns=["point_",
                            "line_",
                            "mpoly_",
                            "poly_"], inplace=True)

        con.execute("drop table if exists test_data_no_nulls_ipc;")
        con.load_table("test_data_no_nulls_ipc", df_in, method=method)

        df_out = con.select_ipc("""select
                                tinyint_,
                                smallint_,
                                int_,
                                bigint_,
                                float_,
                                double_,
                                bool_,
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

    @pytest.mark.parametrize('method', ["rows", "columnar"])
    def test_load_table_text_no_encoding_no_nulls(self, con, method):
        """
        Demonstrate that data can be loaded as text encoding none,
        assuming that user creates the table beforehand/inserting to
        pre-existing table
        """

        con.execute("drop table if exists test_text_no_encoding")

        con.execute("""create table test_text_no_encoding (
                       idx integer,
                       text_ text encoding none
                       )""")

        # reset_index adds a column to sort by, since results not guaranteed
        # to return in sorted order from OmniSci
        df_in = _tests_table_no_nulls(10000)
        df_test = df_in["text_"].reset_index()
        df_test.columns = ["idx", "text_"]

        con.load_table("test_text_no_encoding", df_test, method=method)

        df_out = pd.read_sql("""select
                                *
                                from test_text_no_encoding
                                order by idx""",
                             con)

        assert pd.DataFrame.equals(df_test, df_out)

        con.execute("drop table if exists test_text_no_encoding")

    @pytest.mark.parametrize('method', ["rows"])
    def test_load_table_geospatial_no_nulls(self, con, method):
        """
        Demonstrate that geospatial data can be loaded,
        assuming that user creates the table beforehand/inserting to
        pre-existing table
        """
        con.execute("drop table if exists test_geospatial_no_nulls")

        con.execute("""create table test_geospatial_no_nulls (
                       tinyint_ tinyint,
                       smallint_ smallint,
                       int_ integer,
                       bigint_ bigint,
                       float_ float,
                       double_ double,
                       bool_ boolean,
                       date_ date,
                       datetime_ timestamp,
                       time_ time,
                       text_ text encoding dict(32),
                       point_ point,
                       line_ linestring,
                       mpoly_ multipolygon,
                       poly_ polygon
                       )""")

        df_in = _tests_table_no_nulls(10000)
        con.load_table("test_geospatial_no_nulls", df_in, method='rows')

        df_out = pd.read_sql("""select
                             *
                             from test_geospatial_no_nulls""", con)

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

        # convert geospatial data to Shapely objects to prove their equality
        point_in = [wkt.loads(x) for x in df_in["point_"]]
        point_out = [wkt.loads(x) for x in df_out["point_"]]
        assert all([x.equals_exact(y, 0.000001) for x, y
                    in zip(point_in, point_out)])

        line_in = [wkt.loads(x) for x in df_in["line_"]]
        line_out = [wkt.loads(x) for x in df_out["line_"]]
        assert all([x.equals_exact(y, 0.000001) for x, y
                    in zip(line_in, line_out)])

        mpoly_in = [wkt.loads(x) for x in df_in["mpoly_"]]
        mpoly_out = [wkt.loads(x) for x in df_out["mpoly_"]]
        assert all([x.equals_exact(y, 0.000001) for x, y
                    in zip(mpoly_in, mpoly_out)])

        # TODO: tol only passes at 0.011, whereas others pass at much tighter
        # Figure out why
        poly_in = [wkt.loads(x) for x in df_in["poly_"]]
        poly_out = [wkt.loads(x) for x in df_out["poly_"]]
        assert all([x.equals_exact(y, 0.011) for x, y
                    in zip(poly_in, poly_out)])

        con.execute("drop table if exists test_geospatial_no_nulls")
