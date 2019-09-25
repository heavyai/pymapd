import pytest
pytest.importorskip('rbc')


@pytest.mark.usefixtures("mapd_server")
class TestRuntimeUDF:

    def test_udf_incr(self, con):

        @con('int32(int32)', 'double(double)')
        def incr(x):
            return x + 1

        con.execute('drop table if exists test_udf_incr')
        con.execute('create table test_udf_incr (i4 integer, f8 double)')
        con.execute('insert into test_udf_incr values (1, 2.3);')
        con.execute('insert into test_udf_incr values (2, 3.4);')

        result = list(con.execute('select i4, incr(i4) from test_udf_incr'))
        expected = [(1, 2), (2, 3)]
        assert result == expected

        result = list(con.execute('select f8, incr(f8) from test_udf_incr'))
        expected = [(2.3, 3.3), (3.4, 4.4)]
        assert result == expected

        con.execute('drop table if exists test_udf_incr')
