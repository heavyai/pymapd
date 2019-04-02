from pymapd.exceptions import (_translate_exception, ProgrammingError,
                               DatabaseError)


class TestExceptions:

    def test_invalid_sql_raises(self, invalid_sql):
        result = _translate_exception(invalid_sql)
        assert isinstance(result, DatabaseError)
        assert "Column 'it' not found in any table" in result.args[0]

    def test_nonexistant_table_raises(self, nonexistant_table):
        result = _translate_exception(nonexistant_table)
        assert isinstance(result, DatabaseError)
        assert "Object 'not_a_table' not found" in result.args[0]
