import pytest

from pymapd.cursor import Cursor, _bind_parameters


class TestCursor:

    def test_empty_iterable(self):
        c = Cursor(None)
        result = list(c)
        assert result == []

    def test_context_manager(self, mock_connection):
        c = mock_connection.cursor()
        with c:
            c.execute("select 1;")

    def test_escape_basic(self):
        query = "select * from foo where bar > :baz"
        result = str(_bind_parameters(query, {"baz": 10}))
        expected = 'select * from foo where bar > 10'
        assert result == expected

    def test_escape_malicious(self):
        query = "select * from foo where bar > :baz"
        result = str(_bind_parameters(query, {"baz": '1; drop table foo'}))
        # note the inner quotes
        expected = "select * from foo where bar > '1; drop table foo'"
        assert result == expected

    def test_arraysize(self):
        c = Cursor(None)
        assert c.arraysize == 1
        c.arraysize = 10
        assert c.arraysize == 10

        with pytest.raises(TypeError):
            c.arraysize = 'a'
