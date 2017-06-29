from pymapd.cursor import Cursor


class TestCursor:

    def test_empty_iterable(self):
        c = Cursor(None)
        result = list(c)
        assert result == []

    def test_context_manager(self, mock_connection):
        c = mock_connection.cursor()
        with c:
            c.execute("select 1;")
