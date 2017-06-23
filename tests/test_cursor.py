from pymapd.cursor import Cursor


class TestCursor:

    def test_empty_iterable(self):
        c = Cursor(None)
        result = list(c)
        assert result == []
