import pytest

from pymapd import OperationalError, connect
from pymapd.cursor import Cursor


class TestConnect:

    def test_host_specified(self):
        with pytest.raises(TypeError):
            connect(user='foo')

    def test_raises_right_exception(self):
        with pytest.raises(OperationalError):
            connect(host='localhost', protocol='binary', port=1234)

    def test_close(self, mock_transport, mock_client):
        con = connect(user='user', password='password',
                      host='localhost', dbname='dbname')
        assert con.closed == 0
        con.close()
        assert con.closed == 1

    def test_connect(self, mock_transport, mock_client):
        con = connect(user='user', password='password',
                      host='localhost', dbname='dbname')
        assert mock_client.call_count == 1
        assert con._client.connect.call_args == [
            ('user', 'password', 'dbname')
        ]

    def test_context_manager(self, mock_transport, mock_client):
        con = connect(user='user', password='password',
                      host='localhost', dbname='dbname')
        with con as cur:
            pass

        assert isinstance(cur, Cursor)
        assert con.closed == 0
