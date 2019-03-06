import pytest
from mapd.ttypes import TColumnType, TTypeInfo

from pymapd import OperationalError, connect
from pymapd.cursor import Cursor
from pymapd.connection import _parse_uri, ConnectionInfo
from pymapd._parsers import ColumnDetails, _extract_column_details


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

    def test_commit_noop(self, mock_transport, mock_client):
        con = connect(user='user', password='password',
                      host='localhost', dbname='dbname')
        result = con.commit()  # it worked
        assert result is None

    def test_bad_protocol(self, mock_transport, mock_client):
        with pytest.raises(ValueError) as m:
            connect(user='user', host='localhost', dbname='dbname',
                    protocol='fake-proto')
        assert m.match('fake-proto')


class TestURI:

    def test_parse_uri(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:6274/mapd?'
               'protocol=binary')
        result = _parse_uri(uri)
        expected = ConnectionInfo("mapd", "HyperInteractive", "localhost",
                                  6274, "mapd", "binary")
        assert result == expected

    def test_both_raises(self):
        uri = ('mapd://mapd:HyperInteractive@localhost:6274/mapd?'
               'protocol=binary')
        with pytest.raises(TypeError):
            connect(uri=uri, user='my user')


class TestExtras:
    def test_extract_row_details(self):
        data = [
            TColumnType(col_name='date_',
                        col_type=TTypeInfo(type=6, encoding=4, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=32),
                        is_reserved_keyword=False, src_name=''),
            TColumnType(col_name='trans',
                        col_type=TTypeInfo(type=6, encoding=4, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=32),
                        is_reserved_keyword=False, src_name=''),
            TColumnType(col_name='symbol',
                        col_type=TTypeInfo(type=6, encoding=4, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=32),
                        is_reserved_keyword=False, src_name=''),
            TColumnType(col_name='qty',
                        col_type=TTypeInfo(type=1, encoding=0, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=0),
                        is_reserved_keyword=False, src_name=''),
            TColumnType(col_name='price',
                        col_type=TTypeInfo(type=3, encoding=0, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=0),
                        is_reserved_keyword=False, src_name=''),
            TColumnType(col_name='vol',
                        col_type=TTypeInfo(type=3, encoding=0, nullable=True,
                                           is_array=False, precision=0,
                                           scale=0, comp_param=0),
                        is_reserved_keyword=False, src_name='')]
        result = _extract_column_details(data)

        expected = [
            ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT'),
            ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT'),
            ColumnDetails(name='symbol', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32,
                          encoding='DICT'),
            ColumnDetails(name='qty', type='INT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE'),
            ColumnDetails(name='price', type='FLOAT', nullable=True,
                          precision=0, scale=0, comp_param=0, encoding='NONE'),
            ColumnDetails(name='vol', type='FLOAT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE')
        ]
        assert result == expected
