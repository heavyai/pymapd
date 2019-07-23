import pytest
from omnisci.mapd.ttypes import TColumnType
from omnisci.common.ttypes import TTypeInfo
from pymapd import OperationalError, connect
from pymapd.connection import _parse_uri, ConnectionInfo
from pymapd.exceptions import Error
from pymapd._parsers import ColumnDetails, _extract_column_details


@pytest.mark.usefixtures("mapd_server")
class TestConnect:

    def test_host_specified(self):
        with pytest.raises(TypeError):
            connect(user='foo')

    def test_raises_right_exception(self):
        with pytest.raises(OperationalError):
            connect(host='localhost', protocol='binary', port=1234)

    def test_close(self):
        conn = connect(user='admin', password='HyperInteractive',
                       host='localhost', dbname='omnisci')
        assert conn.closed == 0
        conn.close()
        assert conn.closed == 1

    def test_commit_noop(self, con):
        result = con.commit()  # it worked
        assert result is None

    def test_bad_protocol(self, mock_client):
        with pytest.raises(ValueError) as m:
            connect(user='user', host='localhost', dbname='dbname',
                    protocol='fake-proto')
        assert m.match('fake-proto')

    def test_session_logon_success(self):
        conn = connect(user='admin', password='HyperInteractive',
                       host='localhost', dbname='omnisci')
        sessionid = conn._session
        connnew = connect(sessionid=sessionid, host='localhost')
        assert connnew._session == sessionid

    def test_session_logon_failure(self):
        sessionid = 'ILoveDancingOnTables'
        with pytest.raises(Error):
            connect(sessionid=sessionid, host='localhost')


class TestURI:

    def test_parse_uri(self):
        uri = ('omnisci://admin:HyperInteractive@localhost:6274/omnisci?'
               'protocol=binary')
        result = _parse_uri(uri)
        expected = ConnectionInfo("admin", "HyperInteractive", "localhost",
                                  6274, "omnisci", "binary")
        assert result == expected

    def test_both_raises(self):
        uri = ('omnisci://admin:HyperInteractive@localhost:6274/omnisci?'
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
                          scale=0, comp_param=32, encoding='DICT',
                          is_array=False),
            ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                          scale=0, comp_param=32, encoding='DICT',
                          is_array=False),
            ColumnDetails(name='symbol', type='STR', nullable=True,
                          precision=0, scale=0, comp_param=32,
                          encoding='DICT',
                          is_array=False),
            ColumnDetails(name='qty', type='INT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE',
                          is_array=False),
            ColumnDetails(name='price', type='FLOAT', nullable=True,
                          precision=0, scale=0, comp_param=0, encoding='NONE',
                          is_array=False),
            ColumnDetails(name='vol', type='FLOAT', nullable=True, precision=0,
                          scale=0, comp_param=0, encoding='NONE',
                          is_array=False)
        ]
        assert result == expected
