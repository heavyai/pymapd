import os
import subprocess
import pytest
from pymapd import connect
from omnisci.mapd.ttypes import TMapDException, TApplicationException
from .conftest import no_gpu
import pandas as pd


@pytest.mark.usefixtures("mapd_server")
class TestDeallocate:

    def _data(self):
        HERE = os.path.dirname(__file__)
        df = pd.read_pickle(os.path.join(HERE, "data", "data_load.pkl"))
        return df.itertuples(index=False)

    def _connect(self):

        return connect(user="admin",
                       password='HyperInteractive',
                       host='localhost',
                       port=6274, protocol='binary', dbname='omnisci')

    def _transact(self, con):
        drop = 'drop table if exists iris;'
        con.execute(drop)
        create = '''create table iris (
        id int, sepal_l float, sepal_w float,
        petal_l int, petal_w float, species text);'''
        con.execute(create)
        con.load_table('iris', self._data())

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_deallocate_ipc_gpu(self):
        con = self._connect()
        self._transact(con)
        bmem = int(subprocess.check_output('''nvidia-smi -i 0 \
                                          -q -d MEMORY | \
                                          awk ' FNR == 11 { print $3 }' \
                                          ''', shell=True))
        df = con.select_ipc_gpu('''select sepal_l, sepal_W,
                                petal_l, petal_w, species from iris''')
        con.deallocate_ipc_gpu(df)
        amem = int(subprocess.check_output('''nvidia-smi -i 0 \
                                          -q -d MEMORY | \
                                          awk ' FNR == 11 { print $3 }' \
                                          ''', shell=True))
        con.close()

        assert amem == bmem

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    def test_deallocate_ipc(self):
        con = self._connect()
        self._transact(con)
        bmem = int(subprocess.check_output('''ipcs -m | wc -l''', shell=True))
        df = con.select_ipc('''select sepal_l, sepal_W,
                                petal_l, petal_w, species from iris''')
        con.deallocate_ipc(df)
        amem = int(subprocess.check_output('''ipcs -m | wc -l''', shell=True))
        con.close()

        assert amem == bmem

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_gpu_raises_right_Exception(self):
        con = self._connect()
        self._transact(con)

        df = con.select_ipc_gpu('select species from iris')
        con.deallocate_ipc_gpu(df)

        with pytest.raises(TMapDException) as te:
            con.deallocate_ipc_gpu(df)
        assert 'Exception: current data frame' in str(te.value.error_msg)

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    def test_ipc_raises_right_exception(self):
        con = self._connect()
        self._transact(con)

        df = con.select_ipc('select sepal_l, sepal_W from iris')
        con.deallocate_ipc(df)

        with pytest.raises(TApplicationException) as ae:
            con.deallocate_ipc(df)
        assert 'valid shm ID w/ given shm key' in str(ae.value.message)

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    @pytest.mark.skipif(no_gpu(), reason="No GPU available")
    def test_deallocate_session(self):
        con = self._connect()
        con1 = self._connect()
        self._transact(con)

        df = con.select_ipc_gpu('select id from iris')
        con.close()
        with pytest.raises(TMapDException) as te:
            con1.deallocate_ipc_gpu(df)
        assert 'Exception: current data frame' in str(te.value.error_msg)

    @pytest.mark.skip(reason="deallocate non-functional in recent distros")
    def test_ipc_multiple_df(self):
        con = self._connect()
        self._transact(con)
        bmem = int(subprocess.check_output('''ipcs -m | wc -l''', shell=True))
        df1 = con.select_ipc('''select sepal_l, sepal_W,
                                petal_l, petal_w, species from iris''')
        df2 = con.select_ipc('''select sepal_l, sepal_W,
                                petal_l, petal_w, species from iris''')
        df3 = con.select_ipc('''select sepal_l, sepal_W,
                                petal_l, petal_w, species from iris''')
        con.deallocate_ipc(df1)
        con.deallocate_ipc(df2)
        con.deallocate_ipc(df3)

        amem = int(subprocess.check_output('''ipcs -m | wc -l''', shell=True))
        con.close()

        assert amem == bmem
