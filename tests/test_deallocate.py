import os
import pickle
import pytest
from pymapd import connect
from numba import cuda
from mapd.ttypes import TMapDException, TApplicationException
import pyximport

pyximport.install()
pd = pytest.importorskip("pandas")

HERE = os.path.dirname(__file__)

@pytest.mark.usefixtures("mapd_server")
class TestDeallocate:

    def _data(self):
        df = pd.read_pickle(os.path.join(HERE, "data", "data_load.pkl"))
        return df.itertuples(index=False)

    def _connect(self):

        return connect(user="mapd", password='HyperInteractive', host='localhost',
                       port=9091, protocol='binary', dbname='mapd')

    def _transact(self, con):
        drop = 'drop table if exists iris;'
        con.execute(drop)
        create = ('create table iris (id int, sepal_l float, sepal_w float, '
              'petal_l int, petal_w float, species text);')
        con.execute(create)
        con.load_table('iris',self._data())

    def test_deallocate_ipc(self):
        con = self._connect()
        self._transact(con)
        mem = cuda.current_context().get_memory_info()[0]

        df = con.select_ipc_gpu('select sepal_l, sepal_W, petal_l, petal_w, species from iris')
        con.deallocate_ipc_gpu()
        con.close()

        assert cuda.current_context().get_memory_info()[0] == mem

    def test_deallocate_raises_right_exception(self):
        con = self._connect()
        self._transact(con)

        df = con.select_ipc_gpu('select species from iris')
        con.deallocate_ipc_gpu()

        with pytest.raises(TMapDException) as te:
            con.deallocate_ipc_gpu()
        assert str(te.value.error_msg) == 'Exception: current data frame handle is not bookkept or been inserted twice'

        pdf = con.select_ipc('select sepal_l, sepal_W from iris')
        con.deallocate_ipc()

        with pytest.raises(TApplicationException) as ae:
            con.deallocate_ipc()
        assert str(ae.value.message) == 'failed to get an valid shm ID w/ given shm key of the schema'

    def test_deallocate_session(self):
        con = self._connect()
        con1 = self._connect()
        self._transact(con)

        df = con.select_ipc_gpu('select id from iris')
        with pytest.raises(TMapDException) as te:
            con1.deallocate_ipc_gpu()
        assert str(te.value.error_msg) == 'Exception: current data frame handle is not bookkept or been inserted twice'


