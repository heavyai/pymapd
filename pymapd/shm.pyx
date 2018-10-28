cimport numpy as np
import numpy as np
import pyarrow as pa
from numpy cimport ndarray
from cython cimport view
import pyarrow as pa

# ------------------------
# Shared Memory Wrappers #
# ------------------------
cdef extern from "sys/types.h":
    ctypedef int key_t


cdef extern from "sys/ipc.h":
    int IPC_RMID

cdef extern from "sys/shm.h":

    ctypedef unsigned int shmatt_t

    cdef struct shmid_ds:
        shmatt_t shm_nattch

    int shmget(key_t key, size_t size, int shmflg)
    void* shmat(int shmid, void *shmaddr, int shmflg)
    int shmctl(int shmid, int cmd, shmid_ds *buf) nogil
    int shmdt(const void *shmaddr)


cpdef load_buffer(bytes handle, int size):

    shmkey = <unsigned int>ndarray(shape=1, dtype=np.uint32, buffer=handle)[0]
    shmid = shmget(shmkey, size, 0)
    if shmid == -1:
        raise ValueError("Invalid shared memory key {}".format(shmkey))
    ptr = shmat(shmid, NULL, 0)    # shared memory segment's start address
    # TODO: remove this intermediate NumPy step. Should be easy
    # well, maybe not so easy, since I think this is causing a copy,
    # which is allowing be to detach the shared memory segment immediately
    npbuff = np.asarray(<np.uint8_t[:size]>ptr, dtype=np.uint8)
    pabuff = pa.py_buffer(npbuff.tobytes())

    # release
    # How best to handle failures here?
    rm_status = shmctl(shmid, IPC_RMID, NULL)

    status = shmdt(ptr)
    if status == -1:
        raise TypeError("Could not release shared memory")

    return pabuff
