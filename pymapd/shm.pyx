cimport numpy as np
import numpy as np
import pyarrow as pa
from numpy cimport ndarray
from cython cimport view


# ------------------------
# Shared Memory Wrappers #
# ------------------------
cdef extern from "sys/types.h":
    ctypedef int key_t


cdef extern from "sys/shm.h":

    ctypedef unsigned int shmatt_t

    cdef struct shmid_ds:
        shmatt_t shm_nattch

    int shmget(key_t key, size_t size, int shmflg)
    # TODO: deoes shmat return (void *) or (int *)
    void* shmat(int shmid, void *shmaddr, int shmflg)

    int shmctl(int shmid, int cmd, shmid_ds *buf) nogil

cpdef load_buffer(bytes handle, int size):

    shmkey = <unsigned int>ndarray(shape=1, dtype=np.uint32, buffer=handle)[0]
    shmid = shmget(shmkey, size, 0)
    if shmid == -1:
        raise ValueError("Invalid shared memory key {}".format(shmkey))
    ptr = shmat(shmid, NULL, 0)    # shared memory segment's start address
    # TODO: remove this intermediate NumPy step. Should be easy
    npbuff = np.asarray(<np.uint8_t[:size]>ptr, dtype=np.uint8)
    return pa.frombuffer(npbuff.tobytes())
