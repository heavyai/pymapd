cimport numpy as np
import numpy as np
import pyarrow as pa
from numpy cimport ndarray
from cython cimport view
import pyarrow as pa
from libc.stdint cimport uintptr_t

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

    pabuff = pa.foreign_buffer(<uintptr_t>ptr, size, None)

    return pabuff

#pa.cuda.foreign_buffer in pyarrow 0.11.0
#cpdef load_buffer_gpu(bytes handle, int size):
#
#    shmkey = <unsigned int>ndarray(shape=1, dtype=np.uint32, buffer=handle)[0]
#    shmid = shmget(shmkey, size, 0)
#    if shmid == -1:
#        raise ValueError("Invalid shared memory key {}".format(shmkey))
#    ptr = shmat(shmid, NULL, 0)    # shared memory segment's start address
#
#    pabuff = pa.cuda.foreign_buffer(<uintptr_t>ptr, size, None)
#
#    return pabuff
