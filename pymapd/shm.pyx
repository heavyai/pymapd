import pyarrow as pa
from libc.stdint cimport uintptr_t
import struct

# ------------------------
# Shared Memory Wrappers #
# ------------------------
cdef extern from "sys/shm.h":

    int shmget(int key, size_t size, int shmflg)
    void* shmat(int shmid, void *shmaddr, int shmflg)

cpdef load_buffer(bytes handle, int size):

    shmkey = struct.unpack('<L', handle)[0]
    shmid = shmget(shmkey, size, 0)
    if shmid == -1:
        raise ValueError("Invalid shared memory key {}".format(shmkey))
    ptr = shmat(shmid, NULL, 0)    # shared memory segment's start address

    pabuff = pa.foreign_buffer(<uintptr_t>ptr, size, None)

    return pabuff
