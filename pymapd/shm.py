import pyarrow as pa
import struct
import ctypes
from ctypes.util import find_library

# find c in OS
libc_so = find_library('c')
libc = ctypes.CDLL(libc_so)

# wrap IPC functions needed
shmget = libc.shmget
shmget.restype = ctypes.c_int
shmget.argtypes = (ctypes.c_int, ctypes.c_size_t, ctypes.c_int)

shmat = libc.shmat
shmat.restype = ctypes.c_void_p
shmat.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_int)


def load_buffer(handle, size):

    # OmniSci passes struct as bytes, convert to int
    shmkey = struct.unpack('<L', handle)[0]

    # Take key from OmniSci, get identifier of shared memory segment
    # If shmid is -1, an error has occured
    shmid = shmget(shmkey, size, 0)
    if shmid == -1:
        raise ValueError("Invalid shared memory key {}".format(shmkey))

    # With id of shared memory segment, attach to Python process
    # None lets system choose suitable unused address
    ptr = shmat(shmid, None, 0)

    # With ptr as shared memory segment's start address, make arrow buffer
    pabuff = pa.foreign_buffer(ptr, size, None)

    return pabuff