import pyarrow as pa
import struct
import ctypes
import platform
from ctypes.util import find_library

# find c in OS, ipc not supported on Windows so return None
libc_so = find_library('c')
if platform.system() == "Windows":
    shmget = None
    shmat = None
    shmdt = None
else:
    libc = ctypes.CDLL(libc_so)

    # wrap IPC functions needed
    shmget = libc.shmget
    shmget.restype = ctypes.c_int
    shmget.argtypes = (ctypes.c_int, ctypes.c_size_t, ctypes.c_int)

    shmat = libc.shmat
    shmat.restype = ctypes.c_void_p
    shmat.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_int)

    shmdt = libc.shmdt
    shmdt.restype = ctypes.c_void_p
    shmdt.argtypes = (ctypes.c_void_p,)


def load_buffer(handle, size):

    if find_library('c') is None:
        if platform.system() == "Windows":
            assert("IPC uses POSIX shared memory, which is not supported \
                   on Windows")
        else:
            # libc should be available by default on linux/darwin systems
            assert("ctypes.find_library('c') did not find libc, which is \
                   required for IPC")

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

    # Return the ptr value as well as the buffer, as its too early here
    # to release shared memory from Python
    return (pabuff, ptr)
