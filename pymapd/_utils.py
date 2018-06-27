import datetime


def seconds_to_time(seconds):
    """Convert seconds since midnight to a datetime.time"""
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return datetime.time(h, m, s)


def time_to_seconds(time):
    """Convert a datetime.time to seconds since midnight"""
    if time is None:
        return None
    return 3600 * time.hour + 60 * time.minute + time.second


def datetime_to_seconds(arr):
    """Convert an array of datetime64[ns] to seconds since the UNIX epoch"""
    import numpy as np

    if arr.dtype != np.dtype('datetime64[ns]'):
        raise TypeError("Invalid type {}, expected datetime64[ns]".foramt(
            arr.dtype))
    return arr.view('i8') // 10**9  # ns -> s since epoch


def date_to_seconds(arr):
    data = (
        (arr.astype('datetime64[ns]') - datetime.datetime(1970, 1, 1))
        .dt.total_seconds()
    )
    return data


mapd_to_slot = {
    'BOOL': 'int_col',
    'BOOLEAN': 'int_col',
    'SMALLINT': 'int_col',
    'INT': 'int_col',
    'INTEGER': 'int_col',
    'BIGINT': 'int_col',
    'FLOAT': 'real_col',
    'DOUBLE': 'real_col',
    'TIMESTAMP': 'int_col',
    'DATE': 'int_col',
    'TIME': 'int_col',
    'STR': 'str_col',
}


mapd_to_na = {
    'BOOL': 0,
    'BOOLEAN': 0,
    'SMALLINT': -32768,
    'INT': -2147483648,
    'INTEGER': -2147483648,
    'BIGINT': -9223372036854775808,
    'FLOAT': float('nan'),
    'DOUBLE': float('nan'),
    'TIMESTAMP': -9223372036854775808,
    'DATE': -9223372036854775808,
    'TIME': -9223372036854775808,
    'STR': '',
}
