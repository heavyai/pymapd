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
        if arr.dtype == 'int64':
            # The user has passed a unix timestamp already
            return arr
        elif arr.dtype == 'object' or 'datetime64[ns,' in arr.dtype:
            # Convert to datetime64[ns] from string
            # Or from datetime with timezone information
            arr = arr.astype('datetime64[ns]')
        else:
            raise TypeError("Invalid type {0}, expected one of \
                datetime64[ns], int64 (seconds since epoch), \
                or object (string)".format(arr.dtype))
    return arr.view('i8') // 10**9  # ns -> s since epoch


def date_to_seconds(arr):
    import numpy as np
    data = arr.apply(lambda x: np.datetime64(x, "s").astype(int))
    return data


def change_dash_sources(dashdef, tables):
    oldtablename = dashdef['dashboard']['table']
    newtablename = tables.get(oldtablename, {}).get('name', {}) or oldtablename
    dashdef['dashboard']['table'] = newtablename
    for k, v in dashdef['dashboard']['dataSources'].items():
        for col in v['columnMetadata']:
            col['table'] = newtablename
    for chart, val in dashdef['charts'].items():
        if val.get('dataSource', None):
            dashdef['charts'][chart]['dataSource'] = newtablename

        i = 0
        for dim in val.get('dimensions', []):
            if dim.get('table', {}):
                dashdef['charts'][chart]['dimensions'][i]['table'] = newtablename
            if dim.get('selector', {}).get('table'):
                dashdef['charts'][chart]['dimensions'][i]['selector']['table'] = newtablename
            i += 1

        i = 0
        for m in val.get('measures', []):
            if m.get('table', None):
                dashdef['charts'][chart]['measures'][i]['table'] = newtablename
            i += 1

        il = 0
        for layer in val.get('layers', []):
            im = 0
            if layer.get('dataSource'):
                dashdef['charts'][chart]['layers'][il]['dataSource'] = newtablename
            for measure in layer.get('measures', []):
                if measure.get('table', None):
                    dashdef['charts'][chart]['layers'][il]['measures'][im]['table'] = newtablename
                im += 1
            il += 1
    dashdef['dashboard']['dataSources'][newtablename] = dashdef['dashboard']['dataSources'].pop(oldtablename)
    return dashdef


mapd_to_slot = {
    'BOOL': 'int_col',
    'BOOLEAN': 'int_col',
    'SMALLINT': 'int_col',
    'INT': 'int_col',
    'INTEGER': 'int_col',
    'BIGINT': 'int_col',
    'FLOAT': 'real_col',
    'DECIMAL': 'int_col',
    'DOUBLE': 'real_col',
    'TIMESTAMP': 'int_col',
    'DATE': 'int_col',
    'TIME': 'int_col',
    'STR': 'str_col',
    'POINT': 'str_col',
    'LINESTRING': 'str_col',
    'POLYGON': 'str_col',
    'MULTIPOLYGON': 'str_col',
    'TINYINT': 'int_col',
    'GEOMETRY': 'str_col',
    'GEOGRAPHY': 'str_col',
}


mapd_to_na = {
    'BOOL': -128,
    'BOOLEAN': -128,
    'SMALLINT': -32768,
    'INT': -2147483648,
    'INTEGER': -2147483648,
    'BIGINT': -9223372036854775808,
    'FLOAT': 0,
    'DECIMAL': 0,
    'DOUBLE': 0,
    'TIMESTAMP': -9223372036854775808,
    'DATE': -9223372036854775808,
    'TIME': -9223372036854775808,
    'STR': '',
    'POINT': '',
    'LINESTRING': '',
    'POLYGON': '',
    'MULTIPOLYGON': '',
    'TINYINT': -128,
    'GEOMETRY': '',
    'GEOGRAPHY': '',
}
