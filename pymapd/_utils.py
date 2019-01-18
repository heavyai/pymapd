import datetime
import json
from base64 import b64decode, b64encode

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


def change_dash_sources(dashboard, remap):
    d = dashboard
    r = remap
    dm = json.loads(d.dashboard_metadata)
    tlst = map(str.strip, dm.get('table', '').split(','))
    tlst = [r[t]['name'] if r.get(t, {}).get('name', {}) else t for t in tlst]
    dm['table'] = ', '.join(tlst)
    ds = json.loads(b64decode(d.dashboard_state).decode())
    for ot, defs in r.items():
        nt = defs.get('name', {}) or ot
        if ds['dashboard']['table'] == ot:
            ds['dashboard']['table'] = nt
        for k, v in ds['dashboard']['dataSources'].items():
            for col in v['columnMetadata']:
                if col['table'] == ot:
                    col['table'] = nt
        for c, val in ds['charts'].items():
            if val.get('dataSource', None):
                if ds['charts'][c]['dataSource'] == ot:
                    ds['charts'][c]['dataSource'] = nt

            i = 0
            for dim in val.get('dimensions', []):
                if dim.get('table', {}) == ot:
                    ds['charts'][c]['dimensions'][i]['table'] = nt
                if dim.get('selector', {}).get('table') == ot:
                    ds['charts'][c]['dimensions'][i]['selector']['table'] = nt
                i += 1

            i = 0
            for m in val.get('measures', []):
                if m.get('table', None) == ot:
                    ds['charts'][c]['measures'][i]['table'] = nt
                i += 1

            il = 0
            for layer in val.get('layers', []):
                im = 0
                if layer.get('dataSource', {}) == ot:
                    ds['charts'][c]['layers'][il]['dataSource'] = nt
                for measure in layer.get('measures', []):
                    if measure.get('table', None) == ot:
                        ds['charts'][c]['layers'][il]['measures'][im]['table'] = nt
                    im += 1
                il += 1
        ds['dashboard']['dataSources'][nt] = ds['dashboard']['dataSources'].pop(ot)
    ds = b64encode(json.dumps(ds).encode()).decode()
    dm = json.dumps(dm)
    d.dashboard_state = ds
    d.dashboard_metadata = dm
    return d


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
