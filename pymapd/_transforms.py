import json
from base64 import b64decode, b64encode

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
