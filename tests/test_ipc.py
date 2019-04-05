import pyarrow as pa
import pandas as pd
import numpy as np  # noqa
import pandas.util.testing as tm  # noqa
from pymapd._parsers import _load_schema, _load_data  # noqa


def make_data_batch():
    np.random.seed(1234)
    depdelay = np.random.randint(-5, 30, size=10, dtype=np.int16)
    arrdelay = np.random.randint(-15, 30, size=10, dtype=np.int16)
    depdelay_ = pa.array(depdelay)
    arrdelay_ = pa.array(arrdelay)
    batch = pa.RecordBatch.from_arrays([depdelay_, arrdelay_],
                                       ['depdelay', 'arrdelay'])
    return (depdelay, arrdelay), batch


(depdelay, arrdelay), batch = make_data_batch()
schema_data = batch.schema.serialize().to_pybytes()
data_data = batch.serialize().to_pybytes()


class TestIPC:
    def test_parse_schema(self):
        buf = pa.py_buffer(schema_data)
        result = _load_schema(buf)
        expected = pa.schema([
            pa.field("depdelay", pa.int16()),
            pa.field("arrdelay", pa.int16())
        ])
        assert result.equals(expected)

    def test_parse_data(self):
        buf = pa.py_buffer(data_data)
        schema = pa.schema([
            pa.field("depdelay", pa.int16()),
            pa.field("arrdelay", pa.int16())
        ])
        result = _load_data(buf, schema)
        expected = pd.DataFrame({
            "depdelay": depdelay,
            "arrdelay": arrdelay,
        })[['depdelay', 'arrdelay']]
        tm.assert_frame_equal(result, expected)
