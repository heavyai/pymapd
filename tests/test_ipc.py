import os
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

import numpy as np  # noqa
import pandas.util.testing as tm  # noqa
from pymapd._parsers import _load_schema, _load_data  # noqa


HERE = os.path.dirname(__file__)


with open(os.path.join(HERE, "data", "schema_buffer.dat"), "rb") as f:
    schema_data = f.read()


with open(os.path.join(HERE, "data", "data_buffer.dat"), "rb") as f:
    data_data = f.read()


class TestIPC:
    def test_parse_schema(self):
        buf = pa.frombuffer(schema_data)
        result = _load_schema(buf)
        expected = pa.schema([
            pa.field("depdelay", pa.int16()),
            pa.field("arrdelay", pa.int16())
        ])
        assert result.equals(expected)

    def test_parse_data(self):
        buf = pa.frombuffer(data_data)
        schema = pa.schema([
            pa.field("depdelay", pa.int16()),
            pa.field("arrdelay", pa.int16())
        ])
        result = _load_data(buf, schema)
        expected = pd.DataFrame({
            "depdelay": np.array([1, 12, 17, -4, 30, 10, 30, 12, 3, -3],
                                 dtype=np.int16),
            "arrdelay": np.array([-2, 4, 29, -12, 11, -2, 10, -3, -11, -13],
                                 dtype=np.int16)
        })[['depdelay', 'arrdelay']]
        tm.assert_frame_equal(result, expected)
