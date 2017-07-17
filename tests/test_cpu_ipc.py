from pymapd import connection
import numpy as np


class TestCpuIpc:
    def test_parse_schema_buffer(self):
        data = b'\x08\x01\x00\x00\x10\x00\x00\x00\x0c\x00\x0e\x00\x06\x00\x05\x00\x08\x00\x00\x00\x0c\x00\x00\x00\x00\x01\x02\x00\x10\x00\x00\x00\x00\x00\n\x00\x08\x00\x00\x00\x04\x00\x00\x00\n\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00p\x00\x00\x00\x04\x00\x00\x00\xac\xff\xff\xff\x00\x00\x01\x02<\x00\x00\x00\x1c\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00 \x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x98\xff\xff\xff\x00\x00\x00\x01\x10\x00\x00\x00\x88\xff\xff\xff\x10\x00\x01\x00\x90\xff\xff\xff\x01\x00\x02\x00\x08\x00\x00\x00arrdelay\x00\x00\x00\x00\x14\x00\x18\x00\x08\x00\x06\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x14\x00\x00\x00\x14\x00\x00\x00\x00\x00\x01\x02L\x00\x00\x00$\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x000\x00\x00\x00\x1c\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x10\x00\x00\x00\xf8\xff\xff\xff\x10\x00\x01\x00\x08\x00\x08\x00\x04\x00\x06\x00\x08\x00\x00\x00\x01\x00\x02\x00\x08\x00\x00\x00depdelay\x00\x00\x00\x00\x00\x00\x00\x00'  # noqa
        buffer_ = np.array(list(data), dtype=np.uint8)
        result = connection._load_schema(buffer_)
        expected = {
            'batches': [],
            'schema': {
                'fields': [{
                    'children': [],
                    'name': 'depdelay',
                    'nullable': True,
                    'type': {
                        'bitWidth': 16,
                        'isSigned': True,
                        'name': 'int'
                    },
                    'typeLayout': {
                        'vectors': [{
                            'type': 'VALIDITY',
                            'typeBitWidth': 1
                        }, {
                            'type': 'DATA',
                            'typeBitWidth': 16
                        }]
                    }
                }, {
                    'children': [],
                    'name': 'arrdelay',
                    'nullable': True,
                    'type': {
                        'bitWidth': 16,
                        'isSigned': True,
                        'name': 'int'
                    },
                    'typeLayout': {
                        'vectors': [{
                            'type': 'VALIDITY',
                            'typeBitWidth': 1
                        }, {
                            'type': 'DATA',
                            'typeBitWidth': 16
                        }]
                    }
                }]
            }
        }
        assert result == expected
