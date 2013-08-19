import json
from datetime import datetime
from collections import defaultdict

from dataset.freeze.format.common import Serializer


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()


class JSONSerializer(Serializer):

    def init(self):
        self.buckets = defaultdict(list)

    def write(self, path, result):
        self.buckets[path].append(result)

    def wrap(self, result):
        if self.mode == 'item':
            result = result[0]
        if self.wrap:
            result = {
                'count': self.query.count,
                'results': result
                }
            meta = self.export.get('meta', {})
            if meta is not None:
                result['meta'] = meta
        return result

    def close(self):
        for path, result in self.buckets.items():
            result = self.wrap(result)
            fh = open(path, 'wb')
            data = json.dumps(result,
                    cls=JSONEncoder,
                    indent=self.export.get_int('indent'))
            if self.export.get('callback'):
                data = "%s && %s(%s);" % (self.export.get('callback'),
                                         self.export.get('callback'),
                                         data)
            fh.write(data)
            fh.close()

