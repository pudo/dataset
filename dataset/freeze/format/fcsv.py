import csv
from datetime import datetime

from dataset.freeze.format.common import Serializer


def value_to_str(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, unicode):
        return value.encode('utf-8')
    if value is None:
        return ''
    return value


class CSVSerializer(Serializer):

    def init(self):
        self.handles = {}

    def write(self, path, result):
        keys = result.keys()
        if not path in self.handles:
            fh = open(path, 'wb')
            writer = csv.writer(fh)
            writer.writerow([k.encode('utf-8') for k in keys])
            self.handles[path] = (writer, fh)
        writer, fh = self.handles[path]
        values = [value_to_str(result.get(k)) for k in keys]
        writer.writerow(values)

    def close(self):
        for (writer, fh) in self.handles.values():
            fh.close()


