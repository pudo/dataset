import csv
from datetime import datetime

from dataset.freeze.format.common import Serializer


def value_to_str(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, 'encode'):
        return value.encode('utf-8')
    if value is None:
        return ''
    return value


class CSVSerializer(Serializer):

    def init(self):
        self.handles = {}

    def write(self, path, result):
        keys = list(result.keys())
        if path not in self.handles:

            # handle fileobj that has been passed in:
            if path is not None:
                fh = open(path, 'wb')
            else:
                fh = self.fileobj

            writer = csv.writer(fh)
            writer.writerow([k.encode('utf-8') for k in keys])
            self.handles[path] = (writer, fh)
        writer, fh = self.handles[path]
        values = [value_to_str(result.get(k)) for k in keys]
        writer.writerow(values)

    def close(self):
        for writer, fh in self.handles.values():
            fh.close()
