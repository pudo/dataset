from __future__ import unicode_literals
import csv
from datetime import datetime, date

from six import PY3, text_type

from dataset.freeze.format.common import Serializer


def value_to_str(value):
    if isinstance(value, (date, datetime)):
        return text_type(value.isoformat())
    if not PY3 and hasattr(value, 'encode'):
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
                if PY3:  # pragma: no cover
                    fh = open(path, 'wt', encoding='utf8', newline='')
                else:
                    fh = open(path, 'wb')
            else:
                fh = self.fileobj

            writer = csv.writer(fh)
            if PY3:  # pragma: no cover
                writer.writerow(keys)
            else:
                writer.writerow([value_to_str(k) for k in keys])
            self.handles[path] = (writer, fh)
        writer, fh = self.handles[path]
        values = [value_to_str(result.get(k)) for k in keys]
        writer.writerow(values)

    def close(self):
        for writer, fh in self.handles.values():
            if fh != self.fileobj:
                fh.close()
