import os
import re
import sys
import locale

from six import binary_type, text_type

from dataset.util import FreezeException
from slugify import slugify


TMPL_KEY = re.compile("{{([^}]*)}}")

OPERATIONS = {
        'identity': lambda x: x,
        'lower': lambda x: text_type(x).lower(),
        'slug': slugify
        }


class Serializer(object):

    def __init__(self, export, query):
        self._encoding = locale.getpreferredencoding()
        self.export = export
        self.query = query
        self._paths = []
        self._get_basepath()

        if export.get('filename') == '-':
            export.data['fileobj'] = sys.stdout
        self.fileobj = export.get('fileobj')

    def _get_basepath(self):
        prefix = self.export.get('prefix', '')
        if isinstance(prefix, binary_type):
            prefix = text_type(prefix, encoding=self._encoding)
        prefix = os.path.abspath(prefix)
        prefix = os.path.realpath(prefix)
        self._prefix = prefix
        filename = self.export.get('filename')
        if isinstance(filename, binary_type):
            filename = text_type(filename, encoding=self._encoding)
        if filename is None:
            raise FreezeException("No 'filename' is specified")
        self._basepath = os.path.join(prefix, filename)

    def _tmpl(self, data):
        def repl(m):
            op, key = 'identity', m.group(1)
            if ':' in key:
                op, key = key.split(':', 1)
            return str(OPERATIONS.get(op)(data.get(key, '')))
        path = TMPL_KEY.sub(repl, self._basepath)
        return os.path.realpath(path)

    def file_name(self, row):
        # signal that there is a fileobj available:
        if self.fileobj is not None:
            return None

        path = self._tmpl(row)
        if path not in self._paths:
            if not path.startswith(self._prefix):
                raise FreezeException("Possible path escape detected.")
            dn = os.path.dirname(path)
            if not os.path.isdir(dn):
                os.makedirs(dn)
            self._paths.append(path)
        return path

    @property
    def mode(self):
        mode = self.export.get_normalized('mode', 'list')
        if mode not in ['list', 'item']:
            raise FreezeException("Invalid mode: %s" % mode)
        return mode

    @property
    def wrap(self):
        return self.export.get_bool('wrap', default=self.mode == 'list')

    def serialize(self):
        self.init()
        transforms = self.export.get('transform', {})
        for row in self.query:

            for field, operation in transforms.items():
                row[field] = OPERATIONS.get(operation)(row.get(field))

            self.write(self.file_name(row), row)

        self.close()
