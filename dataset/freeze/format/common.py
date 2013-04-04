import os
import logging
import re
import locale

from dataset.util import FreezeException, slug


TMPL_KEY = re.compile("{{([^}]*)}}")

OPERATIONS = {
        'identity': lambda x: x,
        'lower': lambda x: unicode(x).lower(),
        'slug': slug
        }


class Serializer(object):

    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self._paths = []
        self._get_basepath()

    def _get_basepath(self):
        prefix = self.config.get('prefix')
        prefix = os.path.abspath(prefix)
        prefix = os.path.realpath(prefix)
        self._prefix = prefix
        filename = self.config.get('filename')
        if filename is None:
            raise FreezeException("No 'filename' is specified")
        self._basepath = os.path.join(prefix, filename)

    def _tmpl(self, data):
        def repl(m):
            op, key = 'identity', m.group(1)
            if ':' in key:
                op, key = key.split(':', 1)
            return unicode(OPERATIONS.get(op)(data.get(key, '')))
        path = TMPL_KEY.sub(repl, self._basepath)
        enc = locale.getpreferredencoding()
        return os.path.realpath(path.encode(enc, 'replace'))

    def file_name(self, row):
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
        mode = self.config.get_normalized('mode', 'list')
        if mode not in ['list', 'item']:
            raise FreezeException("Invalid mode: %s" % mode)
        return mode

    @property
    def wrap(self):
        return self.config.get_bool('wrap',
                default=self.mode=='list')

    def serialize(self):
        self.init()
        query = self.engine.query()
        for row in query:
            self.write(self.file_name(row), row)
        self.close()


