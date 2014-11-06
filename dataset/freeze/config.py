import json
import yaml

from six import text_type, PY3

from dataset.util import FreezeException


TRUISH = ['true', 'yes', '1', 'on']

DECODER = {
    'json': json,
    'yaml': yaml
    }


def merge_overlay(data, overlay):
    out = overlay.copy()
    for k, v in data.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            v = merge_overlay(v, out.get(k))
        out[k] = v
    return out


class Configuration(object):

    def __init__(self, file_name):
        self.file_name = file_name
        extension = file_name.rsplit('.', 1)[-1]
        loader = DECODER.get(extension, json)
        try:
            if loader == json and PY3:  # pragma: no cover
                fh = open(file_name, encoding='utf8')
            else:
                fh = open(file_name, 'rb')
            try:
                self.data = loader.load(fh)
            except ValueError as ve:
                raise FreezeException("Invalid freeze file: %s" % ve)
            fh.close()
        except IOError as ioe:
            raise FreezeException(text_type(ioe))

    @property
    def exports(self):
        if not isinstance(self.data, dict):
            raise FreezeException("The root element of the freeze file needs to be a hash")
        if not isinstance(self.data.get('exports'), list):
            raise FreezeException("The freeze file needs to have a list of exports")
        common = self.data.get('common', {})
        for export in self.data.get('exports'):
            yield Export(common, export)


class Export(object):

    def __init__(self, common, data):
        self.data = merge_overlay(data, common)

    def get(self, name, default=None):
        return self.data.get(name, default)

    def get_normalized(self, name, default=None):
        value = self.get(name, default=default)
        if value not in [None, default]:
            value = text_type(value).lower().strip()
        return value

    def get_bool(self, name, default=False):
        value = self.get_normalized(name)
        if value is None:
            return default
        return value in TRUISH

    def get_int(self, name, default=None):
        value = self.get_normalized(name)
        if value is None:
            return default
        return int(value)

    @property
    def skip(self):
        return self.get_bool('skip')

    @property
    def name(self):
        return self.get('name', self.get('query'))
