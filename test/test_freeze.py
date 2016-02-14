# coding: utf8
from __future__ import unicode_literals
import os
from csv import reader
import unittest
from tempfile import mkdtemp
from shutil import rmtree

from six import PY3, text_type, binary_type

from dataset import connect
from dataset.freeze.app import freeze
from dataset.freeze.format.fcsv import value_to_str

from .sample_data import TEST_DATA


class FreezeTestCase(unittest.TestCase):

    def setUp(self):
        self.db = connect('sqlite://')
        self.tbl = self.db['weather']
        for row in TEST_DATA:
            self.tbl.insert(row)
        self.d = mkdtemp()

    def tearDown(self):
        rmtree(self.d, ignore_errors=True)

    def test_freeze(self):
        freeze(self.tbl.all(), format='csv',
               filename=u'wäther.csv'.encode('utf8'), prefix=self.d)
        self.assertTrue(os.path.exists(os.path.join(self.d, u'wäther.csv')))
        freeze(self.tbl.all(), format='csv',
               filename=u'wäther.csv', prefix=self.d)
        self.assertTrue(os.path.exists(os.path.join(self.d, u'wäther.csv')))

    def test_freeze_csv(self):
        freeze(self.tbl.all(), format='csv',
               filename='weather.csv', prefix=self.d)
        path = os.path.join(self.d, 'weather.csv')
        if PY3:
            fh = open(path, 'rt', encoding='utf8', newline='')
        else:
            fh = open(path, 'rU')
        try:
            rows = list(reader(fh))
            keys = rows[0]
            for i, d1 in enumerate(TEST_DATA):
                d2 = dict(zip(keys, rows[i + 1]))
                for k in d1.keys():
                    v2 = d2[k]
                    if not PY3:
                        v2 = v2.decode('utf8')
                    v1 = value_to_str(d1[k])
                    if not isinstance(v1, text_type):
                        if isinstance(v1, binary_type):
                            v1 = text_type(v1, encoding='utf8')
                        else:
                            v1 = '%s' % v1
                    self.assertEqual(v2, v1)
        finally:
            fh.close()

    def test_memory_streams(self):
        if PY3:
            from io import StringIO
        else:
            from io import BytesIO as StringIO

        for fmt in ('csv', 'json', 'tabson'):
            with StringIO() as fd:
                freeze(self.tbl.all(), format=fmt, fileobj=fd)
                self.assertFalse(fd.closed, 'fileobj was closed for format %s' % fmt)
                fd.getvalue()  # should not throw

    def test_freeze_json_no_wrap(self):
        freeze(self.tbl.all(), format='json',
                filename='weather.csv', prefix=self.d, wrap=False)
        path = os.path.join(self.d, 'weather.csv')
        if PY3:
            fh = open(path, 'rt', encoding='utf8', newline='')
        else:
            fh = open(path, 'rU')
        try:
            import json
            data = json.load(fh)
            self.assertIsInstance(data, list,
                'Without wrapping, returned JSON should be a list')
        finally:
            fh.close()

    def test_freeze_json_wrap(self):
        freeze(self.tbl.all(), format='json',
                filename='weather.csv', prefix=self.d, wrap=True)
        path = os.path.join(self.d, 'weather.csv')
        if PY3:
            fh = open(path, 'rt', encoding='utf8', newline='')
        else:
            fh = open(path, 'rU')
        try:
            import json
            data = json.load(fh)
            self.assertIsInstance(data, dict,
                'With wrapping, returned JSON should be a dict')
            self.assertIn('results', data.keys())
            self.assertIn('count', data.keys())
            self.assertIn('meta', data.keys())
        finally:
            fh.close()


class SerializerTestCase(unittest.TestCase):

    def test_serializer(self):
        from dataset.freeze.format.common import Serializer
        from dataset.freeze.config import Export
        from dataset.util import FreezeException

        self.assertRaises(FreezeException, Serializer, {}, {})
        s = Serializer(Export({'filename': 'f'}, {'mode': 'nomode'}), '')
        self.assertRaises(FreezeException, getattr, s, 'wrap')
        s = Serializer(Export({'filename': 'f'}, {}), '')
        s.wrap
        s = Serializer(Export({'filename': '-'}, {}), '')
        self.assertTrue(s.fileobj)

    def test_value_to_str1(self):
        assert '2011-01-01T00:00:00' == value_to_str(TEST_DATA[0]['date']), \
            value_to_str(TEST_DATA[0]['date'])

    def test_value_to_str2(self):
        if PY3:
            assert 'hóla' == value_to_str('\u0068\u00f3\u006c\u0061')
        else:
            assert u'hóla'.encode('utf-8') == value_to_str(u'\u0068\u00f3\u006c\u0061'), \
                [value_to_str(u'\u0068\u00f3\u006c\u0061')]

    def test_value_to_str3(self):
        assert '' == value_to_str(None)

    def test_value_to_str4(self):
        assert [] == value_to_str([])


if __name__ == '__main__':
    unittest.main()
