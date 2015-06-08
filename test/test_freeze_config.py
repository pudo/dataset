import os
import unittest

from dataset.util import FreezeException


class TestConfiguration(unittest.TestCase):
    def test_init(self):
        from dataset.freeze.config import Configuration

        self.assertRaises(FreezeException, Configuration, 'x.x')
        self.assertRaises(FreezeException, Configuration, __file__)
        cfg = Configuration(os.path.join(os.path.dirname(__file__), 'Freezefile.yaml'))
        assert cfg

    def test_exports(self):
        from dataset.freeze.config import Configuration

        cfg = Configuration(os.path.join(os.path.dirname(__file__), 'Freezefile.yaml'))
        exports = list(cfg.exports)
        self.assertEqual(len(exports), 4)
        self.assertFalse(exports[0].skip)
        self.assertTrue(exports[0].get_bool('bool'))
        self.assertEqual(exports[0].get_int('nan', 'default'), 'default')
        self.assertEqual(exports[0].get_int('number'), 5)
        self.assertTrue(exports[0].name)

    def test_exports_fail(self):
        from dataset.freeze.config import Configuration

        cfg = Configuration(os.path.join(os.path.dirname(__file__), 'Freezefile.yaml'))
        cfg.data = None
        self.assertRaises(FreezeException, list, cfg.exports)
        cfg.data = {}
        self.assertRaises(FreezeException, list, cfg.exports)


if __name__ == '__main__':
    unittest.main()
