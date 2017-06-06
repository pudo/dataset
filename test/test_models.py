import unittest
from dataset import connect
from dataset.models import Model
from datetime import datetime
from sample_data import TEST_DATA


class Weather(Model):
    _table = 'weather'
    _indexes = ['place', ]
    _fields = [
        ('date', datetime.now()),
        ('temperature', 0),
        ('place', None),
    ]


class ModelsTestCase(unittest.TestCase):
    def setUp(self):
        self.db = connect('sqlite:///:memory:')
        self.tbl = self.db['weather']

        self.data = {
            'date': datetime(2013, 06, 01, 23, 00, 00),
            'temperature': 24,
            'place': 'Madrid'
        }

        for row in TEST_DATA:
            self.tbl.insert(row)

    def test_model_default_values(self):
        item = Weather(_engine=self.db)
        self.assertEqual(
            item.date,
            Weather(_engine=self.db).date
        )
        self.assertEqual(
            item.temperature,
            Weather(_engine=self.db).temperature
        )
        self.assertEqual(
            item.place,
            Weather(_engine=self.db).place
        )

    def test_model_creation_with_kwargs(self):
        item = Weather(_engine=self.db, **self.data)
        for key in self.data.iterkeys():
            self.assertEqual(
                self.data[key],
                getattr(item, key)
            )

    def test_model_creation_and_modify_fields(self):
        item = Weather(_engine=self.db)
        item.date = self.data['date']
        item.temperature = self.data['temperature']
        item.place = self.data['place']
        for key in self.data.iterkeys():
            self.assertEqual(
                self.data[key],
                getattr(item, key)
            )

    def test_model_insert(self):
        item = Weather(_engine=self.db, **self.data)
        item.save()
        self.assertNotEqual(item.id, None)

    def test_model_update_dont_modify_id(self):
        item = Weather(_engine=self.db, **self.data)
        item.save()
        old_id = item.id
        item.temperature = 24
        item.save()
        self.assertEqual(old_id, item.id)

    def test_model_update_stores_correctly(self):
        item = Weather(_engine=self.db, **self.data)
        item.save()
        item_id = item.id
        item.temperature = 24
        item.save()
        item_check = Weather(_engine=self.db).find_one(temperature=24)
        self.assertEqual(item_id, item_check.id)

    def test_model_delete(self):
        item = Weather(_engine=self.db, **self.data)
        item.save()
        item_id = item.id
        item.delete()
        item_check = Weather(_engine=self.db)
        self.assertRaises(Weather.NotFound, item_check.find_one, id=item_id)
        self.assertNotEqual(item.id, item_id)

    def test_model_find(self):
        Weather(_engine=self.db, **self.data).save()
        item_check = Weather(_engine=self.db).find(
            temperature=self.data['temperature']
        )
        self.assertEqual(len(item_check), 1)
        self.assertEqual(item_check[0].temperature, self.data['temperature'])

    def test_model_find_one(self):
        Weather(_engine=self.db, **self.data).save()
        item_check = Weather(_engine=self.db).find_one(
            temperature=self.data['temperature']
        )
        self.assertTrue(isinstance(item_check, Weather))
        self.assertEqual(item_check.temperature, self.data['temperature'])
