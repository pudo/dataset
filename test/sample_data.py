# -*- encoding: utf-8 -*-
from datetime import datetime

TEST_CITY_1 = u'B€rkeley'
TEST_CITY_2 = u'G€lway'

TEST_DATA = [
    {
        'date': datetime(2011, 1, 1),
        'temperature': 1,
        'place': TEST_CITY_2
    },
    {
        'date': datetime(2011, 1, 2),
        'temperature': -1,
        'place': TEST_CITY_2
    },
    {
        'date': datetime(2011, 1, 3),
        'temperature': 0,
        'place': TEST_CITY_2
    },
    {
        'date': datetime(2011, 1, 1),
        'temperature': 6,
        'place': TEST_CITY_1
    },
    {
        'date': datetime(2011, 1, 2),
        'temperature': 8,
        'place': TEST_CITY_1
    },
    {
        'date': datetime(2011, 1, 3),
        'temperature': 5,
        'place': TEST_CITY_1
    }
]
