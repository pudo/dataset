# shut up useless SA warning:
import warnings
warnings.filterwarnings(
    'ignore', 'Unicode type received non-unicode bind param value.')

from dataset.persistence.database import Database
from dataset.persistence.table import Table
from dataset.freeze.app import freeze

def connect(url):
    """
    Opens a new connection to a database. *url* can be any valid `SQLAlchemy engine URL`_. Returns
    an instance of :py:class:`Database <dataset.Database>`.
    ::

        db = dataset.connect('sqlite:///factbook.db')

    .. _SQLAlchemy Engine URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
    """
    return Database(url)


