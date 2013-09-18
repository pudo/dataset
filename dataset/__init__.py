import os
# shut up useless SA warning:
import warnings
warnings.filterwarnings(
    'ignore', 'Unicode type received non-unicode bind param value.')

from dataset.persistence.database import Database
from dataset.persistence.table import Table
from dataset.freeze.app import freeze
from sqlalchemy import Integer, Text

__all__ = ['Database', 'Table', 'freeze', 'connect']


def connect(url=None, schema=None, reflectMetadata=True):
    """
    Opens a new connection to a database. *url* can be any valid `SQLAlchemy engine URL`_.
    If *url* is not defined it will try to use *DATABASE_URL* from environment variable.
    Returns an instance of :py:class:`Database <dataset.Database>`. Set *reflectMetadata* to False if you
    don't want the entire database schema to be pre-loaded. This significantly speeds up
    connecting to large databases with lots of tables.
    ::

        db = dataset.connect('sqlite:///factbook.db')

    .. _SQLAlchemy Engine URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
    """
    if url is None:
        url = os.environ.get('DATABASE_URL', url)
    return Database(url, schema=schema, reflectMetadata=reflectMetadata)
