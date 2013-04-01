# shut up useless SA warning:
import warnings
warnings.filterwarnings('ignore', 'Unicode type received non-unicode bind param value.')

from dataset.persistence.database import Database
from dataset.persistence.table import Table

def connect(url):
    return Database(url)

