import logging
import argparse

from sqlalchemy.exc import ProgrammingError
from dataset.util import FreezeException
from dataset.persistence.database import Database
from dataset.freeze.config import Configuration, Export
from dataset.freeze.format import get_serializer


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description='Generate static JSON and CSV extracts from a SQL database.',
    epilog='For further information, please check the documentation.')
parser.add_argument('config', metavar='CONFIG', type=str,
                    help='freeze file cofiguration')


def freeze(database, query, format='csv', filename='freeze.csv',
           prefix='.', meta={}, indent=2, mode='list', wrap=True, **kw):
    """
    Perform a data export of a given SQL statement. This is a very
    flexible exporter, allowing for various output formats, metadata
    assignment, and file name templating to dump each record (or a set
    of records) into individual files.
    """
    if isinstance(database, (str, unicode)):
        database = Database(database)
    kw.update({
        'database': database,
        'query': query,
        'format': format,
        'filename': filename,
        'prefix': prefix,
        'meta': meta,
        'indent': indent,
        'mode': mode,
        'wrap': wrap
    })
    return freeze_export(Export({}, kw))


def freeze_export(export):
    try:
        database = export.get('database')
        query = database.query(export.get('query'))
        serializer_cls = get_serializer(export)
        serializer = serializer_cls(export, query)
        serializer.serialize()
    except ProgrammingError, pe:
        raise FreezeException("Invalid query: %s" % pe)


def main():
    try:
        args = parser.parse_args()
        config = Configuration(args.config)
        for export in config.exports:
            if export.skip:
                log.info("Skipping: %s", export.name)
                continue
            log.info("Running: %s", export.name)
            freeze_export(export)
    except FreezeException, fe:
        log.error(fe)

if __name__ == '__main__':
    main()
