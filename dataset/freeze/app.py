import logging
import argparse

from dataset.util import FreezeException
from dataset.freeze.config import Configuration
from dataset.freeze.engine import ExportEngine
from dataset.freeze.format import get_serializer


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
        description='Generate static JSON and CSV extracts from a SQL database.',
        epilog='For further information, please check the documentation.')
parser.add_argument('config', metavar='CONFIG', type=str,
                   help='freeze file cofiguration')

def main():
    try: 
        args = parser.parse_args()
        config = Configuration(args.config)
        for export in config.exports:
            if export.skip:
                log.info("Skipping: %s", export.name)
                continue
            log.info("Running: %s", export.name)
            engine = ExportEngine(export)
            query = engine.query()
            serializer_cls = get_serializer(export)
            serializer = serializer_cls(engine)
            serializer.serialize()
    except FreezeException, fe:
        log.error(fe)

if __name__ == '__main__':
    main()
