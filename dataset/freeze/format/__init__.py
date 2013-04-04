from dataset.freeze.format.fjson import JSONSerializer
from dataset.freeze.format.fcsv import CSVSerializer
from dataset.freeze.format.ftabson import TabsonSerializer

SERIALIZERS = {
    'json': JSONSerializer,
    'csv': CSVSerializer,
    'tabson': TabsonSerializer
    }


def get_serializer(config):
    serializer = config.get_normalized('format', 'json')
    return SERIALIZERS.get(serializer)
