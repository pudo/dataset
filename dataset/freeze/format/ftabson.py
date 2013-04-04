from dataset.freeze.format.fjson import JSONSerializer


class TabsonSerializer(JSONSerializer):

    def wrap(self, result):
        fields = []
        data = []
        if len(result):
            keys = result[0].keys()
            fields = [{'id': k} for k in keys]
            for row in result:
                d = [row.get(k) for k in keys]
                data.append(d)
        result = {
            'count': len(result),
            'fields': fields,
            'data': data
            }
        meta = self.config.get('meta')
        if meta is not None:
            result['meta'] = meta
        return result

