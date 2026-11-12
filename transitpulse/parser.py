import json


def inspect_document(body):
    value = json.loads(body)
    header = value.get('header')
    entities = value.get('entity', [])
    if not isinstance(header, dict) or header.get('gtfs_realtime_version') != '2.0':
        raise ValueError('只支持 GTFS-Realtime 2.0 JSON 表示')
    mode = header.get('incrementality', 'FULL_DATASET')
    if mode not in {'FULL_DATASET', 'DIFFERENTIAL'}:
        raise ValueError('未知增量模式')
    if not isinstance(entities, list):
        raise ValueError('entity 必须是数组')
    seen, result = set(), []
    for entity in entities:
        identifier = entity.get('id')
        if not identifier or identifier in seen:
            raise ValueError('实体标识缺失或同包重复')
        seen.add(identifier)
        kinds = [key for key in ['trip_update', 'vehicle', 'alert'] if key in entity]
        result.append({'id': identifier, 'is_deleted': entity.get('is_deleted', False), 'kinds': kinds})
    return {'timestamp': header.get('timestamp'), 'incrementality': mode, 'entities': result}
