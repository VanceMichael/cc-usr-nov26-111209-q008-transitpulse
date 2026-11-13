"""GTFS-Realtime 2.0 JSON 子集的规范化与班次绑定逻辑。

这里只处理供应商契约里的子集（TripUpdate / VehiclePosition），
不引入 protobuf，也不做路线规划。规范化结果保留契约语义：
arrival 对象缺失与 arrival.time=0 是两种不同证据，
NO_DATA / SKIPPED / CANCELED 不折算成普通延误。
"""

from .schedule import parse_clock, parse_service_date

FULL_DATASET = 'FULL_DATASET'
DIFFERENTIAL = 'DIFFERENTIAL'
VALID_MODES = (FULL_DATASET, DIFFERENTIAL)
ASPECT_KINDS = ('trip_update', 'vehicle')


def validate_feed(payload):
    """单包结构校验，返回精简的头部与实体摘要，不改变语义。"""
    if not isinstance(payload, dict):
        raise ValueError('feed 必须是 JSON 对象')
    header = payload.get('header')
    if not isinstance(header, dict) or header.get('gtfs_realtime_version') != '2.0':
        raise ValueError('只支持 GTFS-Realtime 2.0 JSON 表示')
    mode = header.get('incrementality', FULL_DATASET)
    if mode not in VALID_MODES:
        raise ValueError('未知增量模式')
    entities = payload.get('entity', [])
    if not isinstance(entities, list):
        raise ValueError('entity 必须是数组')
    seen = set()
    summary = []
    for entity in entities:
        if not isinstance(entity, dict):
            raise ValueError('entity 条目必须是对象')
        identifier = entity.get('id')
        if not identifier or not isinstance(identifier, str):
            raise ValueError('实体标识缺失')
        if identifier in seen:
            raise ValueError(f'实体标识同包重复: {identifier}')
        seen.add(identifier)
        kinds = [kind for kind in ASPECT_KINDS if kind in entity]
        if 'alert' in entity:
            kinds.append('alert')
        summary.append(
            {'id': identifier, 'is_deleted': bool(entity.get('is_deleted', False)), 'kinds': kinds}
        )
    return {
        'header_ts': header.get('timestamp'),
        'incrementality': mode,
        'entities': summary,
    }


def _trip_descriptor(container):
    trip = container.get('trip')
    if not isinstance(trip, dict):
        return None
    trip_id = trip.get('trip_id')
    if not trip_id:
        return None
    descriptor = {
        'trip_id': trip_id,
        'route_id': trip.get('route_id'),
        'direction_id': trip.get('direction_id'),
        'start_date': trip.get('start_date'),
        'start_time': trip.get('start_time'),
        'schedule_relationship': trip.get('schedule_relationship'),
    }
    return descriptor


def _stop_time_updates(trip_update):
    result = []
    for raw in trip_update.get('stop_time_update', []) or []:
        if not isinstance(raw, dict):
            raise ValueError('stop_time_update 条目必须是对象')
        has_arrival = isinstance(raw.get('arrival'), dict)
        has_departure = isinstance(raw.get('departure'), dict)
        entry = {
            'stop_sequence': raw.get('stop_sequence'),
            'stop_id': raw.get('stop_id'),
            'has_arrival': has_arrival,
            'arrival_time': raw['arrival'].get('time') if has_arrival else None,
            'arrival_delay': raw['arrival'].get('delay') if has_arrival else None,
            'has_departure': has_departure,
            'departure_time': raw['departure'].get('time') if has_departure else None,
            'departure_delay': raw['departure'].get('delay') if has_departure else None,
            'schedule_relationship': raw.get('schedule_relationship'),
        }
        result.append(entry)
    return result


def normalize_entity(entity):
    """把单个 FeedEntity 规范化为可存储的各方面（aspect）。

    一个实体可同时携带 trip_update 与 vehicle，两者各自有更新时刻，
    调用方必须分别存储，不能用一个覆盖另一个。
    """
    identifier = entity['id']
    aspects = []
    if isinstance(entity.get('trip_update'), dict):
        tu = entity['trip_update']
        descriptor = _trip_descriptor(tu)
        aspects.append(
            {
                'entity_id': identifier,
                'kind': 'trip_update',
                'descriptor': descriptor,
                'aspect_ts': tu.get('timestamp'),
                'schedule_relationship': descriptor.get('schedule_relationship')
                if descriptor else None,
                'stop_time_updates': _stop_time_updates(tu),
                'raw': tu,
            }
        )
    if isinstance(entity.get('vehicle'), dict):
        vehicle = entity['vehicle']
        descriptor = _trip_descriptor(vehicle)
        position = vehicle.get('position') if isinstance(vehicle.get('position'), dict) else None
        aspects.append(
            {
                'entity_id': identifier,
                'kind': 'vehicle',
                'descriptor': descriptor,
                'aspect_ts': vehicle.get('timestamp'),
                'position': position,
                'current_status': vehicle.get('current_status'),
                'stop_id': vehicle.get('stop_id'),
                'raw': vehicle,
            }
        )
    return aspects


def resolve_binding(descriptor, aspect_ts, header_ts, schedule):
    """按契约把 descriptor 绑定到静态班次，返回 (状态, 候选列表)。

    - 精确给出 start_date：按 descriptor 匹配，start_time 一并比对；
    - 缺 start_date：用来源时区与超 24:00 静态时刻推导覆盖候选；
    - 候选为 0 或多于 1 一律保持未绑定，绝不挑“最近的一班”；
    - descriptor 缺失或没有可用于判定的时刻，同样保持未绑定。
    """
    if schedule is None:
        return 'unbound', [], 'no_schedule'
    if descriptor is None:
        return 'unbound', [], 'no_descriptor'
    trip_id = descriptor['trip_id']
    start_time = descriptor.get('start_time')
    if start_time is not None:
        parse_clock(start_time)  # 校验格式，不合法直接报错，事务回滚
    start_date = descriptor.get('start_date')
    if start_date is not None:
        parse_service_date(start_date)
        candidates = schedule.exact(trip_id, start_date, start_time)
        if len(candidates) == 1:
            return 'bound', candidates, None
        reason = 'no_candidate' if not candidates else 'ambiguous'
        return 'unbound', candidates, reason
    reference_ts = aspect_ts if aspect_ts is not None else header_ts
    if reference_ts is None:
        return 'unbound', [], 'no_timestamp'
    candidates = schedule.covering(trip_id, reference_ts, start_time)
    if len(candidates) == 1:
        return 'bound', candidates, None
    reason = 'no_candidate' if not candidates else 'ambiguous'
    return 'unbound', candidates, reason
