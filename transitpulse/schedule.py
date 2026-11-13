"""静态班次表模型：服务日、超过 24:00 的时刻与候选班次推导。

静态表是供应商已经展开到具体服务日的简化 JSON，不是完整 GTFS zip。
一个班次的绝对时间区间按来源时区的“服务日当地午夜 + 时长”解释，
因此 25:10:00 与次日 01:10:00 可能落在同一段绝对时间上。
"""

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# python:3.11-slim 镜像默认不带 tzdata，对演练用到的时区提供固定偏移回退。
_FALLBACK_OFFSETS = {
    'Asia/Shanghai': 8,
    'Asia/Urumqi': 6,
    'UTC': 0,
}


def load_timezone(name):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        if name not in _FALLBACK_OFFSETS:
            raise ValueError(f'未知时区且无内置回退: {name}')
        return timezone(timedelta(hours=_FALLBACK_OFFSETS[name]))


def parse_clock(value):
    """把 HH:MM:SS 解析为当日时长秒，小时允许超过 24。"""
    if not isinstance(value, str):
        raise ValueError('时刻必须是 HH:MM:SS 字符串')
    parts = value.split(':')
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise ValueError(f'时刻格式应为 HH:MM:SS: {value!r}')
    hours, minutes, seconds = (int(part) for part in parts)
    if minutes >= 60 or seconds >= 60:
        raise ValueError(f'时刻的分秒必须小于 60: {value!r}')
    return hours * 3600 + minutes * 60 + seconds


def parse_service_date(value):
    """把 YYYYMMDD 解析为 date 对象。"""
    if not isinstance(value, str) or len(value) != 8 or not value.isdigit():
        raise ValueError(f'服务日必须是 YYYYMMDD: {value!r}')
    return datetime.strptime(value, '%Y%m%d').date()


def service_day_bounds(service_date, tzinfo):
    """返回服务日当地午夜（含）与次日午夜（不含）的 UTC Unix 秒。"""
    midnight = datetime(
        service_date.year, service_date.month, service_date.day, tzinfo=tzinfo
    )
    start = int(midnight.timestamp())
    return start, start + 24 * 3600


class StaticSchedule:
    """一版静态班次表的内存表示，负责实体到班次的候选推导。"""

    def __init__(self, payload):
        if not isinstance(payload, dict):
            raise ValueError('静态表必须是 JSON 对象')
        version = payload.get('version')
        if not version or not isinstance(version, str):
            raise ValueError('静态表缺少 version')
        tz_name = payload.get('timezone')
        if not tz_name:
            raise ValueError('静态表缺少 timezone')
        trips = payload.get('trips')
        if not isinstance(trips, list) or not trips:
            raise ValueError('静态表 trips 必须是非空数组')
        self.version = version
        self.timezone_name = tz_name
        self.timezone = load_timezone(tz_name)
        self.trips = []
        seen = set()
        for raw in trips:
            trip = self._normalize_trip(raw)
            key = (trip['trip_id'], trip['service_date'], trip['start_time'])
            if key in seen:
                raise ValueError(f'静态表班次重复: {key}')
            seen.add(key)
            self.trips.append(trip)

    @classmethod
    def loads(cls, body):
        if isinstance(body, (bytes, bytearray)):
            body = body.decode('utf-8')
        return cls(json.loads(body))

    def _normalize_trip(self, raw):
        for field in ('trip_id', 'service_date', 'start_time', 'end_time'):
            if not raw.get(field):
                raise ValueError(f'静态班次缺少字段 {field}: {raw!r}')
        day = parse_service_date(raw['service_date'])
        start_offset = parse_clock(raw['start_time'])
        end_offset = parse_clock(raw['end_time'])
        if end_offset <= start_offset:
            raise ValueError(
                f"班次 {raw['trip_id']} 结束时刻必须晚于开始时刻: {raw}"
            )
        midnight = datetime(day.year, day.month, day.day, tzinfo=self.timezone)
        start_ts = int((midnight + timedelta(seconds=start_offset)).timestamp())
        end_ts = int((midnight + timedelta(seconds=end_offset)).timestamp())
        return {
            'trip_id': raw['trip_id'],
            'service_date': raw['service_date'],
            'start_time': raw['start_time'],
            'end_time': raw['end_time'],
            'start_ts': start_ts,
            'end_ts': end_ts,
        }

    def exact(self, trip_id, service_date, start_time=None):
        """descriptor 给出 start_date 时的精确匹配，多个候选同样不挑一个。"""
        rows = [
            trip
            for trip in self.trips
            if trip['trip_id'] == trip_id and trip['service_date'] == service_date
        ]
        if start_time is not None:
            rows = [trip for trip in rows if trip['start_time'] == start_time]
        return rows

    def covering(self, trip_id, entity_ts, start_time=None):
        """缺 start_date 时的候选：同 trip_id 且区间覆盖实体时刻的班次。

        区间按 [start_ts, end_ts) 判定；descriptor 若带 start_time，
        还须与候选班次的服务日内时长一致。候选数为 0 或多于 1 都保持未绑定。
        """
        if entity_ts is None:
            return []
        rows = [
            trip
            for trip in self.trips
            if trip['trip_id'] == trip_id
            and trip['start_ts'] <= entity_ts < trip['end_ts']
        ]
        if start_time is not None:
            rows = [trip for trip in rows if trip['start_time'] == start_time]
        return rows

    def day_window(self, local_date):
        """某个当地日历日 [午夜, 次日午夜) 的 UTC Unix 秒窗口。"""
        day = parse_service_date(local_date)
        return service_day_bounds(day, self.timezone)

    def trips_overlapping_day(self, local_date):
        """与当地日历日有时间交集的班次，含前一服务日跨午夜的班次。"""
        window_start, window_end = self.day_window(local_date)
        return [
            trip
            for trip in self.trips
            if trip['start_ts'] < window_end and trip['end_ts'] > window_start
        ]
