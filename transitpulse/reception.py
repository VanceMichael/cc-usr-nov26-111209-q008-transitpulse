"""实时源接收状态库：把 feed 原文接收事实沉淀为可按运营日查询的状态。

设计要点（对应来源契约）：
- FULL_DATASET 中缺席即失效；DIFFERENTIAL 未携带继续有效；
  显式 is_deleted 是独立证据，三种缺席语义分开记账。
- TripUpdate 与 VehiclePosition 是同一实体的两个 aspect，各自有更新时刻。
- 较旧 header 不推进状态；同 header 时间戳异文记为冲突；同文重放可解释。
- 所有状态变更在单个 SQLite 事务内完成，中断不会留下半截状态；
  失败也写一条 receipt，说明原文已归档但业务未接收。
- 静态表换版时冻结旧版快照后再重绑，单个 aspect 重绑失败不影响整条线。
"""

import json
import sqlite3
from datetime import datetime, timezone

from .realtime import normalize_entity, resolve_binding, validate_feed
from .schedule import StaticSchedule, parse_service_date

STATUS_APPLIED = 'applied'
STATUS_REPLAYED = 'replayed'
STATUS_STALE = 'stale'
STATUS_CONFLICT = 'conflict'
STATUS_REJECTED = 'rejected'
STATUS_FAILED = 'failed'

_SCHEMA = """
create table if not exists source_state (
    source text primary key,
    last_header_ts integer,
    last_digest text,
    active_revision text,
    applied_receipt_id integer
);
create table if not exists static_revisions (
    source text not null,
    version text not null,
    timezone text not null,
    payload text not null,
    loaded_at text not null,
    superseded_at text,
    frozen_feed_header_ts integer,
    frozen_receipt_id integer,
    primary key (source, version)
);
create table if not exists static_trips (
    source text not null,
    version text not null,
    trip_id text not null,
    service_date text not null,
    start_time text not null,
    end_time text not null,
    start_ts integer not null,
    end_ts integer not null
);
create index if not exists static_trip_lookup
    on static_trips(source, version, trip_id);
create table if not exists feed_receipts (
    id integer primary key,
    source text not null,
    received_at text not null,
    header_ts integer,
    incrementality text,
    digest text,
    capture_id integer,
    status text not null,
    detail text not null
);
create index if not exists receipt_source on feed_receipts(source, id);
create table if not exists live_entities (
    source text not null,
    entity_id text not null,
    active integer not null,
    first_receipt_id integer not null,
    latest_receipt_id integer not null,
    primary key (source, entity_id)
);
create table if not exists entity_aspects (
    source text not null,
    entity_id text not null,
    kind text not null,
    active integer not null,
    aspect_ts integer,
    descriptor_json text,
    normalized_json text not null,
    binding_status text not null,
    unbound_reason text,
    bound_trip_id text,
    bound_service_date text,
    bound_start_time text,
    bound_version text,
    first_receipt_id integer not null,
    latest_receipt_id integer not null,
    primary key (source, entity_id, kind)
);
create index if not exists aspect_binding
    on entity_aspects(source, bound_version, bound_service_date);
create table if not exists binding_candidates (
    source text not null,
    entity_id text not null,
    kind text not null,
    version text not null,
    trip_id text not null,
    service_date text not null,
    start_time text not null,
    end_time text not null
);
create index if not exists candidate_lookup
    on binding_candidates(source, version, entity_id, kind);
create table if not exists entity_events (
    id integer primary key,
    source text not null,
    entity_id text not null,
    kind text,
    event text not null,
    receipt_id integer,
    revision text,
    detail text not null,
    created_at text not null
);
create index if not exists event_lookup on entity_events(source, entity_id, id);
create table if not exists revision_snapshots (
    source text not null,
    version text not null,
    entity_id text not null,
    kind text not null,
    active integer not null,
    aspect_ts integer,
    descriptor_json text,
    normalized_json text not null,
    binding_status text not null,
    unbound_reason text,
    bound_trip_id text,
    bound_service_date text,
    bound_start_time text,
    latest_receipt_id integer not null
);
create index if not exists snapshot_lookup
    on revision_snapshots(source, version, bound_service_date);
"""


def _now():
    return datetime.now(timezone.utc).isoformat()


class ReceptionStore:
    def __init__(self, database, source='local'):
        self.source = source
        self.connection = sqlite3.connect(database)
        self.connection.execute('pragma foreign_keys=on')
        self.connection.execute('pragma journal_mode=wal')
        self.connection.executescript(_SCHEMA)
        self.connection.execute(
            'insert or ignore into source_state(source) values(?)', (source,)
        )
        self.connection.commit()

    def close(self):
        self.connection.close()

    # ------------------------------------------------------------------ #
    # 静态表
    # ------------------------------------------------------------------ #
    def load_static(self, body):
        """载入一版静态表；换版时冻结旧版快照并重绑所有有效 aspect。

        校验失败直接抛错，当前状态原样保留。重绑逐 aspect 计算，
        单个 aspect 出错只让它自己变为未绑定，不清空整条线路。
        """
        schedule = StaticSchedule.loads(body)
        existing = self.connection.execute(
            'select version from static_revisions where source=? and version=?',
            (self.source, schedule.version),
        ).fetchone()
        if existing is not None:
            raise ValueError(f'静态表版本已存在，拒绝重复载入: {schedule.version}')
        frozen = self.connection.execute(
            'select active_revision, applied_receipt_id from source_state where source=?',
            (self.source,),
        ).fetchone()
        old_version, applied_receipt_id = frozen
        with self.connection:
            self.connection.execute(
                'insert into static_revisions'
                '(source, version, timezone, payload, loaded_at) values(?,?,?,?,?)',
                (self.source, schedule.version, schedule.timezone_name,
                 body if isinstance(body, str) else body.decode('utf-8'), _now()),
            )
            self.connection.executemany(
                'insert into static_trips'
                '(source, version, trip_id, service_date, start_time, end_time,'
                ' start_ts, end_ts) values(?,?,?,?,?,?,?,?)',
                [
                    (self.source, schedule.version, trip['trip_id'],
                     trip['service_date'], trip['start_time'], trip['end_time'],
                     trip['start_ts'], trip['end_ts'])
                    for trip in schedule.trips
                ],
            )
            frozen_header_ts = None
            if old_version is not None:
                self._freeze_revision(old_version)
                self.connection.execute(
                    'update static_revisions set superseded_at=? where source=? and version=?',
                    (_now(), self.source, old_version),
                )
                frozen_header_ts = self.connection.execute(
                    'select last_header_ts from source_state where source=?',
                    (self.source,),
                ).fetchone()[0]
                self.connection.execute(
                    'update static_revisions set frozen_feed_header_ts=?, frozen_receipt_id=?'
                    ' where source=? and version=?',
                    (frozen_header_ts, applied_receipt_id, self.source, old_version),
                )
            self.connection.execute(
                'update source_state set active_revision=? where source=?',
                (schedule.version, self.source),
            )
            self._rebind_all(schedule)
        return {'version': schedule.version, 'previous_version': old_version}

    def _freeze_revision(self, version):
        """把换版前的有效状态按旧版冻结，旧版结果继续可查。"""
        self.connection.execute(
            'delete from revision_snapshots where source=? and version=?',
            (self.source, version),
        )
        self.connection.execute(
            'insert into revision_snapshots'
            '(source, version, entity_id, kind, active, aspect_ts, descriptor_json,'
            ' normalized_json, binding_status, unbound_reason, bound_trip_id,'
            ' bound_service_date, bound_start_time, latest_receipt_id)'
            ' select source, ?, entity_id, kind, active, aspect_ts, descriptor_json,'
            ' normalized_json, binding_status, unbound_reason, bound_trip_id,'
            ' bound_service_date, bound_start_time, latest_receipt_id'
            ' from entity_aspects where source=?',
            (version, self.source),
        )

    def _rebind_all(self, schedule):
        rows = self.connection.execute(
            'select entity_id, kind, aspect_ts, descriptor_json, normalized_json'
            ' from entity_aspects where source=? and active=1',
            (self.source,),
        ).fetchall()
        header_ts = self.connection.execute(
            'select last_header_ts from source_state where source=?', (self.source,)
        ).fetchone()[0]
        for entity_id, kind, aspect_ts, descriptor_json, normalized_json in rows:
            descriptor = json.loads(descriptor_json) if descriptor_json else None
            try:
                binding_status, candidates, reason = resolve_binding(
                    descriptor, aspect_ts, header_ts, schedule
                )
            except (ValueError, TypeError) as error:
                binding_status, candidates, reason = 'unbound', [], 'rebind_error'
                self._record_event(
                    entity_id, kind, 'rebind_failed',
                    {'error': str(error), 'version': schedule.version},
                    revision=schedule.version,
                )
            self._write_binding(entity_id, kind, schedule.version, binding_status, reason, candidates)
            self._record_event(
                entity_id, kind, 'rebind',
                {'status': binding_status, 'reason': reason,
                 'candidates': len(candidates), 'version': schedule.version},
                revision=schedule.version,
            )

    def _active_schedule(self):
        row = self.connection.execute(
            'select payload from static_revisions where source=? and version='
            '(select active_revision from source_state where source=?)',
            (self.source, self.source),
        ).fetchone()
        return StaticSchedule(json.loads(row[0])) if row else None

    # ------------------------------------------------------------------ #
    # feed 接收
    # ------------------------------------------------------------------ #
    def receive_feed(self, body, digest=None, capture_id=None):
        """接收一份 feed，返回收据。除存储故障外，异常都收敛为 receipt。"""
        received_at = _now()
        try:
            payload = json.loads(body)
            summary = validate_feed(payload)
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            return self._write_receipt(
                None, None, digest, capture_id, STATUS_REJECTED,
                {'error': str(error)}, received_at,
            )
        header_ts = summary['header_ts']
        mode = summary['incrementality']
        state = self.connection.execute(
            'select last_header_ts, last_digest from source_state where source=?',
            (self.source,),
        ).fetchone()
        last_header_ts, last_digest = state
        if digest is not None and last_digest == digest:
            return self._write_receipt(
                header_ts, mode, digest, capture_id, STATUS_REPLAYED,
                {'reason': 'identical_payload', 'last_header_ts': last_header_ts},
                received_at,
            )
        if header_ts is not None and last_header_ts is not None and header_ts < last_header_ts:
            return self._write_receipt(
                header_ts, mode, digest, capture_id, STATUS_STALE,
                {'reason': 'older_header', 'last_header_ts': last_header_ts,
                 'incoming_header_ts': header_ts},
                received_at,
            )
        if (header_ts is not None and last_header_ts is not None
                and header_ts == last_header_ts and digest != last_digest):
            return self._write_receipt(
                header_ts, mode, digest, capture_id, STATUS_CONFLICT,
                {'reason': 'same_header_ts_different_payload',
                 'last_header_ts': last_header_ts},
                received_at,
            )
        try:
            receipt_id = self._apply_feed(
                payload, summary, digest, capture_id, received_at,
            )
        except Exception as error:  # 事务中断也要可解释
            self.connection.rollback()
            return self._write_receipt(
                header_ts, mode, digest, capture_id, STATUS_FAILED,
                {'error': f'{type(error).__name__}: {error}'}, received_at,
            )
        return self.get_receipt(receipt_id)

    def _apply_feed(self, payload, summary, digest, capture_id, received_at):
        header_ts = summary['header_ts']
        mode = summary['incrementality']
        schedule = self._active_schedule()
        note = {'entity_count': len(summary['entities']), 'mode': mode}
        with self.connection:
            receipt_id = self._insert_receipt(
                header_ts, mode, digest, capture_id, STATUS_APPLIED, note, received_at,
            )
            if mode == 'FULL_DATASET':
                self._retire_absent_full(summary, receipt_id)
            for entity in payload.get('entity', []):
                self._apply_entity(entity, receipt_id, header_ts, schedule)
            self.connection.execute(
                'update source_state set last_header_ts=?, last_digest=?,'
                ' applied_receipt_id=? where source=?',
                (header_ts, digest, receipt_id, self.source),
            )
        return receipt_id

    def _retire_absent_full(self, summary, receipt_id):
        """FULL_DATASET 的缺席：包内未出现的有效实体整体失效。"""
        present = {entry['id'] for entry in summary['entities']}
        rows = self.connection.execute(
            'select entity_id, kind from entity_aspects where source=? and active=1',
            (self.source,),
        ).fetchall()
        active_entities = {entity_id for entity_id, _ in rows}
        for entity_id in sorted(active_entities - present):
            self.connection.execute(
                'update live_entities set active=0 where source=? and entity_id=?',
                (self.source, entity_id),
            )
            self.connection.execute(
                'update entity_aspects set active=0 where source=? and entity_id=?',
                (self.source, entity_id),
            )
            self._record_event(
                entity_id, None, 'absent_in_full_dataset',
                {'receipt_id': receipt_id}, receipt_id=receipt_id,
            )

    def _apply_entity(self, entity, receipt_id, header_ts, schedule):
        entity_id = entity['id']
        is_deleted = bool(entity.get('is_deleted', False))
        self.connection.execute(
            'insert into live_entities'
            '(source, entity_id, active, first_receipt_id, latest_receipt_id)'
            ' values(?,?,?,?,?) on conflict(source, entity_id) do update set'
            ' active=case when ? then 0 else 1 end, latest_receipt_id=?',
            (self.source, entity_id, 0 if is_deleted else 1, receipt_id, receipt_id,
             1 if is_deleted else 0, receipt_id),
        )
        if is_deleted:
            # 显式 is_deleted：独立证据，与 FULL_DATASET 缺席分开记账。
            self.connection.execute(
                'update entity_aspects set active=0 where source=? and entity_id=?',
                (self.source, entity_id),
            )
            self._record_event(
                entity_id, None, 'is_deleted', {'receipt_id': receipt_id},
                receipt_id=receipt_id,
            )
            return
        for aspect in normalize_entity(entity):
            self._apply_aspect(aspect, receipt_id, header_ts, schedule)

    def _apply_aspect(self, aspect, receipt_id, header_ts, schedule):
        entity_id, kind = aspect['entity_id'], aspect['kind']
        existing = self.connection.execute(
            'select aspect_ts, active from entity_aspects'
            ' where source=? and entity_id=? and kind=?',
            (self.source, entity_id, kind),
        ).fetchone()
        if existing is not None:
            old_ts, old_active = existing
            # aspect 自己的更新时刻旧于已存值：该 aspect 不推进，另一 aspect 不受影响。
            if (aspect['aspect_ts'] is not None and old_ts is not None
                    and aspect['aspect_ts'] < old_ts):
                self._record_event(
                    entity_id, kind, 'stale_aspect_skipped',
                    {'incoming_ts': aspect['aspect_ts'], 'stored_ts': old_ts,
                     'receipt_id': receipt_id}, receipt_id=receipt_id,
                )
                return
        descriptor = aspect['descriptor']
        binding_status, candidates, reason = resolve_binding(
            descriptor, aspect['aspect_ts'], header_ts, schedule,
        )
        normalized = {k: v for k, v in aspect.items() if k != 'raw'}
        self.connection.execute(
            'insert into entity_aspects'
            '(source, entity_id, kind, active, aspect_ts, descriptor_json,'
            ' normalized_json, binding_status, unbound_reason, bound_trip_id,'
            ' bound_service_date, bound_start_time, bound_version,'
            ' first_receipt_id, latest_receipt_id)'
            ' values(?,?,?,1,?,?,?,?,?,?,?,?,?,?,?)'
            ' on conflict(source, entity_id, kind) do update set active=1,'
            ' aspect_ts=excluded.aspect_ts, descriptor_json=excluded.descriptor_json,'
            ' normalized_json=excluded.normalized_json, binding_status=excluded.binding_status,'
            ' unbound_reason=excluded.unbound_reason, bound_trip_id=excluded.bound_trip_id,'
            ' bound_service_date=excluded.bound_service_date,'
            ' bound_start_time=excluded.bound_start_time, bound_version=excluded.bound_version,'
            ' latest_receipt_id=excluded.latest_receipt_id',
            (self.source, entity_id, kind, aspect['aspect_ts'],
             json.dumps(descriptor, ensure_ascii=False),
             json.dumps(normalized, ensure_ascii=False),
             binding_status, reason,
             candidates[0]['trip_id'] if binding_status == 'bound' else None,
             candidates[0]['service_date'] if binding_status == 'bound' else None,
             candidates[0]['start_time'] if binding_status == 'bound' else None,
             self._active_version(), receipt_id, receipt_id),
        )
        self._write_binding(
            entity_id, kind, self._active_version(), binding_status, reason, candidates,
        )
        self._record_event(
            entity_id, kind, 'aspect_upsert',
            {'binding': binding_status, 'reason': reason,
             'aspect_ts': aspect['aspect_ts'], 'receipt_id': receipt_id},
            receipt_id=receipt_id, revision=self._active_version(),
        )

    def _active_version(self):
        return self.connection.execute(
            'select active_revision from source_state where source=?', (self.source,)
        ).fetchone()[0]

    def _write_binding(self, entity_id, kind, version, binding_status, reason, candidates):
        self.connection.execute(
            'delete from binding_candidates where source=? and entity_id=? and kind=? and version=?',
            (self.source, entity_id, kind, version),
        )
        self.connection.execute(
            'update entity_aspects set binding_status=?, unbound_reason=?,'
            ' bound_trip_id=?, bound_service_date=?, bound_start_time=?, bound_version=?'
            ' where source=? and entity_id=? and kind=?',
            (binding_status, reason,
             candidates[0]['trip_id'] if binding_status == 'bound' else None,
             candidates[0]['service_date'] if binding_status == 'bound' else None,
             candidates[0]['start_time'] if binding_status == 'bound' else None,
             version, self.source, entity_id, kind),
        )
        self.connection.executemany(
            'insert into binding_candidates'
            '(source, entity_id, kind, version, trip_id, service_date,'
            ' start_time, end_time) values(?,?,?,?,?,?,?,?)',
            [
                (self.source, entity_id, kind, version, trip['trip_id'],
                 trip['service_date'], trip['start_time'], trip['end_time'])
                for trip in candidates
            ],
        )

    def _record_event(self, entity_id, kind, event, detail, receipt_id=None, revision=None):
        self.connection.execute(
            'insert into entity_events'
            '(source, entity_id, kind, event, receipt_id, revision, detail, created_at)'
            ' values(?,?,?,?,?,?,?,?)',
            (self.source, entity_id, kind, event, receipt_id, revision,
             json.dumps(detail, ensure_ascii=False, sort_keys=True), _now()),
        )

    # ------------------------------------------------------------------ #
    # 收据
    # ------------------------------------------------------------------ #
    def _insert_receipt(self, header_ts, mode, digest, capture_id, status, detail, received_at):
        cursor = self.connection.execute(
            'insert into feed_receipts'
            '(source, received_at, header_ts, incrementality, digest, capture_id,'
            ' status, detail) values(?,?,?,?,?,?,?,?)',
            (self.source, received_at, header_ts, mode, digest, capture_id,
             status, json.dumps(detail, ensure_ascii=False, sort_keys=True)),
        )
        return cursor.lastrowid

    def _write_receipt(self, header_ts, mode, digest, capture_id, status, detail, received_at):
        with self.connection:
            receipt_id = self._insert_receipt(
                header_ts, mode, digest, capture_id, status, detail, received_at,
            )
        return self.get_receipt(receipt_id)

    def get_receipt(self, receipt_id):
        row = self.connection.execute(
            'select id, source, received_at, header_ts, incrementality, digest,'
            ' capture_id, status, detail from feed_receipts where id=?',
            (receipt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(receipt_id)
        return {
            'id': row[0], 'source': row[1], 'received_at': row[2],
            'header_ts': row[3], 'incrementality': row[4], 'digest': row[5],
            'capture_id': row[6], 'status': row[7], 'detail': json.loads(row[8]),
        }

    def receipts(self):
        rows = self.connection.execute(
            'select id, source, received_at, header_ts, incrementality, digest,'
            ' capture_id, status, detail from feed_receipts order by id'
        ).fetchall()
        return [
            {'id': r[0], 'source': r[1], 'received_at': r[2], 'header_ts': r[3],
             'incrementality': r[4], 'digest': r[5], 'capture_id': r[6],
             'status': r[7], 'detail': json.loads(r[8])}
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # 班次日视图
    # ------------------------------------------------------------------ #
    def day_view(self, local_date, version=None):
        """按当地日历日返回可复查的班次现况，可指定旧版静态表。"""
        local_date = parse_service_date(local_date).strftime('%Y%m%d')
        state = self.connection.execute(
            'select active_revision, applied_receipt_id, last_header_ts from source_state'
            ' where source=?',
            (self.source,),
        ).fetchone()
        active_version, applied_receipt_id, last_header_ts = state
        chosen = version or active_version
        if chosen is None:
            raise ValueError('尚未载入静态表，无法生成班次日视图')
        revision_row = self.connection.execute(
            'select timezone, payload, frozen_feed_header_ts, frozen_receipt_id'
            ' from static_revisions where source=? and version=?',
            (self.source, chosen),
        ).fetchone()
        if revision_row is None:
            raise KeyError(f'未知静态表版本: {chosen}')
        schedule = StaticSchedule(json.loads(revision_row[1]))
        is_current = chosen == active_version
        bound_receipt_id = applied_receipt_id if is_current else revision_row[3]
        bound_header_ts = last_header_ts if is_current else revision_row[2]
        trips = schedule.trips_overlapping_day(local_date)
        trip_rows = []
        for trip in trips:
            trip_rows.append(
                self._view_trip(trip, chosen, is_current, schedule)
            )
        unbound = self._view_unbound(local_date, chosen, is_current, schedule)
        removed = self._view_removed(local_date, chosen, is_current)
        return {
            'local_date': local_date,
            'timezone': schedule.timezone_name,
            'static_version': chosen,
            'is_current_static': is_current,
            'bound_feed': {
                'receipt_id': bound_receipt_id,
                'header_ts': bound_header_ts,
            },
            'trips': trip_rows,
            'unbound_entities': unbound,
            'removed_entities': removed,
        }

    def _view_trip(self, trip, version, is_current, schedule):
        if is_current:
            rows = self.connection.execute(
                'select entity_id, kind, active, aspect_ts, normalized_json,'
                ' binding_status, latest_receipt_id from entity_aspects'
                ' where source=? and bound_version=? and bound_trip_id=?'
                ' and bound_service_date=? and bound_start_time=? order by kind, entity_id',
                (self.source, version, trip['trip_id'], trip['service_date'],
                 trip['start_time']),
            ).fetchall()
        else:
            rows = self.connection.execute(
                'select entity_id, kind, active, aspect_ts, normalized_json,'
                ' binding_status, latest_receipt_id from revision_snapshots'
                ' where source=? and version=? and bound_trip_id=?'
                ' and bound_service_date=? and bound_start_time=? order by kind, entity_id',
                (self.source, version, trip['trip_id'], trip['service_date'],
                 trip['start_time']),
            ).fetchall()
        aspects = [self._aspect_view(row) for row in rows if row[2] == 1]
        return {
            'trip_id': trip['trip_id'],
            'service_date': trip['service_date'],
            'start_time': trip['start_time'],
            'end_time': trip['end_time'],
            'start_ts': trip['start_ts'],
            'end_ts': trip['end_ts'],
            'aspects': aspects,
        }

    def _aspect_view(self, row):
        entity_id, kind, active, aspect_ts, normalized_json, binding_status, receipt_id = row
        return {
            'entity_id': entity_id,
            'kind': kind,
            'aspect_ts': aspect_ts,
            'receipt_id': receipt_id,
            'binding_status': binding_status,
            'normalized': json.loads(normalized_json),
        }

    def _view_unbound(self, local_date, version, is_current, schedule):
        if is_current:
            rows = self.connection.execute(
                'select entity_id, kind, aspect_ts, descriptor_json, normalized_json,'
                ' unbound_reason, latest_receipt_id from entity_aspects'
                ' where source=? and active=1 and binding_status=? order by entity_id, kind',
                (self.source, 'unbound'),
            ).fetchall()
        else:
            rows = self.connection.execute(
                'select entity_id, kind, aspect_ts, descriptor_json, normalized_json,'
                ' unbound_reason, latest_receipt_id from revision_snapshots'
                ' where source=? and version=? and binding_status=? order by entity_id, kind',
                (self.source, version, 'unbound'),
            ).fetchall()
        result = []
        for row in rows:
            entity_id, kind, aspect_ts, descriptor_json, normalized_json, reason, receipt_id = row
            candidates = self._candidates(entity_id, kind, version)
            if not is_current and not candidates:
                # 旧版快照写入时的候选从冻结静态班次重新推导，保证旧版可复查。
                descriptor = json.loads(descriptor_json) if descriptor_json else None
                _, recomputed, _ = resolve_binding(
                    descriptor, aspect_ts, None, schedule,
                )
                candidates = [
                    {
                        'trip_id': c['trip_id'],
                        'service_date': c['service_date'],
                        'start_time': c['start_time'],
                        'end_time': c['end_time'],
                    }
                    for c in recomputed
                ]
            result.append(
                {
                    'entity_id': entity_id,
                    'kind': kind,
                    'aspect_ts': aspect_ts,
                    'receipt_id': receipt_id,
                    'unbound_reason': reason,
                    'descriptor': json.loads(descriptor_json) if descriptor_json else None,
                    'candidates': candidates,
                    'normalized': json.loads(normalized_json),
                }
            )
        return result

    def _candidates(self, entity_id, kind, version):
        rows = self.connection.execute(
            'select trip_id, service_date, start_time, end_time from binding_candidates'
            ' where source=? and version=? and entity_id=? and kind=?'
            ' order by service_date, start_time',
            (self.source, version, entity_id, kind),
        ).fetchall()
        return [
            {'trip_id': r[0], 'service_date': r[1], 'start_time': r[2], 'end_time': r[3]}
            for r in rows
        ]

    def _view_removed(self, local_date, version, is_current):
        """失效实体（FULL 缺席 / 显式删除）及其证据，视图里不允许凭空少车。"""
        if is_current:
            rows = self.connection.execute(
                'select entity_id, kind, aspect_ts, normalized_json, latest_receipt_id'
                ' from entity_aspects where source=? and active=0 order by entity_id, kind',
                (self.source,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                'select entity_id, kind, aspect_ts, normalized_json, latest_receipt_id'
                ' from revision_snapshots where source=? and version=? and active=0'
                ' order by entity_id, kind',
                (self.source, version),
            ).fetchall()
        removed = []
        for entity_id, kind, aspect_ts, normalized_json, receipt_id in rows:
            event_row = self.connection.execute(
                'select event, receipt_id, detail from entity_events'
                ' where source=? and entity_id=? and kind is ?'
                ' and event in (?, ?) order by id desc limit 1',
                (self.source, entity_id, kind, 'is_deleted', 'absent_in_full_dataset'),
            ).fetchone()
            if event_row is None:
                event_row = self.connection.execute(
                    'select event, receipt_id, detail from entity_events'
                    ' where source=? and entity_id=? and event in (?, ?)'
                    ' order by id desc limit 1',
                    (self.source, entity_id, 'is_deleted', 'absent_in_full_dataset'),
                ).fetchone()
            removed.append(
                {
                    'entity_id': entity_id,
                    'kind': kind,
                    'aspect_ts': aspect_ts,
                    'last_receipt_id': receipt_id,
                    'removal_event': event_row[0] if event_row else None,
                    'removal_receipt_id': event_row[1] if event_row else None,
                    'removal_detail': json.loads(event_row[2]) if event_row else None,
                }
            )
        return removed

    def entity_events(self, entity_id):
        rows = self.connection.execute(
            'select id, kind, event, receipt_id, revision, detail, created_at'
            ' from entity_events where source=? and entity_id=? order by id',
            (self.source, entity_id),
        ).fetchall()
        return [
            {'id': r[0], 'kind': r[1], 'event': r[2], 'receipt_id': r[3],
             'revision': r[4], 'detail': json.loads(r[5]), 'created_at': r[6]}
            for r in rows
        ]

    def static_versions(self):
        rows = self.connection.execute(
            'select version, timezone, loaded_at, superseded_at, frozen_receipt_id'
            ' from static_revisions where source=? order by loaded_at',
            (self.source,),
        ).fetchall()
        active = self._active_version()
        return [
            {'version': r[0], 'timezone': r[1], 'loaded_at': r[2],
             'superseded_at': r[3], 'frozen_receipt_id': r[4],
             'is_active': r[0] == active}
            for r in rows
        ]
