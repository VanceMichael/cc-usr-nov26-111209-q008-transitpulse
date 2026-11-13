import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from transitpulse.archive import Archive
from transitpulse.reception import ReceptionStore

FIXTURES = Path('transitpulse/fixtures')


class ReceptionTestBase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.archive = Archive(root / 'captures.sqlite3', root / 'objects')
        self.store = ReceptionStore(root / 'reception.sqlite3')
        self.store.load_static((FIXTURES / 'static-v1.json').read_bytes())

    def tearDown(self):
        self.store.close()
        self.archive.close()
        self.directory.cleanup()

    def ingest_fixture(self, name):
        body = (FIXTURES / name).read_bytes()
        capture_id = self.archive.save('local:' + name, 200, 'application/json', body)
        return self.store.receive_feed(
            body, digest=hashlib.sha256(body).hexdigest(), capture_id=capture_id
        )

    def ingest_json(self, payload):
        body = json.dumps(payload).encode()
        return self.store.receive_feed(body, digest=hashlib.sha256(body).hexdigest())


class IncrementalityTests(ReceptionTestBase):
    def test_differential_absence_keeps_entities(self):
        self.ingest_fixture('feed.json')
        empty_diff = {
            'header': {'gtfs_realtime_version': '2.0', 'incrementality': 'DIFFERENTIAL',
                       'timestamp': 1794420005},
            'entity': [],
        }
        receipt = self.ingest_json(empty_diff)
        self.assertEqual(receipt['status'], 'applied')
        view = self.store.day_view('20261112')
        kinds = {a['kind'] for t in view['trips'] for a in t['aspects']}
        self.assertEqual(kinds, {'trip_update', 'vehicle'})

    def test_full_dataset_absence_retires_entities(self):
        self.ingest_fixture('feed.json')
        empty_full = {
            'header': {'gtfs_realtime_version': '2.0', 'incrementality': 'FULL_DATASET',
                       'timestamp': 1794420006},
            'entity': [],
        }
        self.ingest_json(empty_full)
        view = self.store.day_view('20261112')
        self.assertEqual([a for t in view['trips'] for a in t['aspects']], [])
        removed_ids = {r['entity_id'] for r in view['removed_entities']}
        self.assertEqual(removed_ids, {'trip1', 'vehicle1'})
        self.assertTrue(all(r['removal_event'] == 'absent_in_full_dataset'
                            for r in view['removed_entities']))

    def test_vehicle_only_differential_keeps_trip_update(self):
        self.ingest_fixture('feed.json')
        self.ingest_fixture('difference.json')
        view = self.store.day_view('20261112')
        aspects = [a for t in view['trips'] for a in t['aspects']]
        trip_update = next(a for a in aspects if a['kind'] == 'trip_update')
        vehicle = next(a for a in aspects if a['kind'] == 'vehicle')
        # 两个 aspect 各自的更新时刻，差分包只推进 vehicle。
        self.assertEqual(trip_update['aspect_ts'], 1794420000)
        self.assertEqual(vehicle['aspect_ts'], 1794420010)

    def test_stale_aspect_does_not_overwrite_newer_one(self):
        self.ingest_fixture('feed.json')
        self.ingest_fixture('difference.json')
        older = {
            'header': {'gtfs_realtime_version': '2.0', 'incrementality': 'DIFFERENTIAL',
                       'timestamp': 1794420015},
            'entity': [{
                'id': 'vehicle1',
                'vehicle': {'trip': {'trip_id': 'N1', 'start_date': '20261111',
                                     'start_time': '25:10:00'},
                            'timestamp': 1794420005},
            }],
        }
        self.ingest_json(older)
        view = self.store.day_view('20261112')
        vehicle = next(a for t in view['trips'] for a in t['aspects']
                       if a['kind'] == 'vehicle')
        self.assertEqual(vehicle['aspect_ts'], 1794420010)
        events = self.store.entity_events('vehicle1')
        self.assertIn('stale_aspect_skipped', [e['event'] for e in events])

    def test_explicit_delete_is_separate_evidence(self):
        self.ingest_fixture('feed.json')
        self.ingest_fixture('deleted.json')
        view = self.store.day_view('20261112')
        trip_removal = next(r for r in view['removed_entities'] if r['entity_id'] == 'trip1')
        self.assertEqual(trip_removal['removal_event'], 'is_deleted')
        # vehicle1 没有被删除操作波及。
        active = [a for t in view['trips'] for a in t['aspects']]
        self.assertEqual([a['entity_id'] for a in active], ['vehicle1'])


class ReceiptTests(ReceptionTestBase):
    def test_replay_stale_conflict_and_rejection(self):
        first = self.ingest_fixture('feed.json')
        self.assertEqual(first['status'], 'applied')

        replay = self.ingest_fixture('feed.json')
        self.assertEqual(replay['status'], 'replayed')

        stale = self.ingest_json({
            'header': {'gtfs_realtime_version': '2.0', 'timestamp': 1794419999},
            'entity': [],
        })
        self.assertEqual(stale['status'], 'stale')

        conflict = self.ingest_json({
            'header': {'gtfs_realtime_version': '2.0', 'timestamp': 1794420000},
            'entity': [{'id': 'other'}],
        })
        self.assertEqual(conflict['status'], 'conflict')

        rejected = self.store.receive_feed(b'{not json', digest='bad')
        self.assertEqual(rejected['status'], 'rejected')
        statuses = [r['status'] for r in self.store.receipts()]
        self.assertEqual(statuses, ['applied', 'replayed', 'stale', 'conflict', 'rejected'])

    def test_transaction_failure_leaves_state_intact(self):
        self.ingest_fixture('feed.json')
        broken = {
            'header': {'gtfs_realtime_version': '2.0', 'incrementality': 'DIFFERENTIAL',
                       'timestamp': 1794420009},
            'entity': [{
                'id': 'broken',
                'vehicle': {'trip': {'trip_id': 'N1', 'start_time': '99:99:99'},
                            'timestamp': 1794420009},
            }],
        }
        receipt = self.ingest_json(broken)
        self.assertEqual(receipt['status'], 'failed')
        self.assertIn('error', receipt['detail'])
        view = self.store.day_view('20261112')
        ids = {a['entity_id'] for t in view['trips'] for a in t['aspects']}
        self.assertEqual(ids, {'trip1', 'vehicle1'})

    def test_duplicate_static_version_rejected(self):
        with self.assertRaises(ValueError):
            self.store.load_static((FIXTURES / 'static-v1.json').read_bytes())


class RebindTests(ReceptionTestBase):
    def test_rebind_failure_for_one_aspect_does_not_wipe_line(self):
        self.ingest_fixture('feed.json')
        # 手工植入一个描述符不合法的有效 aspect，模拟单个实体重绑失败。
        self.store.connection.execute(
            "insert into entity_aspects"
            "(source, entity_id, kind, active, aspect_ts, descriptor_json,"
            " normalized_json, binding_status, unbound_reason, first_receipt_id,"
            " latest_receipt_id) values('local','broken','vehicle',1,1794420008,"
            " '{\"trip_id\":\"N1\",\"start_time\":\"99:99:99\"}', '{}',"
            " 'unbound','no_candidate',1,1)"
        )
        result = self.store.load_static((FIXTURES / 'static-v2.json').read_bytes())
        self.assertEqual(result['previous_version'], 'static1')
        view = self.store.day_view('20261112')
        bound = {a['entity_id'] for t in view['trips'] for a in t['aspects']}
        self.assertEqual(bound, {'trip1', 'vehicle1'})
        broken = next(u for u in view['unbound_entities'] if u['entity_id'] == 'broken')
        self.assertEqual(broken['unbound_reason'], 'rebind_error')

    def test_old_static_remains_queryable_after_switch(self):
        self.ingest_fixture('feed.json')
        self.ingest_fixture('difference.json')
        self.store.load_static((FIXTURES / 'static-v2.json').read_bytes())
        old_view = self.store.day_view('20261112', version='static1')
        self.assertFalse(old_view['is_current_static'])
        self.assertEqual(old_view['static_version'], 'static1')
        ids = {a['entity_id'] for t in old_view['trips'] for a in t['aspects']}
        self.assertEqual(ids, {'trip1', 'vehicle1'})
        versions = self.store.static_versions()
        self.assertEqual([v['is_active'] for v in versions], [False, True])


class DayViewTests(ReceptionTestBase):
    def test_midnight_trips_listed_side_by_side(self):
        self.ingest_fixture('feed.json')
        view = self.store.day_view('20261112')
        keys = [(t['service_date'], t['start_time']) for t in view['trips']]
        self.assertEqual(keys, [
            ('20261111', '25:10:00'),
            ('20261112', '01:10:00'),
        ])
        self.assertIsNotNone(view['bound_feed']['receipt_id'])
        self.assertEqual(view['bound_feed']['header_ts'], 1794420000)

    def test_arrival_time_zero_and_no_data_contract_survives(self):
        self.ingest_fixture('contract.json')
        view = self.store.day_view('20261112')
        aspect = next(a for t in view['trips'] for a in t['aspects']
                      if a['entity_id'] == 'trip-canceled')
        stops = aspect['normalized']['stop_time_updates']
        self.assertTrue(stops[0]['has_arrival'])
        self.assertEqual(stops[0]['arrival_time'], 0)
        self.assertFalse(stops[1]['has_arrival'])
        self.assertEqual(stops[1]['schedule_relationship'], 'NO_DATA')
        self.assertEqual(stops[2]['schedule_relationship'], 'SKIPPED')
        self.assertEqual(aspect['normalized']['descriptor']['schedule_relationship'],
                         'CANCELED')

    def test_ambiguous_entity_shown_unbound_with_candidates(self):
        self.ingest_fixture('feed.json')
        self.store.load_static((FIXTURES / 'static-v2.json').read_bytes())
        self.ingest_fixture('ambiguous.json')
        view = self.store.day_view('20261112')
        self.assertEqual(len(view['unbound_entities']), 1)
        unbound = view['unbound_entities'][0]
        self.assertEqual(unbound['entity_id'], 'unspecified')
        self.assertEqual(unbound['unbound_reason'], 'ambiguous')
        self.assertEqual(len(unbound['candidates']), 3)
        # FULL_DATASET 缺席的车辆仍在视图里，带证据，不会凭空消失。
        removed = {r['entity_id']: r['removal_event'] for r in view['removed_entities']}
        self.assertEqual(removed['vehicle1'], 'absent_in_full_dataset')

    def test_trace_chain_reaches_raw_entity(self):
        receipt = self.ingest_fixture('feed.json')
        view = self.store.day_view('20261112')
        aspect = next(a for t in view['trips'] for a in t['aspects']
                      if a['entity_id'] == 'vehicle1')
        stored_receipt = self.store.get_receipt(aspect['receipt_id'])
        self.assertEqual(stored_receipt['id'], receipt['id'])
        raw = json.loads(self.archive.read(stored_receipt['capture_id']))
        self.assertIn('vehicle1', [e['id'] for e in raw['entity']])
        events = self.store.entity_events('vehicle1')
        self.assertTrue(any(e['event'] == 'aspect_upsert' for e in events))


if __name__ == '__main__':
    unittest.main()
