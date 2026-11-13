import json
import unittest
from pathlib import Path

from transitpulse.realtime import normalize_entity, resolve_binding, validate_feed
from transitpulse.schedule import StaticSchedule

FIXTURES = Path('transitpulse/fixtures')


class ValidationTests(unittest.TestCase):
    def test_validates_and_keeps_deleted_flag(self):
        summary = validate_feed(json.loads((FIXTURES / 'feed.json').read_text()))
        self.assertEqual(summary['incrementality'], 'FULL_DATASET')
        self.assertEqual([e['id'] for e in summary['entities']], ['trip1', 'vehicle1'])

    def test_rejects_duplicate_and_bad_version(self):
        with self.assertRaises(ValueError):
            validate_feed({'header': {'gtfs_realtime_version': '1.0'}, 'entity': []})
        with self.assertRaises(ValueError):
            validate_feed({'header': {'gtfs_realtime_version': '2.0'},
                           'entity': [{'id': 'x'}, {'id': 'x'}]})


class NormalizationTests(unittest.TestCase):
    def test_arrival_absence_distinct_from_time_zero(self):
        feed = json.loads((FIXTURES / 'feed.json').read_text())
        aspects = normalize_entity(feed['entity'][0])
        self.assertEqual(len(aspects), 1)
        stops = aspects[0]['stop_time_updates']
        self.assertTrue(stops[0]['has_arrival'])
        self.assertEqual(stops[0]['arrival_time'], 0)
        self.assertFalse(stops[1]['has_arrival'])
        self.assertIsNone(stops[1]['arrival_time'])
        self.assertEqual(stops[1]['schedule_relationship'], 'NO_DATA')

    def test_contract_relationships_are_preserved(self):
        feed = json.loads((FIXTURES / 'contract.json').read_text())
        aspect = normalize_entity(feed['entity'][0])[0]
        self.assertEqual(aspect['descriptor']['schedule_relationship'], 'CANCELED')
        relationships = [s['schedule_relationship'] for s in aspect['stop_time_updates']]
        self.assertEqual(relationships, [None, 'NO_DATA', 'SKIPPED'])

    def test_trip_update_and_vehicle_have_own_timestamps(self):
        feed = json.loads((FIXTURES / 'feed.json').read_text())
        by_kind = {}
        for entity in feed['entity']:
            for aspect in normalize_entity(entity):
                by_kind[aspect['kind']] = aspect
        self.assertEqual(by_kind['trip_update']['aspect_ts'], 1794420000)
        self.assertEqual(by_kind['vehicle']['aspect_ts'], 1794420001)


class BindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v1 = StaticSchedule.loads((FIXTURES / 'static-v1.json').read_text())
        cls.v2 = StaticSchedule.loads((FIXTURES / 'static-v2.json').read_text())

    def test_explicit_descriptor_binds(self):
        descriptor = {'trip_id': 'N1', 'start_date': '20261111',
                      'start_time': '25:10:00'}
        status, candidates, reason = resolve_binding(descriptor, 1794420000, None, self.v1)
        self.assertEqual(status, 'bound')
        self.assertIsNone(reason)
        self.assertEqual(candidates[0]['service_date'], '20261111')

    def test_missing_start_date_ambiguous_stays_unbound(self):
        descriptor = {'trip_id': 'N1'}
        status, candidates, reason = resolve_binding(descriptor, 1794420030, None, self.v2)
        self.assertEqual(status, 'unbound')
        self.assertEqual(reason, 'ambiguous')
        self.assertEqual(len(candidates), 3)

    def test_missing_start_date_no_candidate_stays_unbound(self):
        descriptor = {'trip_id': 'N1'}
        status, candidates, reason = resolve_binding(descriptor, 1794420030, None, self.v1)
        self.assertEqual(status, 'unbound')
        self.assertEqual(reason, 'no_candidate')
        self.assertEqual(candidates, [])

    def test_never_picks_nearest_when_exact_match_ambiguous(self):
        descriptor = {'trip_id': 'N1', 'start_date': '20261112'}
        status, candidates, reason = resolve_binding(descriptor, 1794420030, None, self.v2)
        self.assertEqual(status, 'unbound')
        self.assertEqual(reason, 'ambiguous')
        self.assertEqual(len(candidates), 2)

    def test_without_schedule_or_descriptor(self):
        status, _, reason = resolve_binding(None, 1, None, None)
        self.assertEqual((status, reason), ('unbound', 'no_schedule'))
        status, _, reason = resolve_binding(None, 1, None, self.v1)
        self.assertEqual((status, reason), ('unbound', 'no_descriptor'))


if __name__ == '__main__':
    unittest.main()
