import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from transitpulse.schedule import StaticSchedule, parse_clock, parse_service_date


class ScheduleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pathlib import Path
        cls.v1 = StaticSchedule.loads(Path('transitpulse/fixtures/static-v1.json').read_bytes())
        cls.v2 = StaticSchedule.loads(Path('transitpulse/fixtures/static-v2.json').read_bytes())

    def test_over_24h_intervals_match_next_day_local_trip(self):
        # 20261111 的 25:10 与 20261112 的 01:10 是同一段绝对时间。
        late = next(t for t in self.v1.trips if t['service_date'] == '20261111')
        early = next(t for t in self.v2.trips if t['service_date'] == '20261112'
                     and t['start_time'] == '01:10:00')
        self.assertEqual(late['start_ts'], early['start_ts'])
        local = datetime.fromtimestamp(late['start_ts'], ZoneInfo('Asia/Shanghai'))
        self.assertEqual(local.strftime('%Y-%m-%d %H:%M'), '2026-11-12 01:10')

    def test_exact_match_by_descriptor(self):
        candidates = self.v1.exact('N1', '20261111', '25:10:00')
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['end_time'], '26:00:00')

    def test_exact_match_multiple_is_not_resolved(self):
        # v2 在 20261112 有两个班次，精确匹配也不能替运营挑一个。
        candidates = self.v2.exact('N1', '20261112')
        self.assertEqual(len(candidates), 2)

    def test_covering_candidates_ambiguous(self):
        candidates = self.v2.covering('N1', 1794420030)
        self.assertEqual(len(candidates), 3)

    def test_covering_with_start_time_filter(self):
        # start_time 是服务日内时长：01:10:00 不匹配 25:10:00 的班次。
        candidates = self.v2.covering('N1', 1794420030, start_time='01:10:00')
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['service_date'], '20261112')
        candidates = self.v2.covering('N1', 1794420030, start_time='25:10:00')
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['service_date'], '20261111')

    def test_no_candidate_when_outside_all_intervals(self):
        self.assertEqual(self.v1.covering('N1', 1794420030), [])

    def test_day_window_includes_previous_service_day_trip(self):
        rows = self.v2.trips_overlapping_day('20261112')
        self.assertIn('20261111', {r['service_date'] for r in rows})
        self.assertEqual(len(rows), 3)

    def test_clock_and_date_parsing(self):
        self.assertEqual(parse_clock('25:10:00'), 25 * 3600 + 600)
        self.assertEqual(parse_service_date('20261112').day, 12)
        with self.assertRaises(ValueError):
            parse_clock('24:60:00')

    def test_reject_bad_static(self):
        with self.assertRaises(ValueError):
            StaticSchedule({'version': 'x', 'timezone': 'Asia/Shanghai', 'trips': []})


if __name__ == '__main__':
    unittest.main()
