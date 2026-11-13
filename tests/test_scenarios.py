import unittest

from transitpulse.scenarios import run_demo


class ScenarioTests(unittest.TestCase):
    def test_full_demo_report_is_coherent(self):
        report = run_demo()
        names = [step['name'] for step in report['steps']]
        self.assertEqual(names, [
            'load_static_v1', 'full_feed', 'replay',
            'vehicle_only_differential', 'explicit_delete',
            'stale_header', 'content_conflict', 'transaction_failure',
            'load_static_v2', 'ambiguous_full_feed',
        ])
        statuses = {step['name']: step['receipt']['status']
                    for step in report['steps'] if 'receipt' in step}
        self.assertEqual(statuses['replay'], 'replayed')
        self.assertEqual(statuses['stale_header'], 'stale')
        self.assertEqual(statuses['content_conflict'], 'conflict')
        self.assertEqual(statuses['transaction_failure'], 'failed')
        self.assertTrue(
            next(step for step in report['steps']
                 if step['name'] == 'vehicle_only_differential')
            ['trip_update_still_present']
        )

        current = report['day_view_20261112']
        self.assertEqual(current['static_version'], 'static2')
        self.assertEqual(len(current['unbound_entities']), 1)
        self.assertEqual(current['unbound_entities'][0]['entity_id'], 'unspecified')
        self.assertEqual(len(current['unbound_entities'][0]['candidates']), 3)
        # 午夜前服务日的 25:10 与当日 01:10 并列。
        self.assertEqual(
            [(t['service_date'], t['start_time']) for t in current['trips']],
            [('20261111', '25:10:00'), ('20261112', '01:10:00'),
             ('20261112', '01:30:00')],
        )

        old = report['day_view_20261112_static1']
        self.assertEqual(old['static_version'], 'static1')
        self.assertFalse(old['is_current_static'])
        bound = {a['entity_id'] for t in old['trips'] for a in t['aspects']}
        self.assertEqual(bound, {'vehicle1'})
        self.assertEqual(
            {r['entity_id']: r['removal_event'] for r in old['removed_entities']},
            {'trip1': 'is_deleted'},
        )

        chain = report['trace_chain']
        self.assertEqual(chain['view_entity'], 'unspecified')
        self.assertIn('unspecified', chain['raw_feed_entity_ids'])


if __name__ == '__main__':
    unittest.main()
