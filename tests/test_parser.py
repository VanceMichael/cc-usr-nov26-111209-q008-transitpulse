import unittest
from pathlib import Path
from transitpulse.parser import inspect_document

class ParserTests(unittest.TestCase):
    def test_entity_kinds(self):
        result = inspect_document(Path('transitpulse/fixtures/feed.json').read_bytes())
        self.assertEqual(result['entities'][0]['kinds'], ['trip_update'])
        self.assertEqual(result['entities'][1]['kinds'], ['vehicle'])

    def test_modes_and_deleted(self):
        self.assertEqual(inspect_document(Path('transitpulse/fixtures/difference.json').read_bytes())['incrementality'], 'DIFFERENTIAL')
        self.assertTrue(inspect_document(Path('transitpulse/fixtures/deleted.json').read_bytes())['entities'][0]['is_deleted'])

    def test_default_full(self):
        result = inspect_document(b'{"header":{"gtfs_realtime_version":"2.0"}}')
        self.assertEqual(result['incrementality'], 'FULL_DATASET')

    def test_duplicate_id(self):
        with self.assertRaises(ValueError):
            inspect_document(b'{"header":{"gtfs_realtime_version":"2.0"},"entity":[{"id":"x"},{"id":"x"}]}')
