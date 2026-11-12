import base64
import json
import threading
import unittest
import urllib.parse
import urllib.request

from transitpulse.source_server import FixtureServer
from transitpulse.transport import fetch


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.server = FixtureServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    def get(self, path):
        return fetch(self.base + path)

    def phase(self, value):
        self.assertEqual(self.get('/control/phase?value=' + value)[0], 200)

    def test_difference_has_no_trip_entity(self):
        self.phase('difference')
        feed = json.loads(self.get('/feed')[2])
        self.assertEqual(feed['header']['incrementality'], 'DIFFERENTIAL')
        self.assertEqual([entry['id'] for entry in feed['entity']], ['vehicle1'])

    def test_static_revision_and_zero_presence(self):
        full = json.loads(self.get('/feed')[2])
        self.assertEqual(full['entity'][0]['trip_update']['stop_time_update'][0]['arrival']['time'], 0)
        self.phase('ambiguous')
        self.assertEqual(json.loads(self.get('/static')[2])['version'], 'static2')
        self.assertNotIn('start_date', json.loads(self.get('/feed')[2])['entity'][0]['trip_update']['trip'])
