"""端到端演练编排：用本机合成源重现运营报障中的每个变化。

流程对应真实排查顺序：
完整 feed -> 仅车辆差分包 -> 显式删除 -> 重放/旧 header/冲突/坏包 ->
静态表换版重绑 -> 歧义完整包。每一步都沉淀 receipt，视图可经 receipt
回到 capture 与原文实体。
"""

import hashlib
import json
import tempfile
import threading
from pathlib import Path

from .archive import Archive
from .reception import ReceptionStore
from .source_server import FixtureServer
from .transport import fetch


def _ingest(store, archive, base, path, url=None):
    status, content_type, body = fetch(base + path)
    capture_id = archive.save(url or (base + path), status, content_type, body)
    digest = hashlib.sha256(body).hexdigest()
    return store.receive_feed(body, digest=digest, capture_id=capture_id)


def run_demo():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        server = FixtureServer()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = 'http://127.0.0.1:' + str(server.server_address[1])
        archive = Archive(root / 'captures.sqlite3', root / 'objects')
        store = ReceptionStore(root / 'reception.sqlite3')
        report = {'steps': []}
        try:
            # 1. 载入 static1 并接收首个完整 feed。
            _, _, static_body = fetch(base + '/static')
            loaded = store.load_static(static_body)
            report['steps'].append({'name': 'load_static_v1', 'result': loaded})

            first = _ingest(store, archive, base, '/feed')
            report['steps'].append({'name': 'full_feed', 'receipt': first})

            # 2. 同文重放：原文相同，不推进状态。
            replay = _ingest(store, archive, base, '/feed')
            report['steps'].append({'name': 'replay', 'receipt': replay})

            # 3. 仅车辆差分包：TripUpdate 缺席但仍须保留。
            server.phase = 'difference'
            difference = _ingest(store, archive, base, '/feed')
            view_after_difference = store.day_view('20261112')
            report['steps'].append(
                {
                    'name': 'vehicle_only_differential',
                    'receipt': difference,
                    'trip_update_still_present': any(
                        aspect['kind'] == 'trip_update'
                        for trip in view_after_difference['trips']
                        for aspect in trip['aspects']
                    ),
                }
            )

            # 4. 显式 is_deleted：与 FULL_DATASET 缺席是不同证据。
            server.phase = 'deleted'
            deleted = _ingest(store, archive, base, '/feed')
            report['steps'].append({'name': 'explicit_delete', 'receipt': deleted})

            # 5. 较旧 header：拒绝推进，原文仍留 receipt。
            stale_body = json.dumps(
                {
                    'header': {
                        'gtfs_realtime_version': '2.0',
                        'incrementality': 'DIFFERENTIAL',
                        'timestamp': 1794420005,
                    },
                    'entity': [],
                }
            ).encode()
            stale = store.receive_feed(
                stale_body, digest=hashlib.sha256(stale_body).hexdigest()
            )
            report['steps'].append({'name': 'stale_header', 'receipt': stale})

            # 6. 同 header 时间戳异文：冲突，不接收。
            conflict_body = json.dumps(
                {
                    'header': {
                        'gtfs_realtime_version': '2.0',
                        'incrementality': 'DIFFERENTIAL',
                        'timestamp': 1794420020,
                    },
                    'entity': [{'id': 'intruder'}],
                }
            ).encode()
            conflict = store.receive_feed(
                conflict_body, digest=hashlib.sha256(conflict_body).hexdigest()
            )
            report['steps'].append({'name': 'content_conflict', 'receipt': conflict})

            # 7. 坏实体触发事务中断：整体回滚，状态保持，并留 failed receipt。
            bad_body = json.dumps(
                {
                    'header': {
                        'gtfs_realtime_version': '2.0',
                        'incrementality': 'DIFFERENTIAL',
                        'timestamp': 1794420025,
                    },
                    'entity': [
                        {
                            'id': 'broken',
                            'vehicle': {
                                'trip': {'trip_id': 'N1', 'start_time': '99:99:99'},
                                'timestamp': 1794420025,
                            },
                        }
                    ],
                }
            ).encode()
            failed = store.receive_feed(
                bad_body, digest=hashlib.sha256(bad_body).hexdigest()
            )
            report['steps'].append({'name': 'transaction_failure', 'receipt': failed})

            # 8. 换 static2：旧版冻结，重绑有效 aspect。
            server.phase = 'ambiguous'
            _, _, static_v2 = fetch(base + '/static')
            switched = store.load_static(static_v2)
            report['steps'].append({'name': 'load_static_v2', 'result': switched})

            # 9. 歧义完整包：缺 start_date 的实体在 static2 下有三个候选，未绑定。
            ambiguous = _ingest(store, archive, base, '/feed')
            report['steps'].append({'name': 'ambiguous_full_feed', 'receipt': ambiguous})

            # 10. 运营复查视图与追溯链。
            current_view = store.day_view('20261112')
            old_view = store.day_view('20261112', version='static1')
            report['day_view_20261112'] = current_view
            report['day_view_20261112_static1'] = old_view
            report['receipts'] = store.receipts()
            report['static_versions'] = store.static_versions()
            report['events_unspecified'] = store.entity_events('unspecified')

            trace_receipt = store.get_receipt(ambiguous['id'])
            raw = archive.read(trace_receipt['capture_id'])
            report['trace_chain'] = {
                'view_entity': current_view['unbound_entities'][0]['entity_id'],
                'receipt_id': ambiguous['id'],
                'capture_id': trace_receipt['capture_id'],
                'raw_feed_entity_ids': [e['id'] for e in json.loads(raw)['entity']],
            }
            return report
        finally:
            store.close()
            archive.close()
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()
