DESCRIPTION = '公交实时接口 JSON 表示的实体查看器、源响应档案与实时源接收。'
DEMO_PATH = '/feed'

import argparse
import hashlib
import json
import tempfile
import threading
from pathlib import Path

from .archive import Archive
from .parser import inspect_document
from .reception import ReceptionStore
from .scenarios import run_demo
from .source_server import FixtureServer
from .transport import fetch


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    commands = parser.add_subparsers(dest='command', required=True)
    inspect = commands.add_parser('inspect', help='查看单个源文件的结构（单包观察，不改变任何状态）')
    inspect.add_argument('path')
    capture = commands.add_parser('capture', help='发起一次本机 HTTP 请求并归档原文')
    capture.add_argument('url')
    capture.add_argument('--db', default='captures.sqlite3')
    capture.add_argument('--objects', default='objects')
    show = commands.add_parser('show', help='列出原文接收记录')
    show.add_argument('--db', default='captures.sqlite3')
    show.add_argument('--objects', default='objects')

    load_static = commands.add_parser('load-static', help='载入一版静态班次表并（如换版）重绑实时实体')
    load_static.add_argument('path')
    load_static.add_argument('--db', default='reception.sqlite3')
    load_static.add_argument('--source', default='local')

    ingest = commands.add_parser('ingest', help='接收一份供应商 feed JSON 文件')
    ingest.add_argument('path')
    ingest.add_argument('--db', default='reception.sqlite3')
    ingest.add_argument('--source', default='local')
    ingest.add_argument('--capture-db', default='captures.sqlite3')
    ingest.add_argument('--objects', default='objects')

    ingest_url = commands.add_parser('ingest-url', help='从本机源 URL 抓取并接收（先归档原文）')
    ingest_url.add_argument('url')
    ingest_url.add_argument('--db', default='reception.sqlite3')
    ingest_url.add_argument('--source', default='local')
    ingest_url.add_argument('--capture-db', default='captures.sqlite3')
    ingest_url.add_argument('--objects', default='objects')

    day = commands.add_parser('day', help='输出某个当地日历日的班次日视图')
    day.add_argument('date', help='当地日历日 YYYYMMDD')
    day.add_argument('--version', help='静态表版本，缺省为当前绑定版本，可传旧版复查')
    day.add_argument('--db', default='reception.sqlite3')
    day.add_argument('--source', default='local')

    receipts = commands.add_parser('receipts', help='列出 feed 接收收据（含重放、旧 header、冲突、失败）')
    receipts.add_argument('--db', default='reception.sqlite3')
    receipts.add_argument('--source', default='local')

    versions = commands.add_parser('versions', help='列出已载入静态表版本及当前绑定')
    versions.add_argument('--db', default='reception.sqlite3')
    versions.add_argument('--source', default='local')

    trace = commands.add_parser('trace', help='按实体 id 列出全部事件，并用 receipt 追溯原文')
    trace.add_argument('entity_id')
    trace.add_argument('--db', default='reception.sqlite3')
    trace.add_argument('--source', default='local')
    trace.add_argument('--capture-db', default='captures.sqlite3')
    trace.add_argument('--objects', default='objects')

    commands.add_parser('demo', help='运行可复现的端到端接收演练')
    arguments = parser.parse_args()

    if arguments.command == 'inspect':
        result = inspect_document(Path(arguments.path).read_bytes())
        _print(result)
        return

    if arguments.command in {'capture', 'show'}:
        archive = Archive(arguments.db, arguments.objects)
        try:
            if arguments.command == 'capture':
                status, content_type, body = fetch(arguments.url)
                result = {
                    'capture_id': archive.save(arguments.url, status, content_type, body),
                    'http_status': status,
                }
            else:
                result = archive.rows()
        finally:
            archive.close()
        _print(result)
        return

    if arguments.command == 'demo':
        _print(run_demo())
        return

    if arguments.command == 'load-static':
        store = ReceptionStore(arguments.db, arguments.source)
        try:
            result = store.load_static(Path(arguments.path).read_bytes())
        finally:
            store.close()
        _print(result)
        return

    if arguments.command in {'ingest', 'ingest-url'}:
        store = ReceptionStore(arguments.db, arguments.source)
        archive = Archive(arguments.capture_db, arguments.objects)
        try:
            if arguments.command == 'ingest':
                body = Path(arguments.path).read_bytes()
                url = 'file:' + str(Path(arguments.path).resolve())
            else:
                status, content_type, body = fetch(arguments.url)
                url = arguments.url
            capture_id = archive.save(url, 200, 'application/json', body)
            digest = hashlib.sha256(body).hexdigest()
            result = store.receive_feed(body, digest=digest, capture_id=capture_id)
        finally:
            store.close()
            archive.close()
        _print(result)
        return

    if arguments.command == 'day':
        store = ReceptionStore(arguments.db, arguments.source)
        try:
            result = store.day_view(arguments.date, version=arguments.version)
        finally:
            store.close()
        _print(result)
        return

    if arguments.command == 'receipts':
        store = ReceptionStore(arguments.db, arguments.source)
        try:
            result = store.receipts()
        finally:
            store.close()
        _print(result)
        return

    if arguments.command == 'versions':
        store = ReceptionStore(arguments.db, arguments.source)
        try:
            result = store.static_versions()
        finally:
            store.close()
        _print(result)
        return

    if arguments.command == 'trace':
        store = ReceptionStore(arguments.db, arguments.source)
        archive = Archive(arguments.capture_db, arguments.objects)
        try:
            events = store.entity_events(arguments.entity_id)
            referenced = sorted(
                {event['receipt_id'] for event in events if event['receipt_id'] is not None}
            )
            raw_entities = []
            for receipt_id in referenced:
                receipt = store.get_receipt(receipt_id)
                if receipt['capture_id'] is None:
                    continue
                body = json.loads(archive.read(receipt['capture_id']))
                raw_entities.append(
                    {
                        'receipt_id': receipt_id,
                        'capture_id': receipt['capture_id'],
                        'entity': next(
                            (e for e in body.get('entity', []) if e.get('id') == arguments.entity_id),
                            None,
                        ),
                    }
                )
            result = {'entity_id': arguments.entity_id, 'events': events, 'raw_entities': raw_entities}
        finally:
            store.close()
            archive.close()
        _print(result)
        return


if __name__ == '__main__':
    main()
