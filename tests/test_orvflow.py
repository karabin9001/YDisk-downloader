#!/usr/bin/env python3
"""Проверка очереди ORV Cloud (раздачи cloud.orvflow.ru) на локальном сервере, который его изображает:
список с подпапками, одинаковые имена, обрыв и докачка с нужного байта, закрытая раздача. Сеть не трогает.

Запуск: python3 tests/test_orvflow.py
"""
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

os.environ['YDISK_CONFIG_DIR'] = tempfile.mkdtemp(prefix='ydisk-test-config-')
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402
from test_gdrive import QuietServer, blob, reset_app  # noqa: E402

TOKEN = 'NXTIfFTkWOImsgIfmW6bTnTpjDOXdNoN'


class FakeOrv:
    """folders: id → (имя, [ид подпапок]); files: id → (папка, имя, байты); drops: id → оборвать после N байт (раз)."""

    def __init__(self):
        self.folders = {}
        self.files = {}
        self.drops = {}
        self.requests = []  # (id, начало Range)
        self.closed = False
        self.lock = threading.Lock()

    def start(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def send(self, status, body, ctype='application/json', extra=None):
                self.send_response(status)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(body)))
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                if fake.closed or not parsed.path.startswith(f'/s/{TOKEN}/'):
                    self.send(404, b'{"detail":"Not found"}')
                    return
                rest = parsed.path[len(f'/s/{TOKEN}'):]
                if rest == '/list':
                    folder = urllib.parse.parse_qs(parsed.query).get('folder_id', ['root'])[0]
                    self.listing(folder)
                    return
                m = re.fullmatch(r'/file/([\w-]+)/download', rest)
                if m and m.group(1) in fake.files:
                    self.download(m.group(1))
                    return
                self.send(404, b'{"detail":"File not found"}')

            def listing(self, folder):
                if folder not in fake.folders:
                    self.send(404, b'{"detail":"Folder not found"}')
                    return
                name, subs = fake.folders[folder]
                files = [{'id': fid, 'name': fname, 'size': len(data), 'content_type': 'video/mp4'}
                         for fid, (fold, fname, data) in fake.files.items() if fold == folder]
                body = {'folder': {'id': folder, 'is_root': folder == 'root'},
                        'breadcrumbs': [{'id': folder, 'name': name}],
                        'folders': [{'id': s, 'name': fake.folders[s][0]} for s in subs], 'files': files}
                self.send(200, json.dumps(body, ensure_ascii=False).encode())

            def download(self, fid):
                data = fake.files[fid][2]
                m = re.match(r'bytes=(\d+)-', self.headers.get('Range', ''))
                start = int(m.group(1)) if m else 0
                with fake.lock:
                    fake.requests.append((fid, start))
                    drop = fake.drops.pop(fid, None)
                if start >= len(data):
                    self.send(416, b'', extra={'Content-Range': f'bytes */{len(data)}'})
                    return
                body = data[start:]
                self.send_response(206 if m else 200)
                self.send_header('Content-Type', 'video/mp4')
                self.send_header('Content-Length', str(len(body)))
                if m:
                    self.send_header('Content-Range', f'bytes {start}-{len(data) - 1}/{len(data)}')
                self.end_headers()
                if drop is not None:
                    self.wfile.write(body[:drop])
                    self.wfile.flush()
                    self.close_connection = True
                    self.connection.close()
                    return
                self.wfile.write(body)

        self.server = QuietServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        app.ORVFLOW_HOSTS = ('127.0.0.1',)
        self.url = f'http://127.0.0.1:{self.port}/s/{TOKEN}'

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


class ParseTest(unittest.TestCase):
    def test_urls(self):
        info = app.parse_orvflow_url(f'https://cloud.orvflow.ru/s/{TOKEN}')
        self.assertEqual(info, {'share': f'https://cloud.orvflow.ru/s/{TOKEN}', 'folder_id': None})
        info = app.parse_orvflow_url(f'cloud.orvflow.ru/s/{TOKEN}/?folder_id=abc-1')
        self.assertEqual(info['folder_id'], 'abc-1')
        self.assertIsNone(app.parse_orvflow_url('https://disk.yandex.ru/d/abc'))
        with self.assertRaises(app.PermanentError):
            app.parse_orvflow_url('https://cloud.orvflow.ru/login')
        self.assertEqual(app.link_source(f'https://cloud.orvflow.ru/s/{TOKEN}'), 'orvflow')
        self.assertEqual(app.link_source('https://cloud.orvflow.ru/login'), 'orvflow')
        self.assertEqual(app.link_source('https://drive.google.com/drive/folders/x'), 'gdrive')
        self.assertEqual(app.link_source('https://disk.yandex.ru/d/abc'), 'yandex')


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.orv = FakeOrv()
        self.orv.start()
        self.dest = Path(tempfile.mkdtemp(prefix='ydisk-test-'))
        app.BACKOFF_BASE = 0.05
        app.BACKOFF_MAX = 0.2
        app.DEFER_RETRY = 0.5
        app.RESCAN_INTERVAL = 0.2
        app.CHUNK_SIZE = 64 * 1024
        reset_app()
        self.q = None

    def tearDown(self):
        reset_app()
        self.orv.stop()
        app.ORVFLOW_HOSTS = ('cloud.orvflow.ru',)
        shutil.rmtree(self.dest, ignore_errors=True)

    def run_queue(self, url=None, flat=False, timeout=30):
        self.q = app.add_queue([url or self.orv.url], self.dest, None, '', flat, order='282')
        self.q.thread.join(timeout)
        if self.q.thread.is_alive():
            app.stop_queue(self.q)
            self.q.thread.join(10)
            self.fail("Очередь зависла. Журнал:\n" + "\n".join(self.q.state['logs']))
        return self.q.state

    def log(self):
        return "\n".join(self.q.state['logs'])

    def test_folder_with_subfolders_and_same_names(self):
        o = self.orv
        o.folders['root'] = ('26.11.13 КАДИС конференция', ['sub'])
        o.folders['sub'] = ('Звук', [])
        o.files['f1'] = ('root', 'capture 01.mp4', blob('f1', 300_000))
        o.files['f2'] = ('root', 'capture 01.mp4', blob('f2', 200_000))
        o.files['f3'] = ('sub', 'zoom.wav', blob('f3', 50_000))
        state = self.run_queue()
        root = self.dest / '26.11.13 КАДИС конференция'
        self.assertEqual(state['phase'], 'done', self.log())
        self.assertEqual(root.joinpath('capture 01.mp4').read_bytes(), o.files['f1'][2])
        self.assertEqual(root.joinpath('capture 01 [f2].mp4').read_bytes(), o.files['f2'][2])
        self.assertEqual(root.joinpath('Звук', 'zoom.wav').read_bytes(), o.files['f3'][2])
        self.assertEqual(state['total_files'], 3)
        self.assertEqual(state['folders'][0]['source'], 'orvflow')
        self.assertEqual(self.q.order, '282')

    def test_resume_after_drop_from_right_byte(self):
        data = blob('big', 1_000_000)
        self.orv.folders['root'] = ('Папка', [])
        self.orv.files['big'] = ('root', 'a.mp4', data)
        self.orv.drops['big'] = 400_000
        state = self.run_queue(flat=True)
        self.assertEqual(state['phase'], 'done', self.log())
        saved = (self.dest / 'a.mp4').read_bytes()
        self.assertEqual(hashlib.sha256(saved).hexdigest(), hashlib.sha256(data).hexdigest())
        starts = [s for fid, s in self.orv.requests if fid == 'big']
        self.assertEqual(starts[0], 0)
        self.assertGreater(starts[-1], 0, "докачка должна идти с Range, а не с начала")
        self.assertFalse((self.dest / 'a.mp4.part').exists())

    def test_already_downloaded_is_skipped(self):
        data = blob('x', 100_000)
        self.orv.folders['root'] = ('Папка', [])
        self.orv.files['x'] = ('root', 'a.mp4', data)
        (self.dest / 'a.mp4').write_bytes(data)
        state = self.run_queue(flat=True)
        self.assertEqual(state['phase'], 'done', self.log())
        self.assertEqual(self.orv.requests, [])

    def test_closed_share_is_a_problem_not_a_hang(self):
        self.orv.closed = True
        state = self.run_queue()
        self.assertEqual(state['phase'], 'problems', self.log())
        self.assertIn('недоступна', self.log())


if __name__ == '__main__':
    unittest.main()
