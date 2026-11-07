#!/usr/bin/env python3
"""Проверка очереди Google Drive на локальном сервере, который изображает Google:
обрывает соединение, отвечает ошибками, показывает страницу лимита и не трогает сеть.

Запуск: python3 tests/test_gdrive.py
"""
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

os.environ['YDISK_CONFIG_DIR'] = tempfile.mkdtemp(prefix='ydisk-test-config-')
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402

QUOTA_PAGE = b'<html><body>Too many users have viewed or downloaded this file recently.</body></html>'
VIRUS_PAGE = (
    b'<html><body><form id="download-form" action="/download" method="get">'
    b'<input type="hidden" name="id" value="virus"><input type="hidden" name="export" value="download">'
    b'<input type="hidden" name="confirm" value="t"><input type="hidden" name="uuid" value="abc">'
    b'</form></body></html>'
)


def reset_app():
    """Каждая проверка начинает с пустой программы: без очередей и без session.json."""
    for q in list(app.queues):
        q.state['should_stop'] = True
        q.wake.set()
    with app.slot_changed:
        app.slot_changed.notify_all()
    for q in list(app.queues):
        if q.thread:
            q.thread.join(10)
    app.queues.clear()
    app.busy_paths.clear()
    app.app_state.update(quitting=False, max_active=app.DEFAULT_MAX_ACTIVE, logs=[])
    if app.session_path().exists():
        app.session_path().unlink()


def blob(seed, size):
    """Неповторяющиеся байты: сдвиг докачки на байт сразу испортит контрольную сумму."""
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f'{seed}:{counter}'.encode()).digest()
        counter += 1
    return bytes(out[:size])


class QuietServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        pass  # Обрывы соединений здесь и есть проверяемый случай


class FakeGoogle:
    def __init__(self):
        self.folders = {}   # id → (имя, [(id, имя, тип)])
        self.files = {}     # id → байты
        self.plans = {}     # id → список сценариев на следующие запросы тела файла
        self.requests = []  # (id, начало Range, сколько байт просили)
        self.real_quota = set()  # Как у настоящего лимита: Range 0-0 отвечает 206, скачивание получает отказ
        self.holds = {}     # id → Event: тело файла не отдаётся, пока событие не наступит
        self.in_flight = set()  # id файлов, которые ждут на holds
        self.slow = {}      # id → пауза в секундах между кусками по 64 КБ
        self.lock = threading.Lock()
        self.port = 0
        self.server = None

    def start(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def page(self, status, body, extra=None):
                self.send_response(status)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                file_id = query.get('id', [''])[0]
                if parsed.path == '/list':
                    self.listing(file_id)
                elif parsed.path == '/download':
                    self.download(file_id, query)
                else:
                    self.page(404, b'not found')

            def listing(self, folder_id):
                if folder_id not in fake.folders:
                    self.page(404, b'not found')
                    return
                name, entries = fake.folders[folder_id]
                rows = []
                for entry_id, entry_name, kind in entries:
                    href = {
                        'file': f'https://drive.google.com/file/d/{entry_id}/view?usp=drive_web',
                        'dir': f'https://drive.google.com/drive/folders/{entry_id}',
                        'doc': f'https://docs.google.com/document/d/{entry_id}/edit',
                    }[kind]
                    rows.append(f'<div class="flip-entry" id="entry-{entry_id}" tabindex="0"><a href="{href}">'
                                f'<div class="flip-entry-title">{html.escape(entry_name)}</div></a></div>')
                self.page(200, f'<title>{html.escape(name)}</title><div class="flip-entries">{"".join(rows)}</div>'.encode())

            def download(self, file_id, query):
                if file_id not in fake.files:
                    self.page(404, b'not found')
                    return
                data = fake.files[file_id]
                m = re.match(r'bytes=(\d+)-(\d*)', self.headers.get('Range', ''))
                start = int(m.group(1)) if m else 0
                end = int(m.group(2)) if m and m.group(2) else len(data) - 1
                is_probe = end == 0
                plan = None
                with fake.lock:
                    if not is_probe:
                        fake.requests.append((file_id, start, end - start + 1))
                    steps = fake.plans.get(file_id)
                    if steps and (not is_probe or steps[0] == 'quota'):
                        plan = steps.pop(0) if not (is_probe and steps[0] == 'quota') else 'quota'
                if plan == 'quota' or (file_id in fake.real_quota and not is_probe):
                    self.page(403, QUOTA_PAGE)
                    return
                hold = fake.holds.get(file_id)
                if hold and not is_probe:
                    with fake.lock:
                        fake.in_flight.add(file_id)
                    hold.wait(30)
                if isinstance(plan, int):
                    self.page(plan, b'<html>error page</html>', {'Retry-After': '0'})
                    return
                if plan == 'virus' and 'uuid' not in query:
                    self.page(200, VIRUS_PAGE)
                    return
                headers = {'Content-Type': 'video/mp4',
                           'Content-Disposition': f'attachment; filename="{file_id}.bin"'}
                if plan == 'ignore-range':
                    start, end = 0, len(data) - 1
                    status = 200
                else:
                    status = 206
                    if start >= len(data):
                        self.send_response(416)
                        self.send_header('Content-Range', f'bytes */{len(data)}')
                        self.send_header('Content-Length', '0')
                        self.end_headers()
                        return
                    headers['Content-Range'] = f'bytes {start}-{end}/{len(data)}'
                body = data[start:end + 1]
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                if isinstance(plan, tuple) and plan[0] == 'drop':
                    self.wfile.write(body[:plan[1]])
                    self.wfile.flush()
                    self.close_connection = True
                    self.connection.close()
                    return
                delay = fake.slow.get(file_id)
                if delay and not is_probe:
                    for pos in range(0, len(body), 64 * 1024):
                        self.wfile.write(body[pos:pos + 64 * 1024])
                        self.wfile.flush()
                        time.sleep(delay)
                    return
                self.wfile.write(body)

        self.server = QuietServer(('127.0.0.1', self.port), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        app.GDRIVE_LIST_URL = f'http://127.0.0.1:{self.port}/list'
        app.GDRIVE_DOWNLOAD_URL = f'http://127.0.0.1:{self.port}/download'

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.google = FakeGoogle()
        self.google.start()
        self.dest = Path(tempfile.mkdtemp(prefix='ydisk-test-'))
        app.BACKOFF_BASE = 0.05
        app.BACKOFF_MAX = 0.2
        app.QUOTA_RETRY = 0.5
        app.DEFER_RETRY = 0.5
        app.RESCAN_INTERVAL = 0.2
        app.CHUNK_SIZE = 64 * 1024
        reset_app()
        self.q = None

    def tearDown(self):
        reset_app()
        self.google.stop()
        shutil.rmtree(self.dest, ignore_errors=True)

    def run_queue(self, url='https://drive.google.com/drive/folders/root', timeout=30, flat=True):
        self.q = app.add_queue([url], self.dest, None, '', flat)
        self.q.thread.join(timeout)
        if self.q.thread.is_alive():
            app.stop_queue(self.q)
            self.q.thread.join(10)
            self.fail(f"Очередь зависла. Журнал:\n" + "\n".join(self.q.state['logs']))
        return self.q.state

    def one_file(self, size=1_000_000, plan=None):
        self.google.files['f1'] = blob('f1', size)
        self.google.folders['root'] = ('Папка', [('f1', 'a.mp4', 'file')])
        if plan:
            self.google.plans['f1'] = plan
        return self.google.files['f1']

    def assert_saved(self, name, data):
        path = self.dest / name
        self.assertTrue(path.exists(), f"Нет файла {name}. Журнал:\n" + "\n".join(self.q.state['logs']))
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), hashlib.sha256(data).hexdigest())
        self.assertFalse(path.with_name(path.name + '.part').exists())

    def test_nested_folders_duplicates_and_documents(self):
        g = self.google
        g.files.update({'id1': blob(1, 1000), 'id2': blob(2, 2000), 'id3': blob(3, 3000)})
        g.folders['root'] = ('Съёмка & день', [('id1', 'a.mp4', 'file'), ('id2', 'a.mp4', 'file'),
                                               ('sub', 'Звук', 'dir'), ('doc', 'План', 'doc'), ('loop', 'Ярлык', 'dir')])
        g.folders['sub'] = ('Звук', [('id3', 'b.wav', 'file')])
        g.folders['loop'] = ('Ярлык', [('root', 'Назад', 'dir')])
        state = self.run_queue(flat=False)
        root = self.dest / 'Съёмка & день'
        self.assertEqual(root.joinpath('a.mp4').read_bytes(), g.files['id1'])
        self.assertEqual(root.joinpath('a [id2].mp4').read_bytes(), g.files['id2'])
        self.assertEqual(root.joinpath('Звук', 'b.wav').read_bytes(), g.files['id3'])
        self.assertEqual(state['total_files'], 3)
        self.assertEqual(state['folders'][0]['skipped'], 1)
        self.assertIn('пропущено документов Google: 1', state['status_text'])

    def test_resume_after_dropped_connection_continues_from_same_byte(self):
        data = self.one_file(plan=[('drop', 300_000), ('drop', 250_000)])
        self.run_queue()
        self.assert_saved('a.mp4', data)
        starts = [r[1] for r in self.google.requests]
        self.assertEqual(starts[0], 0)
        self.assertEqual(len(starts), 3)
        self.assertGreater(starts[1], 0)
        self.assertGreater(starts[2], starts[1])

    def test_http_errors_are_retried_with_backoff(self):
        data = self.one_file(plan=[403, 429, 500, 503, ('drop', 100_000), 502])
        state = self.run_queue()
        self.assert_saved('a.mp4', data)
        self.assertEqual(state['status_text'], 'Загрузка завершена!')

    def test_error_page_does_not_damage_part(self):
        data = self.one_file(plan=[503])
        (self.dest / 'a.mp4.part').write_bytes(data[:400_000])
        self.run_queue()
        self.assert_saved('a.mp4', data)
        self.assertEqual([r[1] for r in self.google.requests], [400_000, 400_000])

    def test_quota_does_not_block_queue_and_is_retried(self):
        g = self.google
        g.files.update({'q': blob('q', 5000), 'ok': blob('ok', 6000)})
        g.folders['root'] = ('Папка', [('ok', 'ok.mp4', 'file'), ('q', 'q.mp4', 'file')])
        g.plans['q'] = ['quota', 'quota']
        # Лимит держится до тех пор, пока не скачан соседний файл
        seen_deferred = []
        real_defer = app.defer_item

        def spy(item, *args):
            seen_deferred.append((item['name'], (self.dest / 'ok.mp4').exists()))
            g.plans['q'].clear()
            return real_defer(item, *args)

        app.defer_item = spy
        try:
            state = self.run_queue()
        finally:
            app.defer_item = real_defer
        self.assert_saved('q.mp4', g.files['q'])
        self.assert_saved('ok.mp4', g.files['ok'])
        self.assertEqual(state['total_bytes'], 11000)
        self.assertEqual(state['status_text'], 'Загрузка завершена!')
        self.assertTrue(seen_deferred)

    def test_persistent_403_is_deferred_not_fatal(self):
        app.FILE_HTTP_RETRIES = 2
        try:
            data = self.one_file(plan=[403] * 3)
            state = self.run_queue()
        finally:
            app.FILE_HTTP_RETRIES = 6
        self.assert_saved('a.mp4', data)
        self.assertTrue(any('повтор в' in line for line in state['logs']))

    def test_existing_complete_file_is_skipped_and_short_one_is_not(self):
        g = self.google
        g.files.update({'full': blob('full', 4000), 'short': blob('short', 4000)})
        g.folders['root'] = ('Папка', [('full', 'full.mp4', 'file'), ('short', 'short.mp4', 'file')])
        (self.dest / 'full.mp4').write_bytes(g.files['full'])
        (self.dest / 'short.mp4').write_bytes(g.files['short'][:1000])
        state = self.run_queue()
        self.assertEqual([r[0] for r in g.requests], ['short'])
        self.assert_saved('short.mp4', g.files['short'])
        self.assertEqual({c['name']: c['status'] for c in state['completed_list']},
                         {'full.mp4': 'Уже был', 'short.mp4': 'Скачан'})

    def test_server_ignoring_range_rewrites_part(self):
        data = self.one_file(plan=['ignore-range'])
        (self.dest / 'a.mp4.part').write_bytes(b'x' * 1000)
        self.run_queue()
        self.assert_saved('a.mp4', data)

    def test_part_larger_than_file_is_discarded(self):
        data = self.one_file(size=5000)
        (self.dest / 'a.mp4.part').write_bytes(b'x' * 9000)
        self.run_queue()
        self.assert_saved('a.mp4', data)

    def test_file_replaced_on_drive_restarts_from_zero(self):
        data = self.one_file(size=8000)
        (self.dest / 'a.mp4.part').write_bytes(data[:2000])
        real_scan = app.gdrive_fill_sizes

        def scan_old_size(files, q):
            real_scan(files, q)
            files[0]['size'] = 9000

        app.gdrive_fill_sizes = scan_old_size
        try:
            self.run_queue()
        finally:
            app.gdrive_fill_sizes = real_scan
        self.assert_saved('a.mp4', data)

    def test_virus_warning_form_is_confirmed(self):
        g = self.google
        g.files['virus'] = blob('virus', 3000)
        g.folders['root'] = ('Папка', [('virus', 'v.zip', 'file')])
        g.plans['virus'] = ['virus']
        self.run_queue()
        self.assert_saved('v.zip', g.files['virus'])

    def test_new_file_added_during_download_is_fetched(self):
        g = self.google
        g.files.update({'one': blob('one', 200_000), 'two': blob('two', 3000)})
        g.folders['root'] = ('Папка', [('one', 'one.mp4', 'file')])
        real_finish = app.mark_file_done

        def add_second(item, status):
            real_finish(item, status)
            if item['name'] == 'one.mp4':
                g.folders['root'] = ('Папка', [('one', 'one.mp4', 'file'), ('two', 'two.mp4', 'file')])

        app.mark_file_done = add_second
        try:
            state = self.run_queue()
        finally:
            app.mark_file_done = real_finish
        self.assert_saved('two.mp4', g.files['two'])
        self.assertEqual(state['total_files'], 2)

    def test_single_file_link(self):
        data = self.one_file(size=3000)
        self.run_queue(url='https://drive.google.com/file/d/f1/view?usp=sharing')
        self.assert_saved('f1.bin', data)

    def test_private_link_reports_error(self):
        state = self.run_queue(url='https://drive.google.com/drive/folders/missing')
        self.assertIn('не открылось ссылок: 1', state['status_text'])

    def test_server_down_then_back(self):
        data = self.one_file(plan=[('drop', 200_000)])
        original_sleep = app.interruptible_sleep
        calls = []

        def restart_server(seconds, q):
            if not calls:
                self.google.stop()
                time.sleep(0.3)
                self.google.start()
            calls.append(seconds)
            original_sleep(seconds, q)

        app.interruptible_sleep = restart_server
        try:
            self.run_queue()
        finally:
            app.interruptible_sleep = original_sleep
        self.assert_saved('a.mp4', data)
        self.assertGreater(self.google.requests[-1][1], 0)

    def test_stop_keeps_part_and_next_run_resumes(self):
        data = self.one_file(size=30_000_000)
        real_add = app.add_progress

        def stop_midway(item, counted):
            real_add(item, counted)
            if counted > 5_000_000 and not item['queue'].state['should_stop']:
                app.stop_queue(item['queue'])

        app.add_progress = stop_midway
        try:
            self.run_queue()
        finally:
            app.add_progress = real_add
        part = self.dest / 'a.mp4.part'
        self.assertFalse((self.dest / 'a.mp4').exists())
        kept = part.stat().st_size
        self.assertGreater(kept, 0)
        self.run_queue()
        self.assert_saved('a.mp4', data)
        self.assertEqual(self.google.requests[-1][1], kept)

    def test_unfinished_session_resumes_on_startup(self):
        data = self.one_file(size=3000)
        app.update_session(urls=['https://drive.google.com/drive/folders/root'], dest=str(self.dest),
                           proxy='', flat=True, active=True)
        app.resume_unfinished()
        self.q = app.queues[0]
        self.q.thread.join(15)
        self.assert_saved('a.mp4', data)
        self.assertEqual(app.load_session()['queues'], [])


class YandexQueueTest(unittest.TestCase):
    """Яндекс идёт через ту же очередь: докачка должна работать и для него."""

    def setUp(self):
        self.data = blob('yandex', 500_000)
        self.ranges = []
        self.drops = [200_000]
        self.link_errors = [503]
        test = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def send_body(self, status, body, ctype='application/json', extra=None):
                self.send_response(status)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(body)))
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                port = self.server.server_address[1]
                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                if parsed.path == '/api':
                    if 'path' in query:
                        body = {'name': 'a.mp4', 'type': 'file', 'size': len(test.data), 'path': '/a.mp4'}
                    elif query.get('limit') == ['1']:
                        body = {'name': 'Папка', 'type': 'dir'}
                    else:
                        body = {'_embedded': {'items': [{'name': 'a.mp4', 'type': 'file', 'path': '/a.mp4',
                                                         'size': len(test.data)}]}}
                    self.send_body(200, json.dumps(body).encode())
                elif parsed.path == '/api/download':
                    if test.link_errors:
                        self.send_body(test.link_errors.pop(0), b'{}')
                    else:
                        self.send_body(200, json.dumps({'href': f'http://127.0.0.1:{port}/cdn'}).encode())
                else:
                    m = re.match(r'bytes=(\d+)-', self.headers.get('Range', ''))
                    start = int(m.group(1)) if m else 0
                    test.ranges.append(start)
                    body = test.data[start:]
                    self.send_response(206 if m else 200)
                    self.send_header('Content-Length', str(len(body)))
                    if m:
                        self.send_header('Content-Range', f'bytes {start}-{len(test.data) - 1}/{len(test.data)}')
                    self.end_headers()
                    if test.drops:
                        self.wfile.write(body[:test.drops.pop(0)])
                        self.wfile.flush()
                        self.close_connection = True
                        self.connection.close()
                        return
                    self.wfile.write(body)

        self.server = QuietServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.real_api = app.API_BASE
        app.API_BASE = f'http://127.0.0.1:{self.server.server_address[1]}/api'
        self.dest = Path(tempfile.mkdtemp(prefix='ydisk-test-'))
        app.BACKOFF_BASE = 0.05
        app.BACKOFF_MAX = 0.2
        app.CHUNK_SIZE = 64 * 1024
        reset_app()

    def tearDown(self):
        reset_app()
        app.API_BASE = self.real_api
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.dest, ignore_errors=True)

    def test_folder_with_drop_and_link_error(self):
        q = app.add_queue(['https://disk.yandex.ru/d/test'], self.dest)
        q.thread.join(30)
        self.assertFalse(q.thread.is_alive())
        self.assertEqual((self.dest / 'Папка' / 'a.mp4').read_bytes(), self.data)
        self.assertEqual(q.state['status_text'], 'Загрузка завершена!')
        self.assertEqual(self.ranges[0], 0)
        self.assertGreater(self.ranges[1], 0)

    def test_yandex_and_google_queues_side_by_side(self):
        google = FakeGoogle()
        google.start()
        self.addCleanup(google.stop)
        google.files['g'] = blob('g', 500_000)
        google.folders['root'] = ('Гугл', [('g', 'g.mp4', 'file')])
        other = Path(tempfile.mkdtemp(prefix='ydisk-test-'))
        self.addCleanup(shutil.rmtree, other, True)
        yandex = app.add_queue(['https://disk.yandex.ru/d/test'], self.dest)
        drive = app.add_queue(['https://drive.google.com/drive/folders/root'], other, flat=True)
        yandex.thread.join(30)
        drive.thread.join(30)
        self.assertEqual((self.dest / 'Папка' / 'a.mp4').read_bytes(), self.data)
        self.assertEqual((other / 'g.mp4').read_bytes(), google.files['g'])


class PageTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'нужен node')
    def test_page_script_has_no_syntax_errors(self):
        script = re.search(r'<script>(.*)</script>', app.INDEX_HTML, re.S).group(1)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'page.js'
            path.write_text(script, encoding='utf-8')
            result = subprocess.run(['node', '--check', str(path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


class ParsingTest(unittest.TestCase):
    def test_links(self):
        cases = {
            'https://drive.google.com/drive/folders/AbC-1_x?usp=sharing': ('folder', 'AbC-1_x'),
            'https://drive.google.com/drive/u/1/folders/AbC': ('folder', 'AbC'),
            'https://drive.google.com/file/d/XyZ/view?usp=drive_link': ('file', 'XyZ'),
            'https://drive.google.com/open?id=XyZ': ('file', 'XyZ'),
            'https://drive.usercontent.google.com/download?id=XyZ&export=download': ('file', 'XyZ'),
            'drive.google.com/drive/folders/AbC': ('folder', 'AbC'),
        }
        for url, (kind, file_id) in cases.items():
            info = app.parse_gdrive_url(url)
            self.assertEqual((info['kind'], info['id']), (kind, file_id), url)
        self.assertIsNone(app.parse_gdrive_url('https://disk.yandex.ru/d/abc'))
        self.assertEqual(app.parse_gdrive_url('https://docs.google.com/document/d/1/edit')['kind'], 'unknown')
        self.assertEqual(app.parse_gdrive_url(
            'https://drive.google.com/drive/folders/A?resourcekey=0-k')['resource_key'], '0-k')

    def test_proxy_setting(self):
        self.assertIsNone(app.check_proxy(''))
        self.assertIsNotNone(app.check_proxy('127.0.0.1:1080'))
        self.assertIsNotNone(app.check_proxy('socks5h://127.0.0.1'))
        self.assertIsNone(app.check_proxy('http://proxy.local:3128'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
