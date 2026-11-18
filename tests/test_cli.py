#!/usr/bin/env python3
"""Проверка команды ydisk (ydisk_cli.py) против настоящего HTTP-сервера программы
с поддельным Я.Диском и поддельным Google: сеть не трогается.

Запуск: python3 tests/test_cli.py
"""
import contextlib
import io
import json
import shutil
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import test_upload as T  # noqa: E402  Задаёт отдельный YDISK_CONFIG_DIR и поддельный Диск
from test_gdrive import FakeGoogle, blob  # noqa: E402
import ydisk_cli  # noqa: E402

app, uploader = T.app, T.uploader
GDRIVE = 'https://drive.google.com/drive/folders/'


class CliBase(T.Base):
    def setUp(self):
        super().setUp()
        self.google = FakeGoogle()
        self.google.start()
        app.BACKOFF_BASE = 0.05
        app.BACKOFF_MAX = 0.2
        app.CHUNK_SIZE = 64 * 1024

        class QuietHandler(app.RequestHandler):
            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), QuietHandler)
        self.server.daemon_threads = True
        port = self.server.server_address[1]
        self.url = f'http://127.0.0.1:{port}'
        self.real_hosts = app.ALLOWED_HOSTS
        app.ALLOWED_HOSTS = {f'127.0.0.1:{port}'}
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        uploader.load_server_token = lambda: T.TOKEN  # «Токен уже задан на сервере»

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        app.ALLOWED_HOSTS = self.real_hosts
        self.google.stop()
        super().tearDown()
        uploader.load_server_token = UPLOADER_TOKEN_FN

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = ydisk_cli.main(['--at', self.url, '--json', *argv])
        # Программа здесь в том же процессе и печатает свой журнал: ответ команды — последняя JSON-строка
        lines = [x for x in out.getvalue().splitlines() if x.startswith('{')]
        return code, (json.loads(lines[-1]) if lines else None), err.getvalue()


UPLOADER_TOKEN_FN = uploader.load_server_token


class CliTest(CliBase):
    def test_upload_wait_returns_link_and_order(self):
        self.make('P/a.bin', 2000)
        self.make('P/._a.bin', 5)
        code, res, _ = self.run_cli('upload', str(self.tmp / 'P'), '--to', 'Рендеры/3909', '--order', '3909',
                                    '--threads', '4', '--wait', '--timeout', '30')
        self.assertEqual(code, 0, res)
        self.assertEqual(res['display'], 'done')
        self.assertEqual(res['order'], '3909')
        self.assertTrue(res['public_url'].startswith('https://yadi.sk/'))
        self.assertEqual(res['done_files'], 1)
        self.assertEqual(list(self.fake.files), ['disk:/Рендеры/3909/P/a.bin'])
        # Пометка заказа сохраняется в списке очередей
        code, res, _ = self.run_cli('status', '--order', '3909')
        self.assertEqual([i['kind'] for i in res['items']], ['upload'])
        code, res, _ = self.run_cli('status', '--order', 'другой')
        self.assertEqual(res['items'], [])

    def test_upload_failure_exit_code(self):
        self.make('P/huge.bin', 2000)
        self.fake.fail_put['disk:/R/P/huge.bin'] = [413]
        code, res, _ = self.run_cli('upload', str(self.tmp / 'P'), '--to', 'R', '--wait', '--timeout', '30')
        self.assertEqual(code, 1)
        self.assertEqual(res['display'], 'problems')
        self.assertEqual(res['failed'], 1)
        self.assertEqual(res['failed_files'], ['disk:/R/P/huge.bin'])

    def test_controls_and_timeout(self):
        self.make('P/a.bin', 2000)
        self.make('P/b.bin', 1000)
        self.fake.hold = threading.Event()
        code, res, _ = self.run_cli('upload', str(self.tmp / 'P'), '--to', 'R', '--threads', '1')
        self.assertEqual(code, 0)
        qid = res['id']
        self.wait(lambda: self.fake.in_flight == 1, 'заливка идёт')
        code, res, _ = self.run_cli('wait', qid, '--timeout', '0.5')
        self.assertEqual(code, 3)  # не дождались
        self.assertEqual(self.run_cli('pause', qid)[1]['display'], 'paused')
        self.assertEqual(self.run_cli('resume', qid)[1]['display'], 'uploading')
        self.assertEqual(self.run_cli('top', qid)[0], 0)
        self.assertEqual(self.run_cli('stop', qid)[0], 0)
        self.fake.hold.set()
        code, res, _ = self.run_cli('wait', qid, '--timeout', '30')
        self.assertEqual((code, res['display']), (1, 'stopped'))
        self.assertEqual(self.run_cli('restart', qid)[0], 0)
        code, res, _ = self.run_cli('wait', qid, '--timeout', '30')
        self.assertEqual((code, res['display']), (0, 'done'))
        self.assertEqual(self.run_cli('remove', qid)[1], {'id': qid, 'removed': True})
        self.assertEqual(self.run_cli('status')[1]['items'], [])

    def test_download_with_order(self):
        data = blob('clip', 150_000)
        self.google.files['clip'] = data
        self.google.folders['clip'] = ('Клипы', [('clip', 'clip.mp4', 'file')])
        dest = self.tmp / 'down'
        code, res, _ = self.run_cli('download', GDRIVE + 'clip', '--dest', str(dest), '--order', 'L7',
                                    '--wait', '--timeout', '30')
        self.assertEqual(code, 0, res)
        self.assertEqual((res['kind'], res['order'], res['display']), ('download', 'L7', 'done'))
        self.assertEqual((dest / 'Клипы' / 'clip.mp4').read_bytes(), data)
        entries = app.load_session().get('queues', [])
        self.assertEqual(entries, [])  # Завершённая очередь не продолжается после перезапуска
        self.assertEqual(self.run_cli('top', res['id'])[0], 2)  # «Наверх» только у заливок

    def test_errors(self):
        code, res, _ = self.run_cli('pause', 'нет-такой')
        self.assertEqual(code, 2)
        self.assertIn('Нет очереди', res['error'])
        code, res, _ = self.run_cli('upload', str(self.tmp / 'нет'), '--to', 'R')
        self.assertEqual(code, 2)
        self.assertIn('Нет такого', res['error'])

    def test_no_connection(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = ydisk_cli.main(['--at', 'http://127.0.0.1:9', '--json', 'status'])
        self.assertEqual(code, 2)
        self.assertIn('Нет связи', json.loads(out.getvalue())['error'])

    def test_instances(self):
        self.assertEqual(ydisk_cli.base_url('server'), 'http://192.168.1.77:7860')
        self.assertEqual(ydisk_cli.base_url('http://x:1/'), 'http://x:1')


if __name__ == '__main__':
    unittest.main(verbosity=1)
