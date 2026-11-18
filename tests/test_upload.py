#!/usr/bin/env python3
"""Проверка заливки на Яндекс.Диск на поддельном Диске в памяти: сеть не трогается.

Запуск: python3 tests/test_upload.py
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_gdrive import app, reset_app  # noqa: E402  Задаёт отдельный YDISK_CONFIG_DIR

import requests  # noqa: E402
import uploader  # noqa: E402

TOKEN = 'test-token-not-real'
UPLOADER_HOST = 'https://uploader.fake/'


class FakeResp:
    def __init__(self, status, data=None, headers=None):
        self.status_code = status
        self._data = data
        self.headers = headers or {}

    def json(self):
        if self._data is None:
            raise ValueError('no json')
        return self._data


class FakeYandex:
    """Яндекс.Диск в памяти: папки, файлы с размерами, ссылки для заливки, публикация."""

    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}           # disk:/путь → размер
        self.dirs = set()
        self.public = {}
        self.fail_put = {}        # путь → коды ответа на первые PUT
        self.fail_link = {}       # путь → коды ответа на первые запросы ссылки
        self.hold = None          # Event: пока не взведён, PUT «висит» на середине файла
        self.in_flight = 0
        self.max_in_flight = 0
        self.put_attempts = []    # пути в порядке начала отправки
        self.uploader_auth = []   # заголовки Authorization, пришедшие загрузчику (их быть не должно)
        self.api_tokens = set()
        self.sessions = []
        self.list_calls = 0

    def session(self):
        s = FakeSession(self)
        self.sessions.append(s)
        return s

    def parent_exists(self, path):
        parent = path.rsplit('/', 1)[0]
        return parent == 'disk:' or parent in self.dirs

    def handle(self, method, url, params, data, headers):
        params = params or {}
        headers = headers or {}
        if url.startswith(UPLOADER_HOST):
            return self.put_file(urllib.parse.unquote(url[len(UPLOADER_HOST):]), data, headers)
        self.api_tokens.add(headers.get('Authorization'))
        endpoint = url[len(uploader.API):]
        path = params.get('path')
        with self.lock:
            if endpoint == '/resources' and method == 'GET':
                if params.get('fields') == 'public_url':
                    return FakeResp(200, {'public_url': self.public.get(path)})
                self.list_calls += 1
                if path not in self.dirs:
                    return FakeResp(404, {'error': 'DiskNotFoundError'})
                items = sorted(
                    [{'path': d, 'type': 'dir'} for d in self.dirs if d.rsplit('/', 1)[0] == path] +
                    [{'path': f, 'type': 'file', 'size': s} for f, s in self.files.items() if f.rsplit('/', 1)[0] == path],
                    key=lambda x: x['path'])
                off, lim = int(params.get('offset', 0)), int(params.get('limit', 20))
                return FakeResp(200, {'_embedded': {'items': items[off:off + lim], 'total': len(items)}})
            if endpoint == '/resources' and method == 'PUT':
                if path in self.dirs:
                    return FakeResp(409, {'error': 'DiskPathPointsToExistentDirectoryError'})
                if not self.parent_exists(path):
                    return FakeResp(409, {'error': 'DiskPathDoesntExistsError'})
                self.dirs.add(path)
                return FakeResp(201, {})
            if endpoint == '/resources/upload':
                codes = self.fail_link.get(path)
                if codes:
                    code = codes.pop(0)
                    err = 'DiskPathDoesntExistsError' if code == 409 else 'Error'
                    return FakeResp(code, {'error': err}, {'Retry-After': '0'} if code == 429 else {})
                if not self.parent_exists(path):
                    return FakeResp(409, {'error': 'DiskPathDoesntExistsError'})
                return FakeResp(200, {'href': UPLOADER_HOST + urllib.parse.quote(path), 'method': 'PUT', 'templated': False})
            if endpoint == '/resources/publish':
                if path not in self.dirs:
                    return FakeResp(404, {'error': 'DiskNotFoundError'})
                self.public[path] = 'https://yadi.sk/d/fake-' + str(len(self.public))
                return FakeResp(200, {})
        return FakeResp(400, {'error': 'unknown'})

    def put_file(self, path, data, headers):
        self.uploader_auth.append(headers.get('Authorization'))
        expected = len(data)
        with self.lock:
            self.put_attempts.append(path)
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            got = data.read(1024)
            # «Медленная» отправка: ждём, но читаем по нулю байт, чтобы «Стоп» мог оборвать
            while self.hold is not None and not self.hold.wait(0.02):
                data.read(0)
            while True:
                chunk = data.read(65536)
                if not chunk:
                    break
                got += chunk
        finally:
            with self.lock:
                self.in_flight -= 1
        with self.lock:
            codes = self.fail_put.get(path)
            if codes:
                return FakeResp(codes.pop(0), {'error': 'Fake'})
            assert len(got) == expected, (len(got), expected)
            self.files[path] = len(got)
        return FakeResp(201)


class FakeSession:
    def __init__(self, fake):
        self.fake = fake
        self.trust_env = False

    def request(self, method, url, params=None, data=None, timeout=None, headers=None):
        return self.fake.handle(method, url, params, data, headers)


def reset_uploads():
    for q in list(uploader.uploads):
        q.state['should_stop'] = True
        q.abort.set()
        with q.cond:
            q.cond.notify_all()
    for q in list(uploader.uploads):
        if q.thread:
            q.thread.join(10)
    uploader.uploads.clear()
    uploader.flags['quitting'] = False


class Base(unittest.TestCase):
    def setUp(self):
        reset_app()
        reset_uploads()
        self.fake = FakeYandex()
        self.real_session = uploader.hooks['session']
        uploader.hooks['session'] = self.fake.session
        self.saved = {k: getattr(uploader, k) for k in ('RETRY_BASE', 'RETRY_MAX', 'LIST_LIMIT')}
        uploader.RETRY_BASE = 0.01
        uploader.RETRY_MAX = 0.05
        self.tmp = Path(tempfile.mkdtemp(prefix='ydisk-up-test-'))
        os.environ.pop('YANDEX_DISK_TOKEN', None)
        os.environ.pop('YDISK_ENV_FILE', None)

    def tearDown(self):
        if self.fake.hold:
            self.fake.hold.set()
        reset_uploads()
        reset_app()
        uploader.hooks['session'] = self.real_session
        for k, v in self.saved.items():
            setattr(uploader, k, v)
        os.environ.pop('YANDEX_DISK_TOKEN', None)
        os.environ.pop('YDISK_ENV_FILE', None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make(self, rel, size):
        p = self.tmp / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(os.urandom(size))
        return p

    def project(self):
        """Папка «Проект» с мусором macOS."""
        self.make('Проект/a.mp4', 3000)
        self.make('Проект/sub/b.mov', 5000)
        self.make('Проект/sub/deep/c.wav', 1000)
        self.make('Проект/._a.mp4', 10)
        self.make('Проект/.DS_Store', 10)
        self.make('Проект/sub/._b.mov', 10)
        self.make('Проект/.Trashes/old.mp4', 10)
        (self.tmp / 'Проект' / 'empty').mkdir()
        return self.tmp / 'Проект'

    def add(self, sources, remote='Заливка/Тест', threads=4, token=TOKEN):
        q, error = uploader.add([str(s) for s in sources], remote, threads, token)
        self.assertIsNone(error)
        return q

    def wait(self, check, what, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if check():
                return
            time.sleep(0.01)
        logs = "\n".join(line for q in uploader.uploads for line in q.state['logs'])
        self.fail(f"Не дождались: {what}.\n{logs}")

    def finish(self, q):
        q.thread.join(30)
        self.assertFalse(q.thread.is_alive(), q.state['logs'])
        return q.snapshot()


class ScanTest(Base):
    def test_junk_skipped_and_layout(self):
        src = self.project()
        single = self.make('one.txt', 7)
        self.make('._one.txt', 1)
        files, dirs, junk, problems = uploader.scan_sources([src, single, self.tmp / '._one.txt'], 'disk:/R')
        self.assertEqual(problems, [])
        self.assertEqual(sorted(f['remote'] for f in files), [
            'disk:/R/one.txt', 'disk:/R/Проект/a.mp4', 'disk:/R/Проект/sub/b.mov', 'disk:/R/Проект/sub/deep/c.wav'])
        self.assertEqual(junk, 4)  # ._a, .DS_Store, ._b и ._one, переданный отдельным файлом
        self.assertEqual(sorted(dirs), ['disk:/R/Проект', 'disk:/R/Проект/empty', 'disk:/R/Проект/sub',
                                        'disk:/R/Проект/sub/deep'])
        self.assertTrue(all('.Trashes' not in f['remote'] for f in files))

    def test_missing_source_reported(self):
        files, _, _, problems = uploader.scan_sources([self.tmp / 'нет'], 'disk:/R')
        self.assertEqual(files, [])
        self.assertEqual(len(problems), 1)

    def test_normalize_remote(self):
        self.assertEqual(uploader.normalize_remote('Заливка/Проект'), 'disk:/Заливка/Проект')
        self.assertEqual(uploader.normalize_remote(' /Заливка//Проект/ '), 'disk:/Заливка/Проект')
        self.assertEqual(uploader.normalize_remote('disk:/A/B'), 'disk:/A/B')
        self.assertEqual(uploader.normalize_remote('A\\B'), 'disk:/A/B')
        for bad in ('', '/', 'disk:/', 'A/../B'):
            with self.assertRaises(ValueError):
                uploader.normalize_remote(bad)

    def test_raise_for_classification(self):
        cases = [(401, uploader.FatalError), (507, uploader.FatalError), (403, uploader.FatalError),
                 (413, uploader.FileError), (429, uploader.TransientError), (503, uploader.TransientError),
                 (400, uploader.FileError)]
        for code, exc in cases:
            with self.assertRaises(exc, msg=code):
                uploader.raise_for(FakeResp(code, {'error': 'X'}), 'тест')
        with self.assertRaises(uploader.FatalError):
            uploader.raise_for(FakeResp(423, {'error': 'UPLOAD_TRAFFIC_LIMIT_EXCEEDED'}), 'тест')
        with self.assertRaises(uploader.TransientError):
            uploader.raise_for(FakeResp(423, {'error': 'DiskResourceLockedError'}), 'тест')
        with self.assertRaises(uploader.MissingParent):
            uploader.raise_for(FakeResp(409, {'error': 'DiskPathDoesntExistsError'}), 'тест')
        e = None
        try:
            uploader.raise_for(FakeResp(429, {}, {'Retry-After': '7'}), 'тест')
        except uploader.TransientError as ex:
            e = ex
        self.assertEqual(e.retry_after, 7)

    def test_real_session_ignores_system_proxy(self):
        os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7897'
        try:
            s = uploader.make_session()
            self.assertFalse(s.trust_env)
            self.assertEqual(s.proxies, {})
        finally:
            os.environ.pop('HTTPS_PROXY', None)

    def test_server_token_from_env_file(self):
        env = self.tmp / '.env'
        env.write_text('﻿OTHER=1\nYANDEX_DISK_TOKEN="abc123"\n', encoding='utf-8')
        os.environ['YDISK_ENV_FILE'] = str(env)
        self.assertEqual(uploader.load_server_token(), 'abc123')
        os.environ['YANDEX_DISK_TOKEN'] = 'from-env'
        self.assertEqual(uploader.load_server_token(), 'from-env')


class UploadTest(Base):
    def test_uploads_everything_and_publishes(self):
        src = self.project()
        q = self.add([src])
        snap = self.finish(q)
        self.assertEqual(snap['display'], 'done', snap['logs'])
        self.assertEqual(self.fake.files, {
            'disk:/Заливка/Тест/Проект/a.mp4': 3000,
            'disk:/Заливка/Тест/Проект/sub/b.mov': 5000,
            'disk:/Заливка/Тест/Проект/sub/deep/c.wav': 1000,
        })
        self.assertIn('disk:/Заливка/Тест/Проект/empty', self.fake.dirs)  # Пустые папки тоже переносятся
        self.assertEqual(snap['junk_skipped'], 3)
        self.assertEqual(snap['public_url'], self.fake.public['disk:/Заливка/Тест'])
        self.assertEqual(snap['overall_percent'], 100.0)
        # Токен уходит только в API, загрузчику он не нужен
        self.assertEqual(self.fake.api_tokens, {'OAuth ' + TOKEN})
        self.assertEqual(set(self.fake.uploader_auth), {None})

    def test_skips_files_already_there_with_same_size(self):
        src = self.project()
        self.fake.dirs.update({'disk:/Заливка', 'disk:/Заливка/Тест', 'disk:/Заливка/Тест/Проект',
                               'disk:/Заливка/Тест/Проект/sub'})
        self.fake.files['disk:/Заливка/Тест/Проект/a.mp4'] = 3000        # тот же размер — пропуск
        self.fake.files['disk:/Заливка/Тест/Проект/sub/b.mov'] = 123     # другой — заливаем заново
        uploader.LIST_LIMIT = 1  # Заодно проверяем постраничный список
        snap = self.finish(self.add([src]))
        self.assertEqual(snap['display'], 'done', snap['logs'])
        self.assertEqual(sorted(self.fake.put_attempts), ['disk:/Заливка/Тест/Проект/sub/b.mov',
                                                          'disk:/Заливка/Тест/Проект/sub/deep/c.wav'])
        self.assertEqual(snap['skipped_files'], 1)
        self.assertEqual(self.fake.files['disk:/Заливка/Тест/Проект/sub/b.mov'], 5000)

    def test_second_run_uploads_nothing(self):
        src = self.project()
        self.finish(self.add([src]))
        first = list(self.fake.put_attempts)
        snap = self.finish(self.add([src]))
        self.assertEqual(snap['display'], 'done')
        self.assertEqual(self.fake.put_attempts, first)
        self.assertEqual(snap['skipped_files'], 3)

    def test_largest_files_first(self):
        for name, size in (('s.bin', 100), ('l.bin', 9000), ('m.bin', 4000)):
            self.make('P/' + name, size)
        self.finish(self.add([self.tmp / 'P'], threads=1))
        self.assertEqual([p.rsplit('/', 1)[1] for p in self.fake.put_attempts], ['l.bin', 'm.bin', 's.bin'])

    def test_parallel_threads(self):
        for i in range(6):
            self.make(f'P/f{i}.bin', 2000 + i)
        self.fake.hold = threading.Event()
        q = self.add([self.tmp / 'P'], threads=3)
        self.wait(lambda: self.fake.in_flight == 3, 'три файла одновременно')
        time.sleep(0.1)
        self.assertEqual(self.fake.max_in_flight, 3)
        self.assertEqual(len(q.snapshot()['active']), 3)
        self.fake.hold.set()
        self.assertEqual(self.finish(q)['display'], 'done')
        self.assertEqual(len(self.fake.files), 6)

    def test_threads_change_on_the_fly(self):
        for i in range(5):
            self.make(f'P/f{i}.bin', 1000)
        self.fake.hold = threading.Event()
        q = self.add([self.tmp / 'P'], threads=1)
        self.wait(lambda: self.fake.in_flight == 1, 'один поток')
        self.assertIsNone(uploader.set_threads(q, 4))
        self.wait(lambda: self.fake.in_flight == 4, 'четыре потока после смены')
        self.assertIsNotNone(uploader.set_threads(q, 0))
        self.fake.hold.set()
        self.assertEqual(self.finish(q)['display'], 'done')

    def test_retries_transient_errors(self):
        self.make('P/a.bin', 2000)
        self.fake.fail_put['disk:/Заливка/Тест/P/a.bin'] = [503, 500]
        self.fake.fail_link['disk:/Заливка/Тест/P/a.bin'] = [429]
        snap = self.finish(self.add([self.tmp / 'P']))
        self.assertEqual(snap['display'], 'done', snap['logs'])
        self.assertEqual(self.fake.put_attempts.count('disk:/Заливка/Тест/P/a.bin'), 3)

    def test_gives_up_after_retries_without_publishing(self):
        self.make('P/bad.bin', 2000)
        self.make('P/good.bin', 1000)
        self.fake.fail_put['disk:/Заливка/Тест/P/bad.bin'] = [503] * uploader.FILE_RETRIES
        snap = self.finish(self.add([self.tmp / 'P']))
        self.assertEqual(snap['display'], 'problems')
        self.assertEqual(snap['failed_count'], 1)
        self.assertEqual(snap['failed'][0]['name'], 'disk:/Заливка/Тест/P/bad.bin')
        self.assertIn('disk:/Заливка/Тест/P/good.bin', self.fake.files)
        self.assertEqual(self.fake.public, {})
        # «Продолжить» доливает только то, что не залилось, и публикует
        q = uploader.uploads[0]
        self.assertIsNone(uploader.restart(q))
        snap = self.finish(q)
        self.assertEqual(snap['display'], 'done', snap['logs'])
        self.assertEqual(snap['skipped_files'], 1)
        self.assertTrue(snap['public_url'])

    def test_file_too_big_is_failed_others_continue(self):
        self.make('P/huge.bin', 3000)
        self.make('P/ok.bin', 1000)
        self.fake.fail_put['disk:/Заливка/Тест/P/huge.bin'] = [413]
        snap = self.finish(self.add([self.tmp / 'P']))
        self.assertEqual(snap['display'], 'problems')
        self.assertIn('тариф', snap['failed'][0]['error'])
        self.assertEqual(self.fake.put_attempts.count('disk:/Заливка/Тест/P/huge.bin'), 1)
        self.assertIn('disk:/Заливка/Тест/P/ok.bin', self.fake.files)

    def test_no_space_stops_whole_queue(self):
        for i in range(4):
            self.make(f'P/f{i}.bin', 1000 + i)
        self.fake.fail_put['disk:/Заливка/Тест/P/f3.bin'] = [507]
        snap = self.finish(self.add([self.tmp / 'P'], threads=1))
        self.assertEqual(snap['display'], 'problems')
        self.assertIn('места', snap['status_text'])
        self.assertEqual(self.fake.put_attempts, ['disk:/Заливка/Тест/P/f3.bin'])

    def test_recreates_missing_folder(self):
        self.make('P/a.bin', 100)
        self.fake.fail_link['disk:/Заливка/Тест/P/a.bin'] = [409]
        snap = self.finish(self.add([self.tmp / 'P']))
        self.assertEqual(snap['display'], 'done', snap['logs'])

    def test_no_token(self):
        self.make('P/a.bin', 100)
        q, error = uploader.add([str(self.tmp / 'P')], 'X', 4, None)
        self.assertIsNone(q)
        self.assertIn('токен', error)

    def test_bad_input(self):
        self.assertIn('Нет такого', uploader.add([str(self.tmp / 'нет')], 'X', 4, TOKEN)[1])
        self.make('P/a.bin', 100)
        self.assertIsNotNone(uploader.add([str(self.tmp / 'P')], '', 4, TOKEN)[1])
        self.assertIsNotNone(uploader.add([str(self.tmp / 'P')], 'X', 99, TOKEN)[1])

    def test_single_big_file_notice(self):
        self.make('P/big.mov', 5000)
        self.fake.hold = threading.Event()
        q = self.add([self.tmp / 'P'], threads=16)
        self.wait(lambda: self.fake.in_flight == 1, 'файл пошёл')
        self.assertIn('одним соединением', q.snapshot()['notice'])
        self.fake.hold.set()
        self.finish(q)


class ControlTest(Base):
    def files(self, n, prefix='P'):
        for i in range(n):
            self.make(f'{prefix}/f{i}.bin', 1000 + i)
        return self.tmp / prefix

    def test_soft_pause_and_resume(self):
        src = self.files(3)
        self.fake.hold = threading.Event()
        q = self.add([src], threads=1)
        self.wait(lambda: self.fake.in_flight == 1, 'первый файл')
        self.assertTrue(uploader.pause(q))
        self.assertIn('догружаются', q.snapshot()['notice'])
        self.fake.hold.set()
        self.wait(lambda: len(self.fake.files) == 1, 'начатый файл догрузился')
        time.sleep(0.2)
        self.assertEqual(len(self.fake.put_attempts), 1)  # Новые не начались
        self.assertEqual(q.snapshot()['display'], 'paused')
        self.assertTrue(uploader.resume(q))
        self.assertEqual(self.finish(q)['display'], 'done')
        self.assertEqual(len(self.fake.files), 3)

    def test_stop_aborts_and_restart_resumes(self):
        src = self.files(3)
        self.fake.hold = threading.Event()
        q = self.add([src], threads=2)
        self.wait(lambda: self.fake.in_flight == 2, 'два файла')
        self.assertTrue(uploader.stop(q))
        snap = self.finish(q)
        self.assertEqual(snap['display'], 'stopped')
        self.assertEqual(self.fake.files, {})
        self.assertEqual(self.fake.in_flight, 0)
        self.assertNotIn(q.id, [e['id'] for e in app.load_session().get('uploads', [])])
        self.fake.hold.set()
        self.assertIsNone(uploader.restart(q))
        self.assertEqual(self.finish(q)['display'], 'done')
        self.assertEqual(len(self.fake.files), 3)

    def test_move_to_top_pauses_others(self):
        a = self.files(3, 'A')
        b = self.files(3, 'B')
        self.fake.hold = threading.Event()
        q1 = self.add([a], 'R1', threads=1)
        q2 = self.add([b], 'R2', threads=1)
        self.wait(lambda: self.fake.in_flight == 2, 'обе заливки идут')
        self.assertIsNone(uploader.move_to_top(q2))
        self.assertIs(uploader.uploads[0], q2)
        self.assertTrue(q1.state['is_paused'])
        self.assertFalse(q2.state['is_paused'])
        self.fake.hold.set()
        self.assertEqual(self.finish(q2)['display'], 'done')
        self.assertEqual(q1.snapshot()['display'], 'paused')
        uploader.resume(q1)
        self.assertEqual(self.finish(q1)['display'], 'done')

    def test_top_resumes_paused_queue(self):
        src = self.files(2)
        self.fake.hold = threading.Event()
        q = self.add([src], threads=1)
        uploader.pause(q)
        uploader.move_to_top(q)
        self.assertFalse(q.state['is_paused'])
        self.fake.hold.set()
        self.finish(q)

    def test_duplicate_running_upload_rejected(self):
        src = self.files(1)
        self.fake.hold = threading.Event()
        self.add([src])
        q, error = uploader.add([str(src)], 'Заливка/Тест', 4, TOKEN)
        self.assertIsNone(q)
        self.assertIn('уже идёт', error)

    def test_quit_keeps_upload_and_restore_continues(self):
        src = self.files(3)
        self.fake.hold = threading.Event()
        q = self.add([src], threads=1)
        self.wait(lambda: self.fake.in_flight == 1, 'заливка идёт')
        uploader.stop_all_for_quit()
        self.finish(q)
        entries = app.load_session().get('uploads')
        self.assertEqual([e['id'] for e in entries], [q.id])
        self.assertNotIn(TOKEN, json.dumps(app.load_session(), ensure_ascii=False))
        # «Перезапуск программы»: токен берётся с сервера
        uploader.uploads.clear()
        uploader.flags['quitting'] = False
        os.environ['YANDEX_DISK_TOKEN'] = TOKEN
        self.fake.hold.set()
        restored = uploader.restore(entries)
        self.assertEqual(len(restored), 1)
        self.assertEqual(self.finish(restored[0])['display'], 'done')
        self.assertEqual(len(self.fake.files), 3)
        self.assertEqual(app.load_session().get('uploads'), [])

    def test_restore_without_token_or_source_is_blocked(self):
        entries = [{'id': 'a1', 'sources': [str(self.tmp / 'нет')], 'remote': 'disk:/R', 'threads': 4},
                   {'id': 'b2', 'sources': [str(self.files(1))], 'remote': 'disk:/R', 'threads': 4}]
        restored = uploader.restore(entries)
        self.assertEqual([q.snapshot()['display'] for q in restored], ['blocked', 'blocked'])
        self.assertIn('токен', restored[1].state['status_text'])


class ApiTest(Base):
    def setUp(self):
        super().setUp()

        class QuietHandler(app.RequestHandler):
            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), QuietHandler)
        self.server.daemon_threads = True
        port = self.server.server_address[1]
        self.base = f'http://127.0.0.1:{port}'
        self.real_hosts = app.ALLOWED_HOSTS
        app.ALLOWED_HOSTS = {f'127.0.0.1:{port}'}
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.http = requests.Session()
        self.http.trust_env = False

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        app.ALLOWED_HOSTS = self.real_hosts
        super().tearDown()

    def post(self, path, **data):
        return self.http.post(self.base + path, json=data, timeout=5).json()

    def test_start_status_and_controls(self):
        self.make('P/a.bin', 1000)
        self.make('P/b.bin', 2000)
        self.fake.hold = threading.Event()
        res = self.post('/api/upload/start', sources=[str(self.tmp / 'P')], remote='Заливка/Апи', threads=2, token=TOKEN)
        self.assertEqual(res['status'], 'started', res)
        self.wait(lambda: self.fake.in_flight == 2, 'заливка идёт')
        status = self.http.get(self.base + '/api/status', timeout=5)
        self.assertNotIn(TOKEN, status.text)
        up = status.json()['uploads'][0]
        self.assertEqual(up['id'], res['id'])
        self.assertEqual(up['display'], 'uploading')
        self.assertEqual(self.post('/api/upload/threads', id=res['id'], threads=5)['status'], 'ok')
        self.assertEqual(self.post('/api/upload/pause', id=res['id'])['status'], 'ok')
        self.assertEqual(self.post('/api/upload/resume', id=res['id'])['status'], 'ok')
        self.assertEqual(self.post('/api/upload/top', id=res['id'])['status'], 'ok')
        self.assertEqual(self.post('/api/upload/remove', id=res['id'])['status'], 'error')  # ещё идёт
        self.assertEqual(self.post('/api/upload/pause', id='нет')['status'], 'error')
        self.fake.hold.set()
        q = uploader.find(res['id'])
        self.finish(q)
        self.assertEqual(self.post('/api/upload/remove', id=res['id'])['status'], 'ok')
        defaults = self.http.get(self.base + '/api/default_dest', timeout=5).json()
        self.assertEqual(defaults['upload_remote'], 'Заливка/Апи')
        self.assertEqual(defaults['upload_threads'], 2)
        self.assertFalse(defaults['upload_has_server_token'])

    def test_start_errors(self):
        res = self.post('/api/upload/start', sources=[], remote='X', threads=2, token=TOKEN)
        self.assertEqual(res['status'], 'error')
        self.make('P/a.bin', 10)
        res = self.post('/api/upload/start', sources=[str(self.tmp / 'P')], remote='X', threads=2)
        self.assertEqual(res['status'], 'error')
        self.assertIn('токен', res['message'])

    def test_page_has_upload_tab(self):
        page = self.http.get(self.base + '/', timeout=5).text
        self.assertIn('Залить на Яндекс Диск', page)
        self.assertIn('/api/upload/start', page)


class RealHttpTest(Base):
    """Настоящий requests против локального сервера: Content-Length вместо chunked и обрыв по «Стопу»."""

    def setUp(self):
        super().setUp()
        uploader.hooks['session'] = self.real_session
        self.got = {}
        self.slow = threading.Event()
        test = self

        from http.server import BaseHTTPRequestHandler

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                body = json.dumps({'href': f'http://127.0.0.1:{test.port}/put', 'method': 'PUT'}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self):
                test.got['length'] = self.headers.get('Content-Length')
                test.got['chunked'] = self.headers.get('Transfer-Encoding')
                test.got['auth'] = self.headers.get('Authorization')
                left = int(self.headers['Content-Length'])
                data = b''
                while left:
                    if test.slow.is_set():
                        time.sleep(0.05)
                    chunk = self.rfile.read(min(left, 4096))
                    if not chunk:
                        return
                    data += chunk
                    left -= len(chunk)
                test.got['size'] = len(data)
                self.send_response(201)
                self.send_header('Content-Length', '0')
                self.end_headers()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.real_api = uploader.API
        uploader.API = f'http://127.0.0.1:{self.port}/v1/disk'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        uploader.API = self.real_api
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def item(self, size):
        p = self.make('real.bin', size)
        return {'local': str(p), 'remote': 'disk:/R/real.bin', 'name': 'real.bin', 'size': size, 'sent': 0, 'started': 0}

    def test_put_has_content_length(self):
        q = uploader.UploadQueue([], 'disk:/R', 1, TOKEN)
        item = self.item(3 * 1024 * 1024 + 5)
        self.assertEqual(uploader.upload_one(q, item, uploader.make_session()), 'done')
        self.assertEqual(self.got['length'], str(item['size']))
        self.assertIsNone(self.got['chunked'])
        self.assertIsNone(self.got['auth'])
        self.assertEqual(self.got['size'], item['size'])
        self.assertEqual(item['sent'], item['size'])

    def test_stop_breaks_running_put(self):
        q = uploader.UploadQueue([], 'disk:/R', 1, TOKEN)
        item = self.item(64 * 1024 * 1024)
        self.slow.set()
        result = {}
        t = threading.Thread(target=lambda: result.update(r=uploader.upload_one(q, item, uploader.make_session())))
        t.start()
        self.wait(lambda: item['sent'] > 0, 'отправка началась')
        q.abort.set()
        t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(result['r'], 'stopped')
        self.assertLess(item['sent'], item['size'])


if __name__ == '__main__':
    unittest.main(verbosity=1)
