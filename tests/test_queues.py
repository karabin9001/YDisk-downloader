#!/usr/bin/env python3
"""Проверка нескольких очередей на локальной имитации Google: параллельная работа, пауза,
слоты, восстановление после перезапуска, старый session.json и «Повторить сейчас».

Запуск: python3 tests/test_queues.py
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_gdrive import FakeGoogle, app, blob, reset_app  # noqa: E402  Задаёт отдельный YDISK_CONFIG_DIR

import requests  # noqa: E402

GDRIVE = 'https://drive.google.com/drive/folders/'
ROOT_DIR = str(Path(__file__).resolve().parent.parent)


class Base(unittest.TestCase):
    def setUp(self):
        self.google = FakeGoogle()
        self.google.start()
        self.tmp = Path(tempfile.mkdtemp(prefix='ydisk-test-'))
        self.a = self.tmp / 'A'
        self.b = self.tmp / 'B'
        self.c = self.tmp / 'C'
        app.BACKOFF_BASE = 0.05
        app.BACKOFF_MAX = 0.2
        app.QUOTA_RETRY = 3600   # Сам файл не вернётся: проверяем только ручной повтор
        app.DEFER_RETRY = 3600
        app.RESCAN_INTERVAL = 3600
        app.BUSY_RETRY = 0.1
        app.CHUNK_SIZE = 64 * 1024
        reset_app()

    def tearDown(self):
        for event in self.google.holds.values():
            event.set()
        reset_app()
        self.google.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def folder(self, name, size=100_000):
        """Папка на Google из одного файла name.mp4."""
        self.google.files[name] = blob(name, size)
        self.google.folders[name] = (name, [(name, f'{name}.mp4', 'file')])
        return self.google.files[name]

    def add(self, name, dest):
        q = app.add_queue([GDRIVE + name], dest, flat=True)
        self.assertIsNotNone(q)
        return q

    def hold(self, name):
        self.google.holds[name] = threading.Event()
        return self.google.holds[name]

    def wait(self, check, what, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if check():
                return
            time.sleep(0.02)
        logs = "\n".join(line for q in app.queues for line in q.state['logs'])
        self.fail(f"Не дождались: {what}. Состояния: {[q.display_status() for q in app.queues]}\n{logs}")

    def finish(self, *items):
        for q in items:
            q.thread.join(30)
            self.assertFalse(q.thread.is_alive(), q.state['logs'])

    def assert_file(self, dest, name):
        self.assertEqual((dest / f'{name}.mp4').read_bytes(), self.google.files[name])


class ParallelQueues(Base):
    def test_two_queues_download_at_once_into_own_folders(self):
        self.folder('one')
        self.folder('two')
        release_one, release_two = self.hold('one'), self.hold('two')
        q1 = self.add('one', self.a)
        q2 = self.add('two', self.b)
        # Сервер держит оба файла: значит, обе очереди качают одновременно
        self.wait(lambda: self.google.in_flight == {'one', 'two'}, "обе очереди начали качать")
        self.assertEqual(q1.display_status(), 'downloading')
        self.assertEqual(q2.display_status(), 'downloading')
        release_one.set()
        release_two.set()
        self.finish(q1, q2)
        self.assert_file(self.a, 'one')
        self.assert_file(self.b, 'two')
        self.assertFalse((self.a / 'two.mp4').exists())
        self.assertFalse((self.b / 'one.mp4').exists())
        # Прогресс и журнал у каждой очереди свои
        self.assertEqual(q1.state['total_bytes'], 100_000)
        self.assertEqual(q2.state['total_files'], 1)
        self.assertFalse(any('two.mp4' in line for line in q1.state['logs']))
        self.assertFalse(any('one.mp4' in line for line in q2.state['logs']))

    def test_pause_of_one_queue_does_not_hold_the_other(self):
        self.folder('slow', 3_000_000)
        self.folder('other', 2_000_000)
        self.google.slow['slow'] = 0.02
        self.google.slow['other'] = 0.02
        q1 = self.add('slow', self.a)
        q2 = self.add('other', self.b)
        self.wait(lambda: q1.state['downloaded_bytes'] > 200_000, "первая очередь начала качать")
        self.assertTrue(app.pause_queue(q1))
        self.assertEqual(q1.display_status(), 'paused')
        self.wait(lambda: self.google.requests and (self.a / 'slow.mp4.part').exists(), "кусок на диске")
        time.sleep(0.3)  # Поток дочитывает начатый кусок
        frozen = q1.state['downloaded_bytes']
        self.finish(q2)
        self.assert_file(self.b, 'other')
        self.assertEqual(q2.state['status_text'], 'Загрузка завершена!')
        self.assertEqual(q1.state['downloaded_bytes'], frozen)
        self.assertTrue(q1.state['is_running'])
        self.assertFalse((self.a / 'slow.mp4').exists())
        self.google.slow.clear()
        self.assertTrue(app.resume_queue(q1))
        self.finish(q1)
        self.assert_file(self.a, 'slow')
        starts = [r[1] for r in self.google.requests if r[0] == 'slow']
        self.assertGreater(starts[-1], 0, "После паузы файл докачивается, а не качается заново")

    def test_stop_of_one_queue_keeps_the_other(self):
        self.folder('one')
        self.folder('two')
        hold_one, hold_two = self.hold('one'), self.hold('two')
        q1 = self.add('one', self.a)
        q2 = self.add('two', self.b)
        self.wait(lambda: self.google.in_flight == {'one', 'two'}, "обе очереди начали качать")
        app.stop_queue(q1)
        self.assertEqual([e['id'] for e in app.load_session()['queues']], [q2.id])
        hold_one.set()
        self.finish(q1)
        self.assertEqual(q1.display_status(), 'stopped')
        self.assertTrue(q2.state['is_running'])
        hold_two.set()
        self.finish(q2)
        self.assert_file(self.b, 'two')

    def test_same_file_from_two_links_has_one_writer(self):
        data = self.folder('same', 500_000)
        self.google.folders['copy'] = ('copy', [('same', 'same.mp4', 'file')])
        release = self.hold('same')
        q1 = self.add('same', self.a)
        self.wait(lambda: 'same' in self.google.in_flight, "файл качается")
        q2 = self.add('copy', self.a)
        self.wait(lambda: q2.state['folders'] and q2.state['folders'][0]['deferred'] == 1, "вторая ждёт первую")
        self.assertIn('другая очередь', q2.state['folders'][0]['files'][0]['note'])
        self.assertEqual(q2.display_status(), 'deferred')
        release.set()
        self.finish(q1, q2)
        self.assertEqual((self.a / 'same.mp4').read_bytes(), data)
        self.assertEqual(len(self.google.requests), 1, "Файл скачан один раз")
        self.assertEqual(q2.state['completed_list'][0]['status'], 'Уже был')


class Slots(Base):
    def test_extra_queues_wait_in_order_of_adding(self):
        app.set_max_active(1)
        for name in ('one', 'two', 'three'):
            self.folder(name)
        release = self.hold('one')
        q1 = self.add('one', self.a)
        self.wait(lambda: 'one' in self.google.in_flight, "первая начала качать")
        q2 = self.add('two', self.b)
        q3 = self.add('three', self.c)
        self.wait(lambda: q3.display_status() == 'waiting' and q2.display_status() == 'waiting', "две ждут слот")
        time.sleep(0.3)
        self.assertEqual([r[0] for r in self.google.requests], ['one'])
        release.set()
        self.finish(q1, q2, q3)
        self.assertEqual([r[0] for r in self.google.requests], ['one', 'two', 'three'])

    def test_more_slots_let_waiting_queue_start(self):
        app.set_max_active(1)
        self.folder('one')
        self.folder('two')
        release = self.hold('one')
        self.hold('two').set()
        q1 = self.add('one', self.a)
        self.wait(lambda: 'one' in self.google.in_flight, "первая начала качать")
        q2 = self.add('two', self.b)
        self.wait(lambda: q2.display_status() == 'waiting', "вторая ждёт слот")
        app.set_max_active(2)
        self.finish(q2)
        self.assert_file(self.b, 'two')
        release.set()
        self.finish(q1)

    def test_queue_waiting_for_limit_gives_up_its_slot(self):
        app.set_max_active(1)
        self.folder('limited')
        self.folder('free')
        self.google.real_quota.add('limited')
        q1 = self.add('limited', self.a)
        self.wait(lambda: q1.display_status() == 'limit', "первая ждёт лимит")
        self.assertFalse(q1.has_slot)
        q2 = self.add('free', self.b)
        self.finish(q2)
        self.assert_file(self.b, 'free')
        self.assertEqual(q1.display_status(), 'limit')

    def test_paused_queue_gives_up_its_slot_and_takes_turn_after_resume(self):
        app.set_max_active(1)
        self.folder('one', 2_000_000)
        self.folder('two')
        self.google.slow['one'] = 0.02
        q1 = self.add('one', self.a)
        self.wait(lambda: q1.state['downloaded_bytes'] > 100_000, "первая качает")
        release = self.hold('two')
        q2 = self.add('two', self.b)
        self.wait(lambda: q2.display_status() == 'waiting', "вторая ждёт слот")
        app.pause_queue(q1)
        self.wait(lambda: 'two' in self.google.in_flight, "вторая получила слот")
        app.resume_queue(q1)
        self.wait(lambda: q1.display_status() == 'waiting', "первая ждёт слот после паузы")
        self.assertFalse(q1.has_slot)
        self.google.slow.clear()
        release.set()
        self.finish(q2, q1)
        self.assert_file(self.a, 'one')


class RetryNow(Base):
    def limited(self):
        self.folder('limited', 300_000)
        self.google.real_quota.add('limited')
        q = self.add('limited', self.a)
        self.wait(lambda: q.display_status() == 'limit', "файл отложен по лимиту")
        return q

    def file_entry(self, q):
        return q.state['folders'][0]['files'][0]

    def test_probe_says_nothing_real_download_decides(self):
        q = self.limited()
        # Как у настоящего Google: короткая проба проходит, хотя лимит действует
        self.assertEqual(app.gdrive_probe('limited')[0], 300_000)
        self.assertEqual(q.state['total_bytes'], 300_000)
        self.assertIn('настоящим скачиванием', q.state['status_text'])
        self.assertEqual(q.state['file_percent'], 0.0)
        self.assertEqual(q.state['current_file'], '')
        self.assertFalse(any('снят' in line for line in q.state['logs']))

    def test_retry_runs_a_real_download_and_keeps_schedule_when_limit_holds(self):
        q = self.limited()
        scheduled = q.deferred[0]['next_try']
        tries = len(self.google.requests)
        self.assertEqual(app.retry_deferred(q), 1)
        self.assertIn('Повтор запрошен', self.file_entry(q)['note'])
        self.wait(lambda: len(self.google.requests) > tries and q.deferred
                  and q.display_status() == 'limit', "повтор отклонён")
        self.assertEqual(q.deferred[0]['next_try'], scheduled, "Ручной повтор не меняет расписание")
        self.assertEqual(q.deferred[0]['defers'], 1)
        self.assertIn('скачивание отклонено в', self.file_entry(q)['note'])
        self.assertFalse(any('снят' in line for line in q.state['logs']))
        self.assertEqual(self.file_entry(q)['status'], 'deferred')

    def test_retry_finishes_file_when_limit_is_gone(self):
        q = self.limited()
        self.google.real_quota.clear()
        self.assertEqual(app.retry_deferred(q), 1)
        self.finish(q)
        self.assert_file(self.a, 'limited')
        self.assertEqual(q.state['status_text'], 'Загрузка завершена!')
        self.assertTrue(any('лимит Google снят' in line for line in q.state['logs']))

    def test_quota_is_not_bypassed_with_short_ranges(self):
        q = self.limited()
        for _ in range(3):
            tries = len(self.google.requests)
            app.retry_deferred(q)
            self.wait(lambda: len(self.google.requests) > tries and q.deferred, "повтор отклонён")
        self.google.real_quota.clear()
        app.retry_deferred(q)
        self.finish(q)
        for file_id, start, length in self.google.requests:
            self.assertEqual(start + length, 300_000, "Запрос должен просить файл до конца")

    def test_retry_overtakes_files_not_started_yet(self):
        self.google.files.update({'lim': blob('lim', 2000), 'x': blob('x', 2000), 'y': blob('y', 2000)})
        self.google.folders['mix'] = ('mix', [('lim', 'lim.mp4', 'file'), ('x', 'x.mp4', 'file'), ('y', 'y.mp4', 'file')])
        self.google.real_quota.add('lim')
        release = self.hold('x')
        q = self.add('mix', self.a)
        self.wait(lambda: 'x' in self.google.in_flight, "качается второй файл")
        self.assertEqual(q.state['folders'][0]['deferred'], 1)
        self.google.real_quota.clear()
        app.retry_deferred(q)
        time.sleep(0.05)
        release.set()
        self.finish(q)
        self.assertEqual([r[0] for r in self.google.requests], ['lim', 'x', 'lim', 'y'])

    def test_nothing_to_retry(self):
        self.folder('one')
        release = self.hold('one')
        q = self.add('one', self.a)
        self.wait(lambda: 'one' in self.google.in_flight, "файл качается")
        self.assertEqual(app.retry_deferred(q), 0)
        release.set()


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
        return self.http.post(self.base + path, json=data, timeout=10)

    def status(self):
        return self.http.get(self.base + '/api/status', timeout=10).json()

    def start(self, name, dest, **extra):
        return self.post('/api/start', urls=[GDRIVE + name], dest=str(dest), flat=True, **extra)

    def test_second_link_during_limit_gets_own_card(self):
        self.folder('rome')
        self.folder('optika')
        self.google.real_quota.add('rome')
        rome = self.start('rome', self.a).json()['id']
        self.wait(lambda: app.find_queue(rome).display_status() == 'limit', "первая ждёт лимит")
        answer = self.start('optika', self.b)
        self.assertEqual(answer.status_code, 200, answer.text)
        optika = answer.json()['id']
        app.find_queue(optika).thread.join(30)
        cards = {q['id']: q for q in self.status()['queues']}
        self.assertEqual(len(cards), 2)
        self.assertEqual(cards[rome]['display'], 'limit')
        self.assertEqual(cards[rome]['dest'], str(self.a.resolve()))
        self.assertEqual(cards[rome]['deferred_files'], 1)
        self.assertEqual(cards[optika]['display'], 'done')
        self.assertEqual(cards[optika]['dest'], str(self.b.resolve()))
        self.assertNotEqual(cards[rome]['logs'], cards[optika]['logs'])
        self.assert_file(self.b, 'optika')

    def test_buttons_address_one_queue(self):
        self.folder('limited')
        self.google.real_quota.add('limited')
        qid = self.start('limited', self.a).json()['id']
        q = app.find_queue(qid)
        self.wait(lambda: q.display_status() == 'limit', "файл отложен")

        self.assertEqual(self.post('/api/pause', id=qid).json()['status'], 'ok')
        refused = self.post('/api/retry', id=qid)
        self.assertEqual(refused.status_code, 400)
        self.assertIn('Продолжить', refused.json()['message'])
        self.assertEqual(self.post('/api/resume', id=qid).json()['status'], 'ok')
        self.assertEqual(self.post('/api/retry', id=qid).json()['status'], 'ok')

        self.assertEqual(self.post('/api/pause', id='нет такой').status_code, 404)
        self.assertEqual(self.post('/api/remove', id=qid).status_code, 400)  # Ещё работает
        self.assertEqual(self.post('/api/stop', id=qid).json()['status'], 'ok')
        q.thread.join(10)
        self.google.real_quota.clear()
        self.assertEqual(self.post('/api/restart', id=qid).json()['status'], 'ok')
        q.thread.join(20)
        self.assert_file(self.a, 'limited')
        self.assertEqual(self.post('/api/remove', id=qid).json()['status'], 'ok')
        self.assertEqual(self.status()['queues'], [])

    def test_same_link_into_same_folder_is_refused(self):
        self.folder('one')
        release = self.hold('one')
        self.assertEqual(self.start('one', self.a).status_code, 200)
        self.assertEqual(self.start('one', self.a).status_code, 400)
        release.set()

    def test_token_is_never_shown_or_saved(self):
        self.folder('one')
        answer = self.start('one', self.a, token='секретный-токен')
        q = app.find_queue(answer.json()['id'])
        text = self.http.get(self.base + '/api/status', timeout=10).text
        q.thread.join(20)
        self.assertNotIn('секретный-токен', text)
        self.assertNotIn('секретный-токен', app.session_path().read_text(encoding='utf-8'))

    def test_settings(self):
        self.assertEqual(self.post('/api/settings', max_active=3).json()['max_active'], 3)
        self.assertEqual(self.status()['max_active'], 3)
        self.assertEqual(app.load_session()['max_active'], 3)
        for bad in (0, app.MAX_ACTIVE_LIMIT + 1, 'два', True):
            self.assertEqual(self.post('/api/settings', max_active=bad).status_code, 400)

    def test_page_opens(self):
        page = self.http.get(self.base + '/', timeout=10).text
        self.assertIn('Скачать', page)
        self.assertIn('Повторить отложенные', page)


class RestoreAfterRestart(Base):
    def restart_program(self):
        """Как выключение и новый запуск: состояние в памяти пропадает, остаётся session.json."""
        app.stop_all_for_quit()
        with app.slot_changed:
            app.slot_changed.notify_all()
        for event in self.google.holds.values():
            event.set()
        for q in list(app.queues):
            q.thread.join(10)
        self.google.holds.clear()
        self.google.in_flight.clear()
        app.queues.clear()
        app.busy_paths.clear()
        app.app_state.update(quitting=False, max_active=app.DEFAULT_MAX_ACTIVE)
        app.resume_unfinished()

    def test_each_queue_comes_back_with_own_folder_and_order(self):
        app.set_max_active(1)
        self.folder('one', 500_000)
        self.folder('two')
        self.folder('three')
        self.google.slow['one'] = 0.02
        q1 = self.add('one', self.a)
        self.wait(lambda: q1.state['downloaded_bytes'] > 100_000, "первая качает")
        q2 = self.add('two', self.b)
        q3 = self.add('three', self.c)
        self.wait(lambda: q3.display_status() == 'waiting', "остальные ждут слот")
        ids = [q1.id, q2.id, q3.id]

        self.restart_program()
        self.assertEqual(app.app_state['max_active'], 1)
        self.assertEqual([q.id for q in app.queues], ids)
        self.assertEqual([q.dest for q in app.queues], [self.a, self.b, self.c])
        self.google.slow.clear()
        self.finish(*app.queues)
        self.assert_file(self.a, 'one')
        self.assert_file(self.b, 'two')
        self.assert_file(self.c, 'three')
        self.assertGreater([r[1] for r in self.google.requests if r[0] == 'one'][-1], 0, "Докачка, а не заново")
        self.assertEqual(app.load_session()['queues'], [])

    def test_paused_queue_stays_paused_and_lets_others_go(self):
        app.set_max_active(1)
        self.folder('one', 500_000)
        self.folder('two')
        self.google.slow['one'] = 0.02
        q1 = self.add('one', self.a)
        self.wait(lambda: q1.state['downloaded_bytes'] > 100_000, "первая качает")
        app.pause_queue(q1)
        self.hold('two')
        self.add('two', self.b)
        self.wait(lambda: 'two' in self.google.in_flight, "вторая качает")
        self.google.requests.clear()

        self.restart_program()
        restored1, restored2 = app.queues
        self.assertTrue(restored1.state['is_paused'])
        self.assertIn('паузе', restored1.state['status_text'])
        self.finish(restored2)
        self.assertEqual({r[0] for r in self.google.requests}, {'two'})
        self.assertEqual(restored1.display_status(), 'paused')
        self.google.slow.clear()
        app.resume_queue(restored1)
        self.finish(restored1)
        self.assert_file(self.a, 'one')

    def test_stopped_queue_does_not_come_back(self):
        self.folder('one')
        self.folder('two')
        self.hold('one')
        self.hold('two')
        q1 = self.add('one', self.a)
        q2 = self.add('two', self.b)
        self.wait(lambda: self.google.in_flight == {'one', 'two'}, "обе качают")
        app.stop_queue(q1)
        self.restart_program()
        self.assertEqual([q.id for q in app.queues], [q2.id])
        self.finish(*app.queues)

    def test_killed_process_restores_queues(self):
        """Настоящий процесс на отдельном порту: kill -9 посреди двух загрузок и новый запуск."""
        self.folder('one', 3_000_000)
        self.folder('two', 3_000_000)
        self.google.slow.update(one=0.02, two=0.02)
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        config = self.tmp / 'config'
        config.mkdir()
        env = dict(os.environ, YDISK_PORT=str(port), YDISK_CONFIG_DIR=str(config), BROWSER='true')
        launcher = (f"import sys; sys.path.insert(0, {ROOT_DIR!r}); import app; app.CHUNK_SIZE = 64 * 1024; "
                    f"app.GDRIVE_LIST_URL = {self.google_url('list')!r}; "
                    f"app.GDRIVE_DOWNLOAD_URL = {self.google_url('download')!r}; app.run_server()")
        http = requests.Session()
        http.trust_env = False
        base = f'http://127.0.0.1:{port}'

        def launch():
            process = subprocess.Popen([sys.executable, '-c', launcher], env=env, cwd=self.tmp,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.addCleanup(process.kill)
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    http.get(base + '/api/status', timeout=1)
                    return process
                except requests.RequestException:
                    time.sleep(0.05)
            self.fail("Программа не запустилась")

        def queues():
            return http.get(base + '/api/status', timeout=5).json()['queues']

        process = launch()
        for name, dest in (('one', self.a), ('two', self.b)):
            answer = http.post(base + '/api/start', json={'urls': [GDRIVE + name], 'dest': str(dest), 'flat': True}, timeout=5)
            self.assertEqual(answer.status_code, 200, answer.text)
        self.wait(lambda: all(q['downloaded_bytes'] > 200_000 for q in queues()), "обе качают")
        process.kill()
        process.wait(5)
        saved = json.loads((config / 'session.json').read_text(encoding='utf-8'))
        self.assertEqual(len(saved['queues']), 2)

        self.google.slow.clear()
        launch()
        self.wait(lambda: [q['display'] for q in queues()] == ['done', 'done'], "обе докачались", 30)
        self.assert_file(self.a, 'one')
        self.assert_file(self.b, 'two')
        self.assertEqual(json.loads((config / 'session.json').read_text(encoding='utf-8'))['queues'], [])

    def google_url(self, path):
        return f'http://127.0.0.1:{self.google.port}/{path}'


class SessionMigration(Base):
    def write_session(self, **data):
        app.session_path().parent.mkdir(parents=True, exist_ok=True)
        app.session_path().write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')

    def test_old_record_with_one_queue_is_picked_up(self):
        self.folder('old')
        self.write_session(urls=[GDRIVE + 'old'], dest=str(self.a), proxy='', flat=True, active=True)
        app.resume_unfinished()
        self.assertEqual(len(app.queues), 1)
        q = app.queues[0]
        self.assertEqual(q.dest, self.a)
        self.assertTrue(q.flat)
        saved = app.load_session()
        self.assertNotIn('active', saved)
        self.assertNotIn('urls', saved)
        self.assertEqual(saved['dest'], str(self.a), "Папка в форме остаётся")
        self.finish(q)
        self.assert_file(self.a, 'old')
        self.assertEqual(app.load_session()['queues'], [])

    def test_real_shape_of_current_record(self):
        """Запись как в рабочем session.json с Римом: прокси и папка сохраняются."""
        proxy = 'socks5h://127.0.0.1:1080'
        entries = app.session_queue_entries({'urls': [GDRIVE + 'x'], 'dest': '/Volumes/KINGSTON/Рим/01_Исходники',
                                             'proxy': proxy, 'flat': True, 'active': True})
        self.assertEqual(entries, [{'urls': [GDRIVE + 'x'], 'dest': '/Volumes/KINGSTON/Рим/01_Исходники',
                                    'proxy': proxy, 'flat': True}])

    def test_finished_old_record_starts_nothing(self):
        self.write_session(urls=[GDRIVE + 'old'], dest=str(self.a), proxy='', flat=False, active=False)
        app.resume_unfinished()
        self.assertEqual(app.queues, [])
        self.assertEqual(app.load_session()['queues'], [])

    def test_damaged_entries_are_skipped(self):
        self.folder('good')
        self.write_session(queues=[{'urls': 'строка', 'dest': 'x'}, 'мусор', {'urls': [], 'dest': 'x'},
                                   {'urls': [GDRIVE + 'good'], 'dest': str(self.a), 'flat': True}])
        app.resume_unfinished()
        self.assertEqual(len(app.queues), 1)
        self.finish(app.queues[0])
        self.assert_file(self.a, 'good')

    def test_missing_disk_keeps_queue_until_it_returns(self):
        self.folder('old')
        blocker = self.tmp / 'not-a-folder'
        blocker.write_text('x')
        dest = blocker / 'disk'
        self.write_session(urls=[GDRIVE + 'old'], dest=str(dest), proxy='', flat=True, active=True)
        app.resume_unfinished()
        q = app.queues[0]
        self.assertEqual(q.display_status(), 'blocked')
        self.assertIsNotNone(app.restart_queue(q))
        # Сохранение других очередей не должно потерять эту запись
        self.folder('fresh')
        other = self.add('fresh', self.b)
        self.finish(other)
        self.assertEqual([e['dest'] for e in app.load_session()['queues']], [str(dest)])
        blocker.unlink()
        self.assertIsNone(app.restart_queue(q))
        self.finish(q)
        self.assert_file(dest, 'old')


if __name__ == '__main__':
    unittest.main(verbosity=2)
