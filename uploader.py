# -*- coding: utf-8 -*-
"""
Заливка локальных папок и файлов на Яндекс.Диск через REST API.

Как устроено (по замерам 18.11.2026 и документации Яндекса):
- Файл заливается так: GET /v1/disk/resources/upload → ссылка → один PUT с файлом на эту ссылку.
- Яндекс режет скорость заливки по РАСШИРЕНИЮ файла (замер 19.11.2026 с сервера, порт 100 Мбит):
  .mp4 и .bin — короткий рывок ~30 МБ, потом около 1 Мбит/с; .txt — 89 Мбит/с, упор в порт.
  Поэтому каждый файл (кроме .txt) льётся под временным именем «<имя>.ydupload.txt» и сразу
  после заливки переименовывается на Диске (POST /resources/move). Видео после переименования
  Яндекс видит как обычное видео (mime video/mp4).
- Параллельные PUT разных файлов складываются: файлы льются в N потоков (по умолчанию 16),
  самые крупные первыми. На файл выдаётся одна ссылка: один файл идёт одним соединением.
- Яндексу прокси не нужен и только тормозит: сессии всегда идут напрямую (trust_env=False).

Токен в журнал, в ответы интерфейса и в session.json не попадает.
"""

import os
import threading
import time
import secrets
from collections import deque
from pathlib import Path

import requests

API = 'https://cloud-api.yandex.net/v1/disk'
DEFAULT_THREADS = 16
MAX_THREADS = 32
FILE_RETRIES = 6            # попыток на файл, потом он считается неудавшимся
META_RETRIES = 8            # попыток на служебные запросы: список папки, создание папки, публикация
RETRY_BASE = 3              # первая пауза перед повтором, сек
RETRY_MAX = 120
API_TIMEOUT = 30
PUT_TIMEOUT = (30, 900)     # соединение; ожидание ответа после отправки файла (Яндекс может долго «переваривать» большой файл)
READ_BLOCK = 256 * 1024
LIST_LIMIT = 1000
SPEED_WINDOW = 15           # за сколько секунд считать скорость
LOG_LIMIT = 300
FAST_SUFFIX = '.ydupload.txt'  # временное имя на время заливки: .txt Яндекс не притормаживает
MOVE_WAIT = 120             # сколько ждать, пока свежезалитый файл появится на Диске для переименования
ORDER_MAX = 64              # пометка заказа: по ней Пульт показывает заливку в карточке заказа

# Мусор macOS и Windows: на Диске он не нужен
JUNK_NAMES = {'.DS_Store', '.localized', 'Thumbs.db', 'desktop.ini', 'Icon\r'}
JUNK_DIRS = {'.Spotlight-V100', '.Trashes', '.fseventsd', '.TemporaryItems', '.DocumentRevisions-V100', '__MACOSX'}


class TransientError(Exception):
    """Может пройти само: сеть, 429, 5xx. Повторяем с паузой."""
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class FileError(Exception):
    """Этот файл залить нельзя (например, больше лимита тарифа). Остальные продолжаем."""


class FatalError(Exception):
    """Дальше заливать бессмысленно: токен не подходит, кончилось место или лимит трафика."""


class MissingParent(Exception):
    """Яндекс не нашёл папку, в которую кладём файл."""


class Aborted(Exception):
    """Нажали «Стоп» или очередь упала: начатая отправка обрывается."""


# ==========================================
# ВСПОМОГАТЕЛЬНОЕ
# ==========================================

def make_session():
    """Сессия к Яндексу всегда напрямую: системный прокси (например, туннель на сервере) только мешает."""
    s = requests.Session()
    s.trust_env = False
    s.proxies = {}
    return s


def is_junk(name):
    return name.startswith('._') or name in JUNK_NAMES


def normalize_remote(path):
    """«Проекты/Медалл», «/Проекты/Медалл/» и «disk:/Проекты/Медалл» → «disk:/Проекты/Медалл»."""
    p = (path or '').strip().replace('\\', '/')
    if p.startswith('disk:'):
        p = p[5:]
    parts = [x for x in p.split('/') if x and x != '.']
    if not parts:
        raise ValueError('Укажите папку на Диске, например «Заливка/Проект»')
    if any(x == '..' for x in parts):
        raise ValueError('В пути на Диске не должно быть «..»')
    return 'disk:/' + '/'.join(parts)


def remote_parents(remote):
    """disk:/A/B/C → [disk:/A, disk:/A/B, disk:/A/B/C]."""
    parts = remote[len('disk:/'):].split('/')
    return ['disk:/' + '/'.join(parts[:i]) for i in range(1, len(parts) + 1)]


def load_server_token():
    """Токен сервера: переменная YANDEX_DISK_TOKEN или файл .env из YDISK_ENV_FILE. Значение не печатаем."""
    tok = os.environ.get('YANDEX_DISK_TOKEN', '').strip()
    if tok:
        return tok
    env_file = os.environ.get('YDISK_ENV_FILE', '').strip()
    if not env_file:
        return None
    try:
        with open(env_file, encoding='utf-8-sig') as fh:
            for line in fh:
                line = line.strip()
                if line.startswith('export '):
                    line = line[7:].lstrip()
                if line.startswith('YANDEX_DISK_TOKEN='):
                    value = line.split('=', 1)[1].strip().strip('"').strip("'")
                    return value or None
    except OSError:
        return None
    return None


def retry_after_seconds(r):
    try:
        return max(0, int(r.headers.get('Retry-After', '')))
    except (TypeError, ValueError):
        return None


def raise_for(r, what):
    """Переводит ответ Яндекса в наши исключения. Текст ошибки — для журнала, без токена."""
    s = r.status_code
    if s < 400:
        return
    code, desc = '', ''
    try:
        data = r.json()
        code = str(data.get('error') or '')
        desc = str(data.get('description') or data.get('message') or '')
    except ValueError:
        pass
    detail = f"{what}: HTTP {s}" + (f" {code}" if code else '') + (f" — {desc}" if desc else '')
    if s == 401:
        raise FatalError('Токен Яндекс.Диска не подходит или устарел (401)')
    if s == 507 or 'Quota' in code or 'Storage' in code:
        raise FatalError('На Яндекс.Диске не хватает места')
    if s == 423 and 'TRAFFIC' in code.upper():
        raise FatalError('Исчерпан месячный лимит загрузки на Яндекс.Диск')
    if s == 403:
        raise FatalError(f'Яндекс запретил запрос. {detail}')
    if s == 413:
        raise FileError('Файл больше, чем разрешает тариф Диска (413)')
    if s == 409 and 'DoesntExist' in code:
        raise MissingParent(detail)
    if s in (408, 423, 429) or s >= 500:
        raise TransientError(detail, retry_after_seconds(r))
    raise FileError(detail)


def clean_order(value):
    """Пометка заказа — короткая строка без переводов строк. Пусто — без заказа."""
    return ' '.join(str(value or '').split())[:ORDER_MAX]


def format_eta(seconds):
    if seconds is None or seconds < 0:
        return '--:--'
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    if d:
        return f"{d}д {h}ч"
    if h:
        return f"{h}ч {m}мин"
    return f"{m:02d}:{s:02d}"


def format_speed(bps):
    """bps — байт в секунду. Показываем в Мбит/с: так Яндекс и провайдер меряют скорость."""
    mbit = bps * 8 / 1e6
    return f"{mbit:.1f} Мбит/с" if mbit < 100 else f"{mbit:.0f} Мбит/с"


# ==========================================
# ЛОКАЛЬНЫЕ ФАЙЛЫ И ДИСК
# ==========================================

def scan_sources(sources, remote_root):
    """Что заливать. Папка кладётся на Диск подпапкой со своим именем, файл — прямо в remote_root.

    Возвращает (files, dirs, junk, problems): files — [{local, remote, name, size}],
    dirs — папки на Диске, которые нужны (кроме remote_root и его родителей).
    """
    files, dirs, problems = [], [], []
    junk = 0
    seen = set()

    def add_file(local, remote):
        nonlocal junk
        if remote in seen:
            problems.append(f"Дважды одно и то же место на Диске, второй файл пропущен: {local}")
            return
        try:
            size = local.stat().st_size
        except OSError as e:
            problems.append(f"Не прочитать {local}: {e}")
            return
        seen.add(remote)
        files.append({'local': str(local), 'remote': remote, 'name': remote.split('/')[-1], 'size': size})

    for raw in sources:
        src = Path(raw).expanduser()
        if not src.exists():
            problems.append(f"Нет такого файла или папки: {src}")
            continue
        if src.is_file():
            if is_junk(src.name):
                junk += 1
                continue
            add_file(src, f"{remote_root}/{src.name}")
            continue
        base = f"{remote_root}/{src.name}"
        dirs.append(base)
        for cur, subdirs, names in os.walk(src):
            subdirs[:] = sorted(d for d in subdirs if d not in JUNK_DIRS and not d.startswith('._'))
            rel = Path(cur).relative_to(src)
            remote_cur = base if str(rel) == '.' else base + '/' + '/'.join(rel.parts)
            for d in subdirs:
                dirs.append(f"{remote_cur}/{d}")
            for name in sorted(names):
                if is_junk(name):
                    junk += 1
                    continue
                add_file(Path(cur) / name, f"{remote_cur}/{name}")
    return files, dirs, junk, problems


def api_request(sess, token, method, endpoint, params, what):
    try:
        r = sess.request(method, API + endpoint, params=params, timeout=API_TIMEOUT,
                         headers={'Authorization': 'OAuth ' + token, 'Accept': 'application/json'})
    except requests.RequestException as e:
        raise TransientError(f"{what}: {type(e).__name__}") from None
    return r


def list_remote(sess, token, root):
    """Все файлы и папки под root на Диске: ({путь: размер}, {папки}). Нет папки — пусто."""
    files, dirs = {}, set()
    stack = [root]
    while stack:
        path = stack.pop()
        offset = 0
        while True:
            r = api_request(sess, token, 'GET', '/resources', {
                'path': path, 'limit': LIST_LIMIT, 'offset': offset,
                'fields': '_embedded.items.path,_embedded.items.type,_embedded.items.size,_embedded.total',
            }, 'Список папки на Диске')
            if r.status_code == 404:
                break
            raise_for(r, 'Список папки на Диске')
            emb = r.json().get('_embedded') or {}
            items = emb.get('items') or []
            dirs.add(path)
            for it in items:
                if it.get('type') == 'dir':
                    dirs.add(it['path'])
                    stack.append(it['path'])
                else:
                    files[it['path']] = it.get('size')
            offset += len(items)
            if not items or offset >= (emb.get('total') or 0):
                break
    return files, dirs


def temp_remote(remote):
    """Временное имя на время заливки: «a.mp4» → «a.mp4.ydupload.txt». Файлы .txt — как есть."""
    return remote if remote.lower().endswith('.txt') else remote + FAST_SUFFIX


def move_into_place(sess, token, src, dst, abort=None):
    """Переименовывает залитый временный файл в настоящее имя (с заменой).

    Сразу после PUT файл может ещё «перевариваться» и не находиться (404) — ждём до MOVE_WAIT с.
    Большие перемещения Яндекс делает асинхронно (202 + операция) — дожидаемся её.
    """
    deadline = time.time() + MOVE_WAIT
    while True:
        r = api_request(sess, token, 'POST', '/resources/move',
                        {'from': src, 'path': dst, 'overwrite': 'true'}, 'Переименование на Диске')
        if r.status_code == 404 and time.time() < deadline:
            if abort is not None and abort.wait(2):
                raise Aborted()
            if abort is None:
                time.sleep(2)
            continue
        raise_for(r, 'Переименование на Диске')
        break
    if r.status_code != 202:
        return
    try:
        href = (r.json() or {}).get('href') or ''
    except ValueError:
        href = ''
    if not href.startswith(API):
        return
    while time.time() < deadline:
        o = api_request(sess, token, 'GET', href[len(API):], {}, 'Переименование на Диске')
        raise_for(o, 'Переименование на Диске')
        status = (o.json() or {}).get('status')
        if status == 'success':
            return
        if status == 'failed':
            raise TransientError('Переименование на Диске: Яндекс вернул failed')
        if abort is not None and abort.wait(1):
            raise Aborted()
        if abort is None:
            time.sleep(1)
    raise TransientError('Переименование на Диске: операция не закончилась вовремя')


def make_dir(sess, token, path):
    r = api_request(sess, token, 'PUT', '/resources', {'path': path}, 'Создание папки на Диске')
    if r.status_code == 409:
        return  # Уже есть
    raise_for(r, 'Создание папки на Диске')


def publish(sess, token, path):
    """Публикует папку и возвращает публичную ссылку."""
    r = api_request(sess, token, 'PUT', '/resources/publish', {'path': path}, 'Публикация папки')
    raise_for(r, 'Публикация папки')
    r = api_request(sess, token, 'GET', '/resources', {'path': path, 'fields': 'public_url'}, 'Публичная ссылка')
    raise_for(r, 'Публичная ссылка')
    return r.json().get('public_url')


class ProgressReader:
    """Отдаёт файл в PUT кусками: считает отправленное и обрывает отправку по «Стопу».

    Длина известна заранее, поэтому requests ставит Content-Length, а не chunked.
    """
    def __init__(self, path, on_bytes, abort_event):
        self._f = open(path, 'rb')
        self._size = os.fstat(self._f.fileno()).st_size
        self._on_bytes = on_bytes
        self._abort = abort_event

    def __len__(self):
        return self._size

    def read(self, n=-1):
        if self._abort.is_set():
            raise Aborted()
        chunk = self._f.read(READ_BLOCK if n is None or n < 0 else min(n, READ_BLOCK))
        if chunk:
            self._on_bytes(len(chunk))
        return chunk

    def close(self):
        self._f.close()


# ==========================================
# ОЧЕРЕДЬ ЗАЛИВКИ
# ==========================================

ulock = threading.RLock()
uploads = []                 # Очереди заливки в порядке приоритета: верхняя — главная
hooks = {'save': None, 'session': make_session}
flags = {'quitting': False}  # Программа выключается: очереди остаются в session.json как есть


def new_state():
    return {
        'is_running': False, 'is_paused': False, 'should_stop': False,
        'phase': 'waiting', 'status_text': 'Ждёт запуска', 'notice': '',
        'total_files': 0, 'total_bytes': 0,
        'done_files': 0, 'done_bytes': 0,       # залито в этот запуск
        'skipped_files': 0, 'skipped_bytes': 0,  # уже было на Диске с тем же размером
        'junk_skipped': 0,
        'failed': [],                            # [{name, error}]
        'public_url': '',
        'logs': [],
    }


class UploadQueue:
    def __init__(self, sources, remote, threads=DEFAULT_THREADS, token=None, queue_id=None,
                 paused=False, had_token=False, public_url='', order=''):
        self.id = queue_id or secrets.token_hex(4)
        self.order = clean_order(order)
        self.sources = list(sources)
        self.remote = remote
        self.threads = threads
        self.token = token               # Только в памяти
        self.had_token = had_token or bool(token)
        self.keep_in_session = True
        self.thread = None
        self.workers = {}                # номер потока → Thread
        self.pending = deque()
        self.active = {}                 # remote → {name, size, sent, started}
        self.transmitted = 0             # всё отправленное, включая оборванные попытки: для скорости
        self.samples = deque()
        self.abort = threading.Event()   # «Стоп» или фатальная ошибка: рвём отправку
        self.cond = threading.Condition(ulock)
        self.fatal = None
        self.state = new_state()
        self.state['is_paused'] = paused
        self.state['public_url'] = public_url or ''

    # ---------- журнал и состояние ----------

    def log(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(f"[up {self.id}] {line}", flush=True)
        with ulock:
            logs = self.state['logs']
            logs.append(line)
            if len(logs) > LOG_LIMIT:
                del logs[0]

    def set_phase(self, phase, text):
        with ulock:
            self.state['phase'] = phase
            self.state['status_text'] = text

    def display_status(self):
        st = self.state
        if st['is_running']:
            if st['should_stop']:
                return 'stopping'
            if st['is_paused']:
                return 'paused'
        return st['phase']

    def _add_sent(self, item, n):
        with ulock:
            item['sent'] += n
            self.transmitted += n

    def _speed(self, now):
        """Скорость по скользящему окну. Вызывать под ulock."""
        self.samples.append((now, self.transmitted))
        while len(self.samples) > 2 and now - self.samples[0][0] > SPEED_WINDOW:
            self.samples.popleft()
        t0, b0 = self.samples[0]
        return (self.transmitted - b0) / (now - t0) if now - t0 >= 1 else 0.0

    def snapshot(self):
        """Состояние для интерфейса. Токена здесь нет."""
        now = time.time()
        with ulock:
            st = self.state
            speed = self._speed(now) if st['phase'] == 'uploading' else 0.0
            active = []
            tail = 0.0
            in_flight = 0
            for it in sorted(self.active.values(), key=lambda x: -x['size']):
                elapsed = now - it['started']
                fspeed = it['sent'] / elapsed if elapsed >= 2 else 0.0
                in_flight += it['sent']
                if fspeed > 0:
                    tail = max(tail, (it['size'] - it['sent']) / fspeed)
                active.append({'name': it['name'], 'size': it['size'], 'sent': it['sent'],
                               'percent': round(100 * it['sent'] / it['size'], 1) if it['size'] else 0,
                               'speed_str': format_speed(fspeed) if fspeed else ''})
            progress = st['done_bytes'] + st['skipped_bytes'] + in_flight
            total = st['total_bytes']
            eta = None
            if speed > 0 and st['phase'] == 'uploading':
                eta = (total - progress) / speed
                # Пока файлы не кончились, время ограничено и самым долгим одиночным файлом
                eta = max(eta, tail)
            notice = st['notice']
            single_note = (st['phase'] == 'uploading' and not self.pending and 0 < len(self.active) < self.threads
                           and not st['is_paused'])
            if single_note:
                n = len(self.active)
                notice = (f"Остались только начатые файлы ({n}). Каждый файл Яндекс принимает одним соединением, "
                          "по частям его не залить. Файлы идут под временным именем .txt, чтобы Яндекс не "
                          "притормаживал видео, и в конце переименовываются.")
            elif st['is_paused'] and self.active and st['is_running']:
                notice = (f"Новые файлы не начинаются. Уже начатые ({len(self.active)}) догружаются, "
                          "чтобы не терять отправленное: прервать их можно кнопкой «Стоп».")
            data = {k: v for k, v in st.items() if k != 'failed'}
            data.update(
                id=self.id, kind='upload', order=self.order, sources=self.sources, remote=self.remote, threads=self.threads,
                display=self.display_status(), active=active, pending_files=len(self.pending),
                failed=st['failed'][-100:], failed_count=len(st['failed']),
                progress_bytes=progress, notice=notice,
                overall_percent=round(min(100.0, 100 * progress / total), 1) if total else (100.0 if st['phase'] == 'done' else 0.0),
                speed_bps=speed, speed_str=format_speed(speed) if speed else '',
                eta_str=format_eta(eta) if eta is not None else '--:--',
                logs=list(st['logs']),
            )
            return data

    def session_entry(self):
        return {'id': self.id, 'order': self.order, 'sources': self.sources, 'remote': self.remote, 'threads': self.threads,
                'paused': self.state['is_paused'], 'had_token': self.had_token,
                'public_url': self.state['public_url']}

    # ---------- запуск ----------

    def start(self):
        with ulock:
            fresh = new_state()
            fresh['logs'] = self.state['logs']
            fresh['is_paused'] = self.state['is_paused']
            fresh['public_url'] = self.state['public_url']
            fresh['is_running'] = True
            self.state = fresh
            self.pending = deque()
            self.active = {}
            self.workers = {}
            self.samples = deque()
            self.transmitted = 0
            self.fatal = None
            self.abort.clear()
            self.keep_in_session = True
            self.thread = threading.Thread(target=upload_worker, args=(self,), daemon=True)
        self.thread.start()

    def block(self, reason):
        with ulock:
            self.state['phase'] = 'blocked'
            self.state['status_text'] = reason
        self.log(f"⚠️ Заливка не продолжена: {reason}")


def save():
    fn = hooks.get('save')
    if fn:
        with ulock:
            entries = [q.session_entry() for q in uploads if q.keep_in_session]
        fn(entries)


def sleep_or_abort(q, seconds):
    """Пауза между повторами. True — пора бросать (Стоп или фатальная ошибка)."""
    return q.abort.wait(seconds)


def with_retries(q, fn, what, tries=META_RETRIES):
    for attempt in range(1, tries + 1):
        if q.abort.is_set():
            raise Aborted()
        try:
            return fn()
        except TransientError as e:
            if attempt == tries:
                raise FatalError(f"{what}: не получилось за {tries} попыток ({e})")
            delay = e.retry_after if e.retry_after is not None else min(RETRY_MAX, RETRY_BASE * 2 ** (attempt - 1))
            q.log(f"🔁 {e}. Повтор через {int(delay)} с")
            if sleep_or_abort(q, delay):
                raise Aborted()


def upload_one(q, item, sess):
    """Заливает один файл с повторами. Возвращает 'done', 'failed' или 'stopped'."""
    token = q.token
    last_error = ''
    tmp = temp_remote(item['remote'])
    for attempt in range(1, FILE_RETRIES + 1):
        if q.abort.is_set():
            return 'stopped'
        with ulock:
            item['sent'] = 0
            item['started'] = time.time()
        reader = None
        try:
            r = api_request(sess, token, 'GET', '/resources/upload',
                            {'path': tmp, 'overwrite': 'true', 'fields': 'href,method'},
                            'Ссылка для заливки')
            try:
                raise_for(r, 'Ссылка для заливки')
            except MissingParent:
                # Папку кто-то удалил или она не создалась: создаём и пробуем снова
                for parent in remote_parents(item['remote'].rsplit('/', 1)[0]):
                    make_dir(sess, token, parent)
                raise TransientError('Не было папки на Диске, создана')
            link = r.json()
            reader = ProgressReader(item['local'], lambda n: q._add_sent(item, n), q.abort)
            try:
                p = sess.request(link.get('method') or 'PUT', link['href'], data=reader, timeout=PUT_TIMEOUT)
            except requests.RequestException as e:
                raise TransientError(f"Обрыв при отправке: {type(e).__name__}") from None
            if p.status_code in (200, 201, 202):
                if tmp != item['remote']:
                    # Файл уже на Диске под временным именем: повторяем только переименование
                    try:
                        with_retries(q, lambda: move_into_place(sess, token, tmp, item['remote'], q.abort),
                                     'Переименование на Диске')
                    except FatalError as e:
                        item['error'] = f"Залит, но не переименован (на Диске лежит «{tmp.rsplit('/', 1)[-1]}»): {e}"
                        return 'failed'
                return 'done'
            raise_for(p, 'Отправка файла')
            raise TransientError(f"Отправка файла: неожиданный ответ HTTP {p.status_code}")
        except Aborted:
            return 'stopped'
        except FatalError as e:
            q_fail_fatal(q, str(e))
            return 'stopped'
        except FileError as e:
            item['error'] = str(e)
            return 'failed'
        except (TransientError, OSError) as e:
            if q.abort.is_set():
                return 'stopped'
            if isinstance(e, OSError) and not isinstance(e, TransientError):
                # Файл пропал или не читается с диска: повторять бесполезно
                item['error'] = f"Не прочитать файл: {e}"
                return 'failed'
            last_error = str(e)
            if attempt == FILE_RETRIES:
                break
            retry_after = getattr(e, 'retry_after', None)
            delay = retry_after if retry_after is not None else min(RETRY_MAX, RETRY_BASE * 2 ** (attempt - 1))
            q.log(f"🔁 {item['name']}: {e}. Попытка {attempt + 1} из {FILE_RETRIES} через {int(delay)} с")
            if sleep_or_abort(q, delay):
                return 'stopped'
        finally:
            if reader:
                reader.close()
    item['error'] = f"Не залилось за {FILE_RETRIES} попыток: {last_error}"
    return 'failed'


def q_fail_fatal(q, message):
    with ulock:
        if q.fatal is None:
            q.fatal = message
            first = True
        else:
            first = False
        q.abort.set()
        q.cond.notify_all()
    if first:
        q.log(f"❌ {message}. Заливка остановлена")


def worker_loop(q, idx):
    sess = hooks['session']()
    while True:
        with q.cond:
            while True:
                if q.abort.is_set() or not q.pending:
                    return
                if not q.state['is_paused'] and idx < q.threads:
                    break
                q.cond.wait(1)
            item = q.pending.popleft()
            item['sent'] = 0
            item['started'] = time.time()
            q.active[item['remote']] = item
        result = upload_one(q, item, sess)
        with q.cond:
            q.active.pop(item['remote'], None)
            st = q.state
            if result == 'done':
                st['done_files'] += 1
                st['done_bytes'] += item['size']
            elif result == 'failed':
                st['failed'].append({'name': item['remote'], 'error': item.get('error', '')})
            else:
                q.pending.appendleft(item)  # Не начат заново: при «Запустить снова» пойдёт первым
            q.cond.notify_all()
        if result == 'done':
            q.log(f"✅ {item['remote'].split('disk:/', 1)[-1]} ({item['size'] / 1024**2:.1f} МБ)")
        elif result == 'failed':
            q.log(f"❌ {item['remote'].split('disk:/', 1)[-1]}: {item.get('error', '')}")


def ensure_workers(q):
    """Запускает недостающие потоки (в том числе после увеличения их числа на ходу). Под ulock."""
    for idx in range(q.threads):
        t = q.workers.get(idx)
        if (t is None or not t.is_alive()) and q.pending and not q.abort.is_set():
            t = threading.Thread(target=worker_loop, args=(q, idx), daemon=True)
            q.workers[idx] = t
            t.start()


def run_upload(q):
    st = q.state
    if not q.token:
        raise FatalError('Нет токена Яндекс.Диска: впишите его в поле «Токен» или задайте на сервере')
    sess = hooks['session']()

    q.set_phase('scanning', 'Смотрю, что заливать…')
    files, dirs, junk, problems = scan_sources(q.sources, q.remote)
    for p in problems:
        q.log(f"⚠️ {p}")
    with ulock:
        st['junk_skipped'] = junk
    if junk:
        q.log(f"🧹 Пропущено служебных файлов macOS/Windows (._*, .DS_Store и т. п.): {junk}")
    if not files and not dirs:
        raise FatalError('Нечего заливать: файлы не найдены')
    total = sum(f['size'] for f in files)
    with ulock:
        st['total_files'] = len(files)
        st['total_bytes'] = total
    q.log(f"📦 Найдено {len(files)} файлов, {total / 1024**3:.2f} ГБ → {q.remote}")

    q.set_phase('checking', 'Проверяю, что уже есть на Диске…')
    remote_files, remote_dirs = with_retries(q, lambda: list_remote(sess, q.token, q.remote), 'Список папки на Диске')
    need = [d for d in remote_parents(q.remote) + dirs if d not in remote_dirs]
    if need:
        q.set_phase('checking', f'Создаю папки на Диске: {len(need)}')
    for d in need:
        with_retries(q, lambda d=d: make_dir(sess, q.token, d), 'Создание папки на Диске')

    todo = []
    for f in files:
        if remote_files.get(f['remote']) == f['size']:
            with ulock:
                st['skipped_files'] += 1
                st['skipped_bytes'] += f['size']
        else:
            todo.append(dict(f, sent=0, started=0))
    if st['skipped_files']:
        q.log(f"⏭️ Уже на Диске с тем же размером, пропущено: {st['skipped_files']} файлов "
              f"({st['skipped_bytes'] / 1024**3:.2f} ГБ)")
    todo.sort(key=lambda f: -f['size'])  # Крупные первыми: они самые долгие
    big = [f for f in todo if f['size'] >= 2 * 1024**3]
    if big:
        q.log(f"🐢 Больших файлов (от 2 ГБ): {len(big)}. Каждый идёт одним соединением, поэтому они начаты первыми.")

    with q.cond:
        q.pending = deque(todo)
        st['phase'] = 'uploading'
        st['status_text'] = f"Заливка: {len(todo)} файлов, {sum(f['size'] for f in todo) / 1024**3:.2f} ГБ"
        q.samples.clear()
    q.log(f"🚀 Заливаю {len(todo)} файлов в {q.threads} потоков")

    while True:
        with q.cond:
            ensure_workers(q)
            alive = [t for t in q.workers.values() if t.is_alive()]
            if not alive and (not q.pending or q.abort.is_set()):
                break
            q.cond.wait(0.5)

    if q.fatal:
        raise FatalError(q.fatal)
    if q.abort.is_set():
        raise Aborted()
    if st['failed']:
        return
    q.set_phase('publishing', 'Публикую папку…')
    url = with_retries(q, lambda: publish(sess, q.token, q.remote), 'Публикация папки')
    with ulock:
        st['public_url'] = url or ''
    q.log(f"🔗 Публичная ссылка: {url}")


def upload_worker(q):
    st = q.state
    q.log(f"🚀 Заливка на Яндекс.Диск: {', '.join(q.sources)} → {q.remote}")
    error = None
    crashed = False
    aborted = False
    try:
        run_upload(q)
    except Aborted:
        aborted = True
    except FatalError as e:
        error = str(e)
        if q.fatal is None:
            q.log(f"❌ {error}")
    except Exception as e:  # Не должно случаться: показываем, а не падаем молча
        crashed = True
        error = f"Сбой программы: {type(e).__name__}: {e}"
        q.log(f"❌ {error}")
    q.abort.set()  # Добиваем потоки, если что-то осталось
    for t in list(q.workers.values()):
        t.join(5)

    quitting = flags['quitting']
    with ulock:
        st['is_running'] = False
        if not quitting:  # Заливка на паузе останется на паузе и после перезапуска
            st['is_paused'] = False
        if (st['should_stop'] or aborted) and not error:
            st['phase'], st['status_text'] = 'stopped', 'Заливка остановлена'
        elif error:
            st['phase'] = 'crashed' if crashed else 'problems'
            st['status_text'] = error + '. Нажмите «Продолжить», чтобы попробовать снова'
        elif st['failed']:
            st['phase'] = 'problems'
            st['status_text'] = (f"Не залито файлов: {len(st['failed'])}. Нажмите «Продолжить» — залитые "
                                 "пропустятся, остальные попробуем снова")
        else:
            st['phase'] = 'done'
            st['status_text'] = 'Заливка завершена'
        # Незавершённую заливку продолжим после перезапуска программы, если её не остановили «Стопом»
        if not quitting and not crashed:
            q.keep_in_session = False
        q.active = {}
    save()
    q.log('🏁 Заливка завершена' if st['phase'] == 'done' else f"🏁 Итог: {st['status_text']}")


# ==========================================
# УПРАВЛЕНИЕ
# ==========================================

def find(queue_id):
    with ulock:
        return next((q for q in uploads if q.id == queue_id), None)


def valid_threads(value):
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_THREADS


def add(sources, remote, threads=DEFAULT_THREADS, token=None, order=''):
    """Новая очередь заливки. Возвращает (очередь, None) или (None, текст ошибки)."""
    sources = [str(Path(s).expanduser()) for s in sources if str(s).strip()]
    if not sources:
        return None, 'Укажите, что заливать: папку или файлы'
    missing = [s for s in sources if not Path(s).exists()]
    if missing:
        return None, f"Нет такого файла или папки: {missing[0]}"
    try:
        remote = normalize_remote(remote)
    except ValueError as e:
        return None, str(e)
    if not valid_threads(threads):
        return None, f"Потоков: от 1 до {MAX_THREADS}"
    token = token or load_server_token()
    if not token:
        return None, 'Нужен токен Яндекс.Диска: впишите его в поле «Токен»'
    q = UploadQueue(sources, remote, threads, token, order=order)
    with ulock:
        for other in uploads:
            if other.state['is_running'] and other.remote == remote and set(other.sources) == set(sources):
                return None, 'Эта заливка уже идёт'
        uploads.append(q)
        q.state['is_running'] = True
    q.start()
    save()
    return q, None


def pause(q):
    with q.cond:
        st = q.state
        if not st['is_running'] or st['should_stop'] or st['is_paused']:
            return False
        st['is_paused'] = True
        q.cond.notify_all()
    q.log('⏸️ Пауза: новые файлы не начинаются, начатые догружаются')
    save()
    return True


def resume(q):
    with q.cond:
        st = q.state
        if not st['is_running'] or not st['is_paused']:
            return False
        st['is_paused'] = False
        q.cond.notify_all()
    q.log('▶️ Продолжено')
    save()
    return True


def stop(q):
    with q.cond:
        if not q.state['is_running']:
            return False
        q.state['should_stop'] = True
        q.keep_in_session = False
        q.abort.set()
        q.cond.notify_all()
    q.log('⏹️ Стоп: начатые файлы оборваны, при «Продолжить» они начнутся заново')
    save()
    return True


def restart(q, token=None):
    """«Продолжить» после стопа или ошибок: уже залитое пропустится по размеру."""
    if token:
        q.token = token
        q.had_token = True
    if not q.token:
        q.token = load_server_token()
    if not q.token:
        return 'Нужен токен Яндекс.Диска: впишите его в поле «Токен» и нажмите «Продолжить» ещё раз'
    missing = [s for s in q.sources if not Path(s).exists()]
    if missing:
        return f"Нет такого файла или папки: {missing[0]}"
    with ulock:
        if q.state['is_running']:
            return 'Заливка ещё идёт'
        q.state['is_running'] = True
        q.state['is_paused'] = False
    q.start()
    save()
    return None


def remove(q):
    with ulock:
        if q.state['is_running']:
            return False
        uploads.remove(q)
    save()
    return True


def set_threads(q, value):
    if not valid_threads(value):
        return f"Потоков: от 1 до {MAX_THREADS}"
    with q.cond:
        q.threads = value
        q.cond.notify_all()  # Лишние потоки доделают свой файл и уснут, недостающие запустит главный цикл
    q.log(f"🔧 Потоков: {value}")
    save()
    return None


def move_to_top(q):
    """Приоритет: очередь встаёт первой и продолжается, остальные идущие заливки — на паузу."""
    with ulock:
        if q not in uploads:
            return 'Такой заливки нет'
        uploads.remove(q)
        uploads.insert(0, q)
        others = [x for x in uploads if x is not q and x.state['is_running'] and not x.state['is_paused']
                  and not x.state['should_stop']]
    paused = [x for x in others if pause(x)]
    if q.state['is_running'] and q.state['is_paused']:
        resume(q)
    q.log('⬆️ Поднята наверх' + (f", на паузу поставлено заливок: {len(paused)}" if paused else ''))
    save()
    return None


def snapshot_all():
    with ulock:
        items = list(uploads)
    return [q.snapshot() for q in items]


def restore(entries, log=print):
    """Продолжает заливки, прерванные выключением программы или перезагрузкой."""
    server_token = load_server_token()
    restored = []
    for e in entries or []:
        if not isinstance(e, dict) or not isinstance(e.get('sources'), list) or not e.get('remote'):
            continue
        threads = e.get('threads') if valid_threads(e.get('threads')) else DEFAULT_THREADS
        q = UploadQueue(e['sources'], e['remote'], threads, server_token, queue_id=e.get('id'),
                        paused=bool(e.get('paused')), had_token=bool(e.get('had_token')),
                        public_url=e.get('public_url') or '', order=e.get('order') or '')
        restored.append(q)
    with ulock:
        uploads.extend(restored)
    for q in restored:
        missing = [s for s in q.sources if not Path(s).exists()]
        if missing:
            q.block(f"нет {missing[0]} (не подключён диск?)")
        elif not q.token:
            q.block('нет токена: впишите его в поле «Токен» и нажмите «Продолжить»')
        else:
            with ulock:
                q.state['is_running'] = True
            q.start()
    save()
    return restored


def stop_all_for_quit():
    """Выключение программы: начатые заливки остаются в session.json и продолжатся при запуске."""
    with ulock:
        flags['quitting'] = True
        for q in uploads:
            if q.state['is_running']:
                q.abort.set()
                q.cond.notify_all()
