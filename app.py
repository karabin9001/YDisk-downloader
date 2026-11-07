#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YDisk Downloader — локальный веб-инструмент пакетного скачивания с Яндекс.Диска и Google Drive.
Работает на Mac и Windows, скачивает на максимальной скорости с автодокачкой.
Ведёт несколько очередей сразу: у каждой своя папка, прогресс, журнал и кнопки Пауза / Стоп.
"""

import os
import sys
import io
import time
import json
import re
import html
import secrets
import threading
import subprocess
import urllib.parse
import webbrowser
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# Настройка безопасной кодировки консоли
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Модуль для SOCKS-прокси. Ставим до requests: он проверяет его наличие при импорте
try:
    import socks
except ImportError:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "PySocks"], timeout=60)
        import socks
    except Exception:
        socks = None  # Без него работает всё, кроме прокси

try:
    import requests
except ImportError:
    print("Установка библиотеки requests...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

HOST = "127.0.0.1"
PORT = int(os.environ.get('YDISK_PORT') or 7860)  # Другой порт нужен только для проверок

CHUNK_SIZE = 2 * 1024 * 1024
BACKOFF_BASE = 2            # первая пауза перед повтором, сек
BACKOFF_MAX = 120           # самая длинная пауза при обрыве связи, сек
FILE_HTTP_RETRIES = 6       # столько ошибок HTTP подряд, и файл уходит в конец очереди
SCAN_HTTP_RETRIES = 8       # то же при сканировании ссылки
DEFER_RETRY = 600           # через сколько вернуться к отложенному файлу, сек
QUOTA_RETRY = 3600          # то же при дневном лимите Google, сек: каждая попытка тоже идёт в счёт лимита
QUOTA_RETRY_MAX = 6 * 3600
DEFER_GIVE_UP = 48 * 3600   # дольше не ждём: лимит Google снимается за сутки
RESCAN_INTERVAL = 300       # как часто перепроверять папки, пока ждём отложенные файлы, сек
DEFAULT_MAX_ACTIVE = 2      # сколько очередей качается одновременно: не мешают друг другу на одном диске и канале
MAX_ACTIVE_LIMIT = 6
BUSY_RETRY = 5              # как часто проверять файл, который качает другая очередь, сек
LEGACY_SESSION_KEYS = ('active', 'urls')  # старый session.json с одной очередью
PROXY_SCHEMES = ('http', 'https', 'socks4', 'socks4a', 'socks5', 'socks5h')

class RetryableHTTP(Exception):
    """Сервер ответил ошибкой, которая может пройти сама (403, 429, 5xx)."""
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after

class QuotaExceeded(Exception):
    """Дневной лимит скачиваний файла на Google Drive."""

class PermanentError(Exception):
    """Повторять бессмысленно: файла нет или к нему нет доступа."""

class DownloadQueue:
    """Одна очередь: свои ссылки, папка, прогресс, журнал и кнопки. Качает в собственном потоке."""

    def __init__(self, urls, dest, token=None, proxy='', flat=False, queue_id=None, paused=False, had_token=False):
        self.id = queue_id or secrets.token_hex(4)
        self.urls = urls
        self.dest = Path(dest)
        self.token = token
        self.proxy = proxy
        self.flat = flat
        self.had_token = had_token or bool(token)  # Сам токен на диск не пишем
        self.thread = None
        self.has_slot = False
        self.wants_slot = False
        self.keep_in_session = True  # Продолжать ли очередь после перезапуска
        self.deferred = []           # Отложенные файлы: меняется под lock
        self.wake = threading.Event()
        self.state = new_queue_state()
        self.state["is_paused"] = paused

    def can_take_slot(self):
        """Вызывать под lock. Слоты выдаются в порядке добавления очередей."""
        if sum(1 for x in queues if x.has_slot) >= app_state["max_active"]:
            return False
        first = next((x for x in queues if x.wants_slot and not x.state["is_paused"]), None)
        return first is None or first is self

    def gate(self):
        """Ждёт конца паузы и свободный слот. False: очередь остановили, качать нельзя."""
        with slot_changed:
            st = self.state
            while True:
                if st["should_stop"]:
                    self.wants_slot = False
                    slot_changed.notify_all()
                    return False
                if not st["is_paused"]:
                    if self.has_slot:
                        return True
                    self.wants_slot = True
                    if self.can_take_slot():
                        self.has_slot = True
                        self.wants_slot = False
                        slot_changed.notify_all()
                        return True
                st["speed_mbs"] = 0.0
                st["speed_str"] = "0 MB/s"
                slot_changed.wait(1)

    def try_slot(self):
        """Берёт слот, только если он свободен прямо сейчас."""
        with lock:
            if not self.has_slot and self.can_take_slot():
                self.has_slot = True
            return self.has_slot

    def release_slot(self):
        with slot_changed:
            self.has_slot = False
            self.wants_slot = False
            slot_changed.notify_all()

    def start(self):
        """Запускает очередь заново. Журнал остаётся, прогресс считается с начала."""
        with lock:
            fresh = new_queue_state()
            fresh["logs"] = self.state["logs"]
            fresh["is_paused"] = self.state["is_paused"]
            fresh["is_running"] = True
            if fresh["is_paused"]:
                fresh["status_text"] = "Загрузка на паузе ⏸️"
            self.state = fresh
            self.deferred = []
            self.keep_in_session = True
            self.wants_slot = not fresh["is_paused"]  # Сразу занимаем место в очереди на слот
            self.wake.clear()
            self.thread = threading.Thread(target=download_worker, args=(self,), daemon=True)
        self.thread.start()

    def block(self, reason):
        """Начать нельзя (например, не подключён диск). Запись остаётся до следующего запуска."""
        with lock:
            self.state["phase"] = "blocked"
            self.state["status_text"] = reason
        add_log(f"⚠️ Очередь не продолжена: {reason}", self)

    def display_status(self):
        st = self.state
        if st["is_running"]:
            if st["should_stop"]:
                return "stopping"
            if st["is_paused"]:
                return "paused"
            if self.wants_slot:
                return "waiting"
        return st["phase"]

    def snapshot(self):
        """Вызывать под lock. Токена и адреса прокси в ответе нет."""
        data = dict(self.state)
        data.update(id=self.id, urls=self.urls, dest=str(self.dest), flat=self.flat, proxy_on=bool(self.proxy),
                    has_slot=self.has_slot, display=self.display_status(), deferred_files=len(self.deferred))
        return data

    def session_entry(self):
        return {"id": self.id, "urls": self.urls, "dest": str(self.dest), "proxy": self.proxy, "flat": self.flat,
                "paused": self.state["is_paused"], "had_token": self.had_token}

def new_queue_state():
    return {
        "is_running": False,
        "is_paused": False,
        "should_stop": False,
        "phase": "waiting",
        "notice": "",
        "status_text": "Ждёт свободного слота",
        "current_file": "",
        "current_file_idx": 0,
        "total_files": 0,
        "file_percent": 0.0,
        "file_downloaded_mb": 0.0,
        "file_total_mb": 0.0,
        "overall_percent": 0.0,
        "speed_mbs": 0.0,
        "speed_str": "0 MB/s",
        "eta_str": "--:--",
        "total_bytes": 0,
        "downloaded_bytes": 0,
        "completed_list": [],
        "folders": [],
        "logs": []
    }

lock = threading.Lock()
slot_changed = threading.Condition(lock)  # Будит очереди, которые ждут слот или конец паузы
session_lock = threading.RLock()

queues = []      # Все очереди в порядке добавления: меняется под lock
busy_paths = {}  # Файл, который качается прямо сейчас → id очереди
app_state = {"quitting": False, "max_active": DEFAULT_MAX_ACTIVE, "logs": []}

def add_log(msg: str, q=None):
    """Без очереди запись попадает в общий журнал программы."""
    timestamp = time.strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(f"[{q.id}] {formatted}" if q else formatted)
    with lock:
        logs = q.state["logs"] if q else app_state["logs"]
        logs.append(formatted)
        if len(logs) > 200:
            logs.pop(0)

def format_eta(seconds):
    if seconds is None or seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}ч {m}мин"
    return f"{m}мин {s}сек"

API_BASE = 'https://cloud-api.yandex.net/v1/disk/public/resources'
# Символы, которые нельзя использовать в именах файлов на Windows
WIN_FORBIDDEN = '<>:"|?*'

def _auth_headers(token):
    return {'Authorization': f'OAuth {token.strip()}'} if token else {}

def get_public_resources(public_key, path='', token=None, offset=0, limit=100):
    params = {'public_key': public_key, 'limit': limit, 'offset': offset}
    if path:
        params['path'] = path
    last_error = None
    for attempt in range(3):
        try:
            r = requests.get(API_BASE, params=params, headers=_auth_headers(token), timeout=25)
            if r.status_code == 429 or r.status_code >= 500:
                raise Exception(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            raise
        except Exception as e:
            last_error = e
            time.sleep(2 * (attempt + 1))
    raise Exception(f"API Яндекса не отвечает: {last_error}")

def scan_folder_recursive(public_key, path='', token=None, dirs=None):
    """Возвращает список файлов папки со всеми подпапками. Пути подпапок складывает в dirs."""
    files = []
    offset = 0
    limit = 100
    while True:
        # Ошибку не глушим: иначе папка молча скачается не полностью
        data = get_public_resources(public_key, path=path, token=token, offset=offset, limit=limit)
        items = data.get('_embedded', {}).get('items', [])
        for it in items:
            if it.get('type') == 'dir':
                if dirs is not None:
                    dirs.append(it.get('path'))
                files.extend(scan_folder_recursive(public_key, it.get('path'), token=token, dirs=dirs))
            else:
                files.append({
                    'name': it.get('name'),
                    'path': it.get('path'),
                    'size': it.get('size', 0)
                })
        if len(items) < limit:
            break
        offset += limit
    return files

def safe_rel_path(remote_path: str) -> Path:
    """Превращает путь с Диска в безопасный относительный путь (без '..' и запрещённых символов)."""
    parts = []
    for part in remote_path.replace('\\', '/').split('/'):
        if part in ('', '.', '..'):
            continue
        if os.name == 'nt':
            part = ''.join('_' if c in WIN_FORBIDDEN else c for c in part).rstrip(' .') or '_'
        parts.append(part)
    return Path(*parts) if parts else Path('_')

def get_download_link(public_key, path, token=None):
    params = {'public_key': public_key}
    if path:
        params['path'] = path
    r = requests.get(f'{API_BASE}/download', params=params, headers=_auth_headers(token), timeout=25)
    if r.status_code == 200:
        href = r.json().get('href')
        if href:
            return href
    try:
        reason = r.json().get('message') or r.json().get('description') or ''
    except ValueError:
        reason = ''
    message = f"Яндекс не выдал ссылку на файл (HTTP {r.status_code}). {reason}".strip()
    if is_retryable_status(r.status_code):
        raise RetryableHTTP(message, retry_after_seconds(r))
    raise PermanentError(message)

def retry_after_seconds(r):
    value = r.headers.get('Retry-After', '')
    return int(value) if value.isdigit() else None

def is_retryable_status(status):
    return status in (403, 408, 429) or status >= 500

# ==========================================
# GOOGLE DRIVE
# ==========================================

GDRIVE_HOSTS = ('drive.google.com', 'drive.usercontent.google.com', 'docs.google.com')
GDRIVE_LIST_URL = 'https://drive.google.com/embeddedfolderview'
GDRIVE_DOWNLOAD_URL = 'https://drive.usercontent.google.com/download'
GDRIVE_FILE_PAGE_URL = 'https://drive.google.com/file/d/{}/view'
GDRIVE_PROBE_THREADS = 8
# Страница ошибки Google: запрашиваем на английском, но на всякий случай знаем и русский текст
GDRIVE_QUOTA_MARKERS = (
    'too many users have viewed or downloaded',
    'download quota',
    'quota exceeded',
    'exceeded the download',
    "you can't view or download this file",
    'слишком много пользователей',
    'превышена квота',
)

def gdrive_get(url, params=None, headers=None, stream=False, proxy=''):
    """proxy: адрес прокси очереди. Пустая строка: соединение напрямую."""
    session = requests.Session()
    # Настройка в окне программы главнее переменных окружения
    session.trust_env = False
    if proxy:
        session.proxies = {'http': proxy, 'https': proxy}
    all_headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'en'}
    all_headers.update(headers or {})
    return session.get(url, params=params, headers=all_headers, stream=stream, timeout=30)

def parse_gdrive_url(url):
    """Возвращает {'kind', 'id', 'resource_key'} для ссылки Google Drive, None для прочих ссылок."""
    parsed = urllib.parse.urlparse(url if '://' in url else 'https://' + url)
    host = (parsed.hostname or '').lower()
    if host not in GDRIVE_HOSTS:
        return None
    query = urllib.parse.parse_qs(parsed.query)
    resource_key = (query.get('resourcekey') or [None])[0]
    m = re.search(r'/folders/([\w-]+)', parsed.path)
    if m:
        return {'kind': 'folder', 'id': m.group(1), 'resource_key': resource_key}
    m = re.search(r'/file/(?:u/\d+/)?d/([\w-]+)', parsed.path)
    if m:
        return {'kind': 'file', 'id': m.group(1), 'resource_key': resource_key}
    if query.get('id'):
        return {'kind': 'file', 'id': query['id'][0], 'resource_key': resource_key}
    return {'kind': 'unknown', 'id': '', 'resource_key': None}

def gdrive_check_public(r, what):
    if 'accounts.google.com' in r.url or r.status_code in (401, 404):
        raise PermanentError(f"{what} недоступен: нужен доступ по ссылке «Все, у кого есть ссылка»")

def gdrive_list_folder(folder_id, resource_key=None, proxy=''):
    """Читает одну папку, без вложенных. Возвращает (имя папки, список записей)."""
    params = {'id': folder_id}
    if resource_key:
        params['resourcekey'] = resource_key
    r = gdrive_get(GDRIVE_LIST_URL, params=params, proxy=proxy)
    gdrive_check_public(r, "Объект Google Drive")
    if r.status_code != 200:
        raise RetryableHTTP(f"Google Drive вернул HTTP {r.status_code}", retry_after_seconds(r))
    page = r.content.decode('utf-8', errors='replace')
    if 'flip-entries' not in page:
        raise RetryableHTTP("Google Drive вернул страницу без списка файлов")

    title = re.search(r'<title>(.*?)</title>', page, re.S)
    entries = []
    chunks = page.split('class="flip-entry"')[1:]
    for chunk in chunks:
        entry_id = re.search(r'id="entry-([^"]+)"', chunk)
        href = re.search(r'<a href="([^"]+)"', chunk)
        name = re.search(r'flip-entry-title">(.*?)</div>', chunk, re.S)
        if not (entry_id and href and name):
            # Лучше явная ошибка, чем папка, скачанная не полностью
            raise RetryableHTTP("Не удалось разобрать список файлов Google Drive")
        link = html.unescape(href.group(1))
        if '/folders/' in link:
            kind = 'dir'
        elif '/file/d/' in link:
            kind = 'file'
        else:
            kind = 'document'  # Документы, таблицы и презентации Google
        link_key = (urllib.parse.parse_qs(urllib.parse.urlparse(link).query).get('resourcekey') or [None])[0]
        entries.append({'id': entry_id.group(1), 'name': html.unescape(name.group(1)).strip(),
                        'kind': kind, 'resource_key': link_key})
    if len(entries) != page.count('id="entry-'):
        raise RetryableHTTP("Список файлов Google Drive разобран не полностью")
    return html.unescape(title.group(1)).strip() if title else folder_id, entries

def gdrive_walk(q, folder_id, resource_key, prefix, files, dirs, skipped, visited):
    """Обходит папку со всеми вложенными. Ярлыки могут зациклить обход, поэтому помним visited."""
    visited.add(folder_id)
    title, entries = with_retries(lambda: gdrive_list_folder(folder_id, resource_key, q.proxy), "Список файлов Google Drive", q)
    used_names = set()
    for e in sorted(entries, key=lambda x: x['id']):
        # В одной папке Google Drive могут лежать файлы с одинаковыми именами
        name = e['name'].replace('/', '_') or e['id']
        if name.casefold() in used_names:
            stem, ext = os.path.splitext(name)
            name = f"{stem} [{e['id'][:8]}]{ext}"
        used_names.add(name.casefold())
        rel = prefix + name
        if e['kind'] == 'dir':
            if e['id'] not in visited:
                dirs.append(rel)
                gdrive_walk(q, e['id'], e['resource_key'], rel + '/', files, dirs, skipped, visited)
        elif e['kind'] == 'document':
            skipped.append(rel)
        else:
            files.append({'name': name, 'path': rel, 'size': None, 'gdrive_id': e['id'],
                          'resource_key': e['resource_key'] or resource_key})
    return title

def gdrive_open(file_id, resource_key=None, start=0, end='', proxy=''):
    """Открывает поток файла. Страницы Google вместо файла превращает в понятные ошибки."""
    params = {'id': file_id, 'export': 'download', 'confirm': 't'}
    if resource_key:
        params['resourcekey'] = resource_key
    headers = {'Range': f'bytes={start}-{end}', 'Accept-Encoding': 'identity'}

    for _ in range(3):
        r = gdrive_get(GDRIVE_DOWNLOAD_URL, params=params, headers=headers, stream=True, proxy=proxy)
        if r.status_code == 416:
            return r
        is_attachment = 'attachment' in r.headers.get('Content-Disposition', '').lower()
        is_page = 'text/html' in r.headers.get('Content-Type', '').lower()
        if r.status_code in (200, 206) and (is_attachment or not is_page):
            return r

        status = r.status_code
        retry_after = retry_after_seconds(r)
        url = r.url
        body = r.raw.read(256 * 1024, decode_content=True).decode('utf-8', errors='replace')
        r.close()
        low_body = body.lower()

        if any(marker in low_body for marker in GDRIVE_QUOTA_MARKERS):
            raise QuotaExceeded()
        if 'accounts.google.com' in url or status in (401, 404):
            raise PermanentError("Файл недоступен: нужен доступ по ссылке «Все, у кого есть ссылка»")
        form = re.search(r'<form[^>]*id="download-form".*?</form>', body, re.S)
        if status == 200 and form:
            # Предупреждение Google о большом файле: повторяем запрос с полями формы
            for field in re.findall(r'<input[^>]*type="hidden"[^>]*>', form.group(0)):
                name = re.search(r'name="([^"]*)"', field)
                value = re.search(r'value="([^"]*)"', field)
                if name:
                    params[name.group(1)] = html.unescape(value.group(1)) if value else ''
            continue
        if is_retryable_status(status):
            raise RetryableHTTP(f"Google Drive вернул HTTP {status}", retry_after)
        raise RetryableHTTP(f"Google Drive вернул страницу вместо файла (HTTP {status})")
    raise RetryableHTTP("Google Drive не отдал файл после подтверждения")

def gdrive_filename(headers):
    disposition = headers.get('Content-Disposition', '')
    m = re.search(r"filename\*=UTF-8''([^;]+)", disposition, re.I)
    if m:
        return urllib.parse.unquote(m.group(1)).strip()
    m = re.search(r'filename="([^"]*)"', disposition)
    return m.group(1).strip() if m else ''

def gdrive_probe(file_id, resource_key=None, proxy=''):
    """Узнаёт размер и имя файла, запросив один байт.
    Про лимит это ничего не доказывает: файл с лимитом на такой запрос всё равно отвечает 206."""
    r = gdrive_open(file_id, resource_key, 0, 0, proxy)
    try:
        return response_range(r)[1], gdrive_filename(r.headers)
    finally:
        r.close()

def gdrive_page_title(file_id, proxy=''):
    """Имя файла, у которого исчерпан лимит: скачать его нельзя, а страница открывается."""
    try:
        page = gdrive_get(GDRIVE_FILE_PAGE_URL.format(file_id), proxy=proxy).content.decode('utf-8', errors='replace')
    except requests.RequestException:
        return ''
    m = re.search(r'<meta property="og:title" content="([^"]*)"', page)
    return html.unescape(m.group(1)).strip() if m else ''

def gdrive_fill_sizes(files, q):
    """Размер Drive отдаёт только по файлу, поэтому опрашиваем их параллельно."""
    done = [0]

    def probe(f):
        try:
            size, _ = with_retries(lambda: gdrive_probe(f['gdrive_id'], f['resource_key'], q.proxy), "Размер файла Google Drive", q)
            f['size'] = size
        except QuotaExceeded:
            f['note'] = 'Лимит Google, размер станет известен позже'
            f['quota'] = True
        except PermanentError as e:
            f['note'] = str(e)
        with lock:
            done[0] += 1
            q.state["status_text"] = f"Google Drive: узнаём размеры файлов {done[0]} из {len(files)}"

    with ThreadPoolExecutor(max_workers=GDRIVE_PROBE_THREADS) as pool:
        list(pool.map(probe, files))

def scan_gdrive_link(raw_url, known_ids, q):
    """Описание ссылки в том же виде, что и у Яндекса. Для известных файлов размер не спрашивает."""
    info = parse_gdrive_url(raw_url)
    if info['kind'] == 'folder':
        files, dirs, skipped = [], [], []
        name = gdrive_walk(q, info['id'], info['resource_key'], '', files, dirs, skipped, set())
        gdrive_fill_sizes([f for f in files if f['gdrive_id'] not in known_ids], q)
        files.sort(key=lambda f: f['path'])
        return {'name': name, 'is_folder': True, 'dirs': dirs, 'skipped': skipped, 'files': files}

    if info['kind'] == 'file':
        size, name = None, ''
        note = ''
        quota = False
        try:
            size, name = with_retries(lambda: gdrive_probe(info['id'], info['resource_key'], q.proxy), "Файл Google Drive", q)
        except QuotaExceeded:
            note = 'Лимит Google, размер станет известен позже'
            quota = True
        name = (name or gdrive_page_title(info['id'], q.proxy) or f"gdrive_{info['id']}").replace('/', '_')
        return {'name': name, 'is_folder': False, 'dirs': [], 'skipped': [],
                'files': [{'name': name, 'path': name, 'size': size, 'note': note, 'quota': quota,
                           'gdrive_id': info['id'], 'resource_key': info['resource_key']}]}

    raise PermanentError("Это ссылка Google, но не на папку и не на файл Drive")

# ==========================================
# СОХРАНЕНИЕ ОЧЕРЕДИ МЕЖДУ ЗАПУСКАМИ
# ==========================================

def session_path() -> Path:
    custom = os.environ.get('YDISK_CONFIG_DIR')
    if custom:
        return Path(custom) / 'session.json'
    if sys.platform == 'darwin':
        return Path.home() / 'Library' / 'Application Support' / 'YDisk Downloader' / 'session.json'
    if os.name == 'nt':
        return Path(os.environ.get('APPDATA', str(Path.home()))) / 'YDisk Downloader' / 'session.json'
    return Path.home() / '.config' / 'ydisk-downloader' / 'session.json'

def load_session() -> dict:
    try:
        data = json.loads(session_path().read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}

def update_session(**fields):
    """Токен сюда не попадает: файл хранится открытым текстом."""
    path = session_path()
    with session_lock:
        data = load_session()
        data.update(fields)
        if 'queues' in fields:
            # Старый формат с одной очередью больше не нужен
            for key in LEGACY_SESSION_KEYS:
                data.pop(key, None)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + '.tmp')
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
            os.replace(tmp, path)
        except OSError as e:
            add_log(f"⚠️ Не удалось сохранить настройки: {e}")

def save_queues():
    with session_lock:
        with lock:
            entries = [q.session_entry() for q in queues if q.keep_in_session]
        update_session(queues=entries)

def session_queue_entries(session):
    """Записи незавершённых очередей. Старый формат хранил одну очередь в корне файла."""
    if isinstance(session.get('queues'), list):
        entries = session['queues']
    elif session.get('active'):
        entries = [{'urls': session.get('urls'), 'dest': session.get('dest'),
                    'proxy': session.get('proxy'), 'flat': session.get('flat')}]
    else:
        entries = []
    valid = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get('urls'), list) or not entry.get('dest'):
            continue
        urls = [u for u in entry['urls'] if isinstance(u, str) and u.strip()]
        if urls:
            valid.append(dict(entry, urls=urls))
    return valid

def valid_max_active(value):
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_ACTIVE_LIMIT

def check_proxy(proxy: str):
    """Возвращает текст ошибки или None, если адрес прокси годится."""
    if not proxy:
        return None
    parsed = urllib.parse.urlparse(proxy)
    try:
        port = parsed.port
    except ValueError:
        port = None
    if parsed.scheme not in PROXY_SCHEMES or not parsed.hostname or not port:
        return "Адрес прокси должен выглядеть так: socks5h://127.0.0.1:1080 (без прокси оставьте поле пустым)"
    if parsed.scheme.startswith('socks') and socks is None:
        return "Для SOCKS-прокси нужен модуль PySocks: pip install PySocks"
    return None

dialog_lock = threading.Lock()

MAC_CHOOSE_FOLDER = """
on run argv
    set promptText to "Куда сохранить файлы?"
    tell application "System Events" to activate
    if (count of argv) > 0 then
        set p to choose folder with prompt promptText default location (POSIX file (item 1 of argv))
    else
        set p to choose folder with prompt promptText
    end if
    return POSIX path of p
end run
"""

WIN_CHOOSE_FOLDER = r"""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Add-Type -AssemblyName System.Windows.Forms
$dlg = New-Object System.Windows.Forms.FolderBrowserDialog
$dlg.Description = 'Куда сохранить файлы?'
$dlg.ShowNewFolderButton = $true
if ($env:YD_INITIAL_DIR -and (Test-Path -LiteralPath $env:YD_INITIAL_DIR)) { $dlg.SelectedPath = $env:YD_INITIAL_DIR }
$owner = New-Object System.Windows.Forms.Form -Property @{ TopMost = $true; ShowInTaskbar = $false }
if ($dlg.ShowDialog($owner) -eq [System.Windows.Forms.DialogResult]::OK) { Write-Output $dlg.SelectedPath }
$owner.Dispose()
"""

def choose_folder_dialog(initial: str = ''):
    """Открывает системное окно выбора папки. Возвращает путь или None, если пользователь нажал «Отмена»."""
    initial_dir = ''
    if initial:
        candidate = Path(initial).expanduser()
        # Если папки ещё нет, начинаем с ближайшей существующей родительской
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        if candidate.is_dir():
            initial_dir = str(candidate)

    if sys.platform == 'darwin':
        args = ['osascript', '-e', MAC_CHOOSE_FOLDER] + ([initial_dir] if initial_dir else [])
        r = subprocess.run(args, capture_output=True, text=True, encoding='utf-8')
        if r.returncode != 0:
            if '-128' in r.stderr:  # «Отмена»
                return None
            raise Exception(r.stderr.strip() or 'osascript завершился с ошибкой')
        return r.stdout.strip().rstrip('/') or '/'

    if os.name == 'nt':
        env = dict(os.environ, YD_INITIAL_DIR=initial_dir)
        r = subprocess.run(
            ['powershell', '-NoProfile', '-STA', '-ExecutionPolicy', 'Bypass', '-Command', WIN_CHOOSE_FOLDER],
            capture_output=True, env=env, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        out = r.stdout.decode('utf-8', errors='replace').strip().lstrip('﻿')
        if r.returncode != 0 and not out:
            raise Exception(r.stderr.decode('utf-8', errors='replace').strip() or 'PowerShell завершился с ошибкой')
        return out or None

    # Linux и прочие системы: пробуем tkinter
    import tkinter
    from tkinter import filedialog
    root = tkinter.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    path = filedialog.askdirectory(initialdir=initial_dir or None, title='Куда сохранить файлы?')
    root.destroy()
    return path or None

def interruptible_sleep(seconds, q):
    """Пауза между повторами, которую можно прервать кнопкой «Стоп»."""
    deadline = time.time() + seconds
    while time.time() < deadline and not q.state["should_stop"]:
        time.sleep(min(0.3, max(0, deadline - time.time())))

def backoff_delay(fails):
    return min(BACKOFF_MAX, BACKOFF_BASE * 2 ** max(0, fails - 1))

def set_notice(q, text=''):
    with lock:
        q.state["notice"] = text
        if text:
            q.state["speed_str"] = "0 MB/s"
            q.state["speed_mbs"] = 0.0

def with_retries(fn, what, q):
    """Сканирование: при обрыве связи ждём сколько нужно, а ошибки HTTP повторяем ограниченно."""
    fails = 0
    while True:
        if not q.gate():
            raise PermanentError("Остановлено пользователем")
        try:
            result = fn()
            set_notice(q)
            return result
        except (PermanentError, QuotaExceeded):
            raise
        except requests.HTTPError as e:
            raise PermanentError(str(e))
        except RetryableHTTP as e:
            fails += 1
            if fails >= SCAN_HTTP_RETRIES:
                raise PermanentError(str(e))
            delay = max(backoff_delay(fails), e.retry_after or 0)
            reason = str(e)
        except Exception as e:
            fails += 1
            delay = backoff_delay(fails)
            reason = str(e)
        add_log(f"   ⚠️ {what}: {reason}. Повтор через {delay:.0f} сек", q)
        set_notice(q, f"Нет связи, повтор через {delay:.0f} сек")
        interruptible_sleep(delay, q)

def add_progress(item, counted):
    """Запоминает, сколько байт файла уже учтено в общем прогрессе."""
    st = item['queue'].state
    with lock:
        st["downloaded_bytes"] += counted - item['counted']
        item['counted'] = counted
        total = st["total_bytes"]
        st["overall_percent"] = min(100.0, st["downloaded_bytes"] / total * 100) if total else 0.0

def learn_size(item, size):
    """Размер файла стал известен или изменился, пока файл лежал в очереди."""
    st = item['queue'].state
    delta = size - (item['size'] or 0)
    item['size'] = size
    with lock:
        st["total_bytes"] += delta
        st["file_total_mb"] = size / (1024 * 1024)
        fm = st["folders"][item['folder_idx']]
        fm['total_bytes'] += delta
        fm['total_gb'] = round(fm['total_bytes'] / (1024**3), 2)
        item['file']['size'] = size

def set_file_status(item, status, note=''):
    with lock:
        item['file']['status'] = status
        item['file']['note'] = note
        fm = item['queue'].state["folders"][item['folder_idx']]
        recount_folder(fm)
        if status == 'active':
            fm['status'] = 'active'

def recount_folder(fm):
    fm['deferred'] = sum(1 for f in fm['files'] if f['status'] == 'deferred')
    fm['failed'] = sum(1 for f in fm['files'] if f['status'] == 'failed')

def mark_file_done(item, status):
    st = item['queue'].state
    with lock:
        st["completed_list"].append({"name": item['dest_path'].name, "size": item['size'], "status": status})
        item['file']['status'] = 'done'
        item['file']['quota'] = False
        item['file']['note'] = status
        fm = st["folders"][item['folder_idx']]
        fm['done_files'] += 1
        fm['done_bytes'] += item['size'] or 0
        fm['done_gb'] = round(fm['done_bytes'] / (1024**3), 2)
        fm['status'] = 'done' if fm['done_files'] >= fm['total_files'] else 'active'
        recount_folder(fm)

def file_label(item):
    return f"[{item['idx']}/{item['queue'].state['total_files']}]"

def defer_item(item, reason, base_delay, max_delay):
    """Откладывает файл на потом: он не держит очередь, но и не теряется."""
    q = item['queue']
    now = time.time()
    manual = item['manual']
    item['manual'] = False
    item['first_defer'] = item['first_defer'] or now
    if not manual and now - item['first_defer'] > DEFER_GIVE_UP:
        return fail_item(item, f"{reason}: не удалось за {DEFER_GIVE_UP // 3600} ч")
    if manual and item['auto_try'] > now:
        # Неудачный ручной повтор не сдвигает автоматическую попытку и не удлиняет паузу
        item['next_try'] = item['auto_try']
    else:
        item['next_try'] = now + min(max_delay, base_delay * 2 ** item['defers'])
        item['defers'] += 1
    checked = time.strftime("%H:%M", time.localtime(now))
    when = time.strftime("%H:%M", time.localtime(item['next_try']))
    add_log(f"{file_label(item)} ⏳ {item['name']}: {reason}. Скачивание отклонено в {checked}. Отложен, повтор в {when}", q)
    with lock:
        item['file']['status'] = 'deferred'
        item['file']['quota'] = bool(item.get('quota'))
        item['file']['note'] = f"{reason}: скачивание отклонено в {checked}, повтор в {when}"
        recount_folder(q.state["folders"][item['folder_idx']])
        q.deferred.append(item)
    return 'deferred'

def fail_item(item, reason):
    q = item['queue']
    add_log(f"{file_label(item)} ❌ Не удалось скачать {item['name']}: {reason}", q)
    with lock:
        item['file']['status'] = 'failed'
        item['file']['note'] = reason
        recount_folder(q.state["folders"][item['folder_idx']])
    return 'failed'

def path_key(path):
    # На Mac и Windows имена файлов обычно не различают регистр
    return os.path.normcase(str(path)).casefold()

def claim_path(item):
    """Две очереди не должны писать в один .part, если их файлы совпали по пути."""
    key = path_key(item['dest_path'])
    with lock:
        if busy_paths.get(key, item['queue'].id) != item['queue'].id:
            return False
        busy_paths[key] = item['queue'].id
        return True

def release_path(item):
    with lock:
        busy_paths.pop(path_key(item['dest_path']), None)

def postpone_busy(item):
    """Этот файл качает другая очередь. Когда закончит, файл окажется уже скачанным."""
    q = item['queue']
    with lock:
        item['next_try'] = time.time() + BUSY_RETRY
        item['file']['status'] = 'deferred'
        item['file']['note'] = 'Этот файл сейчас качает другая очередь'
        recount_folder(q.state["folders"][item['folder_idx']])
        q.deferred.append(item)

def open_stream(item, start_byte):
    """Единственное место очереди, которое зависит от источника файла."""
    q = item['queue']
    if item['source'] == 'gdrive':
        return gdrive_open(item['gdrive_id'], item['resource_key'], start_byte, proxy=q.proxy)

    # Ссылка CDN живёт недолго, поэтому берём новую на каждую попытку
    href = get_download_link(item['public_key'], item['remote_path'], token=q.token)
    headers = {'Range': f'bytes={start_byte}-'} if start_byte > 0 else {}
    r = requests.get(href, headers=headers, stream=True, timeout=30)
    if r.status_code in (200, 206, 416):
        return r
    status = r.status_code
    retry_after = retry_after_seconds(r)
    r.close()
    if is_retryable_status(status):
        raise RetryableHTTP(f"HTTP статус: {status}", retry_after)
    raise PermanentError(f"HTTP статус: {status}")

def response_range(r):
    """Возвращает (первый байт, полный размер) ответа. Полный размер может быть неизвестен."""
    m = re.match(r'bytes (?:(\d+)-\d+|\*)/(\d+|\*)', r.headers.get('Content-Range', ''))
    if m:
        total = None if m.group(2) == '*' else int(m.group(2))
        return int(m.group(1) or 0), total
    length = r.headers.get('Content-Length')
    if r.status_code == 200 and length and length.isdigit() and not r.headers.get('Content-Encoding'):
        return 0, int(length)
    return 0, None

def download_item(item):
    """Качает один файл до конца. Итог: 'done', 'failed', 'deferred' или 'stopped'."""
    q = item['queue']
    st = q.state
    dest = item['dest_path']
    temp_path = dest.with_name(dest.name + ".part")
    label = file_label(item)

    def finish(status):
        add_progress(item, item['size'] or 0)
        mark_file_done(item, status)
        set_notice(q)
        return 'done'

    with lock:
        st["current_file_idx"] = item['idx']
        st["current_file"] = dest.name
        st["file_total_mb"] = (item['size'] or 0) / (1024 * 1024)
        st["file_downloaded_mb"] = 0.0
        st["file_percent"] = 0.0

    # Размер неизвестен (лимит Google при сканировании): проверим его по ответу сервера
    if item['size'] is not None and dest.exists() and dest.stat().st_size == item['size']:
        add_log(f"{label} ⏭️ Уже скачан: {dest.name}", q)
        return finish("Уже был")

    if item['quota']:
        # Снят ли лимит, покажут только настоящие данные файла: короткая проба его не видит
        set_file_status(item, 'active', 'проверяем лимит настоящим скачиванием')
    else:
        set_file_status(item, 'active')
    fails = 0        # подряд без единого нового байта
    http_fails = 0
    announced = False

    while True:
        if not q.gate():
            set_file_status(item, 'queued')
            return 'stopped'

        paused_mid_file = False
        got = 0
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            start_byte = temp_path.stat().st_size if temp_path.exists() else 0
            if item['size'] is not None and start_byte > item['size']:
                temp_path.unlink()
                start_byte = 0
            add_progress(item, start_byte)

            if not announced or start_byte:
                size_text = f"{item['size'] / (1024*1024):.1f} MB" if item['size'] is not None else "размер неизвестен"
                verb = f"↪️ Докачка с байта {start_byte}" if start_byte else "⬇️ Скачивание"
                add_log(f"{label} {verb}: {dest.name} ({size_text})", q)
                announced = True

            if not (item['size'] and start_byte == item['size']):
                t_start = time.time()
                with open_stream(item, start_byte) as r:
                    first_byte, total = response_range(r)
                    set_notice(q)

                    if r.status_code == 416:
                        # Просили байты за концом файла
                        if start_byte != (item['size'] if total is None else total):
                            if not start_byte:
                                raise RetryableHTTP("Сервер ответил HTTP 416")
                            temp_path.unlink()
                            add_progress(item, 0)
                            continue
                        if item['size'] is None:
                            learn_size(item, total)
                    else:
                        if total is not None and item['size'] is None:
                            learn_size(item, total)
                            if dest.exists() and dest.stat().st_size == total:
                                add_log(f"{label} ⏭️ Уже скачан: {dest.name}", q)
                                return finish("Уже был")
                        elif total is not None and total != item['size']:
                            # Файл заменили на Диске: старый кусок к нему не подходит
                            add_log(f"{label} ⚠️ Размер файла на Диске изменился: {item['size']} → {total}, качаем заново", q)
                            learn_size(item, total)
                            if temp_path.exists():
                                temp_path.unlink()
                            add_progress(item, 0)
                            continue

                        if r.status_code == 206:
                            if first_byte != start_byte:
                                raise Exception(f"Сервер отдал файл с байта {first_byte} вместо {start_byte}")
                            mode = 'ab'
                        else:
                            # Сервер отдал файл с начала: убираем из прогресса то, что считали раньше
                            mode = 'wb'
                            start_byte = 0
                            add_progress(item, 0)

                        expected_size = item['size'] or 0
                        current_file_downloaded = start_byte
                        with open(temp_path, mode) as f:
                            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                                if st["should_stop"]:
                                    break
                                if st["is_paused"] or not q.has_slot:
                                    # Рвём соединение: за долгую паузу CDN всё равно его закроет
                                    paused_mid_file = True
                                    break
                                if not chunk:
                                    continue
                                f.write(chunk)
                                c_len = len(chunk)
                                current_file_downloaded += c_len
                                got += c_len
                                add_progress(item, current_file_downloaded)
                                if item['quota']:
                                    item['quota'] = False
                                    set_file_status(item, 'active')
                                    add_log(f"{label} ✅ {dest.name}: лимит Google снят, файл отдаёт данные", q)

                                elapsed = time.time() - t_start
                                spd = got / elapsed if elapsed > 0 else 0
                                spd_mb = spd / (1024 * 1024)
                                remain_bytes = max(0, st["total_bytes"] - st["downloaded_bytes"])
                                eta = remain_bytes / spd if spd > 0 else None
                                pct = (current_file_downloaded / expected_size * 100) if expected_size > 0 else 0

                                with lock:
                                    st["file_percent"] = pct
                                    st["file_downloaded_mb"] = current_file_downloaded / (1024 * 1024)
                                    st["speed_mbs"] = spd_mb
                                    st["speed_str"] = f"{spd_mb:.1f} MB/s"
                                    st["eta_str"] = format_eta(eta)

            if st["should_stop"]:
                set_file_status(item, 'queued')
                return 'stopped'
            if paused_mid_file:
                # Пауза не считается неудачной попыткой
                continue

            on_disk = temp_path.stat().st_size if temp_path.exists() else 0
            if item['size'] and on_disk != item['size']:
                raise Exception(f"Соединение оборвалось: получено {on_disk} из {item['size']} байт")

            if not temp_path.exists():
                # Пустой файл: сервер мог не прислать ни одного байта
                temp_path.touch()
            os.replace(temp_path, dest)
            add_log(f"{label} ✅ Готово: {dest.name}", q)
            if item['size'] is None:
                learn_size(item, on_disk)
            return finish("Скачан")

        except PermanentError as e:
            set_notice(q)
            return fail_item(item, str(e))

        except QuotaExceeded:
            set_notice(q)
            item['quota'] = True
            return defer_item(item, "Дневной лимит скачиваний Google", QUOTA_RETRY, QUOTA_RETRY_MAX)

        except RetryableHTTP as e:
            if got:
                fails = http_fails = 0
            http_fails += 1
            if http_fails > FILE_HTTP_RETRIES:
                set_notice(q)
                item['quota'] = False
                return defer_item(item, str(e), DEFER_RETRY, QUOTA_RETRY_MAX)
            delay = max(backoff_delay(http_fails), e.retry_after or 0)
            reason = str(e)

        except Exception as e:
            # Обрыв связи, сон ноутбука, упавший туннель, отключённый диск: ждём, файл не бросаем
            if got:
                fails = http_fails = 0
            fails += 1
            delay = backoff_delay(fails)
            reason = str(e)

        on_disk = temp_path.stat().st_size if temp_path.exists() else 0
        add_log(f"   ⚠️ {reason}. На диске {on_disk} байт, повтор через {delay:.0f} сек", q)
        set_notice(q, f"Сбой, повтор через {delay:.0f} сек")
        interruptible_sleep(delay, q)

def scan_yandex_link(raw_url, q):
    token = q.token
    root_info = with_retries(lambda: get_public_resources(raw_url, limit=1, token=token), "API Яндекса", q)
    root_name = root_info.get('name') or 'Яндекс_Файлы'
    if root_info.get('type') == 'file':
        # Ссылка ведёт на один файл, а не на папку
        return {'name': root_name, 'is_folder': False, 'dirs': [], 'skipped': [],
                'files': [{'name': root_name, 'path': '', 'size': root_info.get('size', 0)}]}
    dirs = []
    files = with_retries(lambda: scan_folder_recursive(raw_url, token=token, dirs=dirs), "API Яндекса", q)
    return {'name': root_name, 'is_folder': True, 'dirs': sorted(set(dirs)), 'skipped': [], 'files': files}

def file_key(raw_url, f):
    return f"gdrive:{f['gdrive_id']}" if f.get('gdrive_id') else f"yandex:{raw_url}:{f['path']}"

def waiting_text(q):
    """Что показать, пока в очереди остались только отложенные файлы. Вызывать под lock."""
    quota = sum(1 for i in q.deferred if i['quota'])
    when = time.strftime("%H:%M", time.localtime(max(time.time(), min(i['next_try'] for i in q.deferred))))
    if quota == len(q.deferred):
        return f"Лимит Google: отложено файлов {quota}. Следующая проверка настоящим скачиванием в {when}"
    if quota:
        return f"Отложено файлов {len(q.deferred)}, из них по лимиту Google {quota}. Ближайший повтор в {when}"
    return f"Отложено файлов {len(q.deferred)}. Ближайший повтор в {when}"

def run_queue(q):
    st = q.state
    folders_meta = st["folders"]
    known = {}           # ключ файла → элемент очереди
    link_folders = {}    # ссылка → номер карточки
    failed_links = []
    pending = deque()
    failed_files = []
    skipped_docs = []

    def add_link(raw_url):
        """Сканирует ссылку и ставит в очередь файлы, которых в ней ещё нет. Возвращает их число."""
        if parse_gdrive_url(raw_url):
            scan = scan_gdrive_link(raw_url, {k.split(':', 1)[1] for k in known if k.startswith('gdrive:')}, q)
        else:
            scan = scan_yandex_link(raw_url, q)

        target_folder = q.dest
        if scan['is_folder'] and not q.flat:
            target_folder = q.dest / safe_rel_path(scan['name'])
        target_folder.mkdir(parents=True, exist_ok=True)
        # Создаём и пустые подпапки, чтобы структура совпадала с Диском
        for d in scan['dirs']:
            (target_folder / safe_rel_path(d)).mkdir(parents=True, exist_ok=True)

        new_files = [f for f in scan['files'] if file_key(raw_url, f) not in known]
        is_new_link = raw_url not in link_folders
        if is_new_link:
            add_log(f"📁 '{scan['name']}': найдено {len(scan['files'])} файлов", q)
            with lock:
                link_folders[raw_url] = len(folders_meta)
                folders_meta.append({
                    "name": scan['name'], "url": raw_url, "source": "gdrive" if parse_gdrive_url(raw_url) else "yandex",
                    "total_files": 0, "done_files": 0, "total_bytes": 0, "done_bytes": 0,
                    "total_gb": 0.0, "done_gb": 0.0, "status": "wait",
                    "deferred": 0, "failed": 0, "skipped": len(scan['skipped']), "files": []
                })
            for path in scan['skipped']:
                add_log(f"   ⚠️ Пропущен документ Google (его нельзя скачать файлом): {path}", q)
            skipped_docs.extend(scan['skipped'])
        elif new_files:
            add_log(f"🆕 '{scan['name']}': появилось новых файлов: {len(new_files)}", q)

        folder_idx = link_folders[raw_url]
        fm = folders_meta[folder_idx]
        for f in new_files:
            size = f['size']
            entry = {"name": f['path'] or f['name'], "size": size or 0, "status": "queued", "note": f.get('note', '')}
            if f.get('gdrive_id'):
                # Страница файла в Google: оттуда можно сделать копию, когда файл упёрся в лимит
                entry["url"] = GDRIVE_FILE_PAGE_URL.format(f['gdrive_id'])
            item = {
                "queue": q,
                "source": "gdrive" if f.get('gdrive_id') else "yandex",
                "public_key": raw_url,
                "folder_idx": folder_idx,
                "remote_path": f['path'],
                "gdrive_id": f.get('gdrive_id'),
                "resource_key": f.get('resource_key'),
                "dest_path": target_folder / (safe_rel_path(f['path']) if f['path'] else safe_rel_path(scan['name'])),
                "name": f['name'],
                "size": size,
                "file": entry,
                "counted": 0,
                "defers": 0,
                "first_defer": None,
                "next_try": 0,
                "auto_try": 0,       # Срок автоматического повтора, пока ждём ручной
                "manual": False,     # Нажали «Повторить сейчас»
                "quota": bool(f.get('quota')),  # Последний отказ был из-за лимита Google
            }
            known[file_key(raw_url, f)] = item
            pending.append(item)
            with lock:
                fm['files'].append(entry)
                fm['total_files'] += 1
                fm['total_bytes'] += size or 0
                fm['total_gb'] = round(fm['total_bytes'] / (1024**3), 2)
                if fm['status'] == 'done':
                    fm['status'] = 'active'
                st["total_files"] += 1
                st["total_bytes"] += size or 0
                item['idx'] = st["total_files"]
        return len(new_files)

    def queue_text():
        return f"Скачивание файлов (всего {st['total_files']} шт, {st['total_bytes'] / (1024**3):.2f} GB)"

    def set_phase(phase, text):
        with lock:
            st["phase"] = phase
            st["status_text"] = text

    for raw_url in q.urls:
        raw_url = raw_url.strip()
        if not raw_url:
            continue
        if not q.gate():
            break
        set_phase("scanning", "Сканирование ссылок...")
        add_log(f"🔍 Сканирование ссылки: {raw_url}", q)
        try:
            add_link(raw_url)
        except Exception as e:
            failed_links.append(raw_url)
            add_log(f"❌ Ошибка проверки ссылки {raw_url}: {e}", q)

    set_phase("downloading", queue_text())
    add_log(f"📦 Итого в очереди: {st['total_files']} файлов ({st['total_bytes'] / (1024**3):.2f} GB)", q)

    last_rescan = time.time()
    while not st["should_stop"]:
        now = time.time()
        with lock:
            due = [i for i in q.deferred if i['next_try'] <= now]
            q.deferred = [i for i in q.deferred if i['next_try'] > now]
        for item in due:
            # «Повторить сейчас» обгоняет ещё не начатые файлы
            if item['manual']:
                pending.appendleft(item)
            else:
                pending.append(item)

        if pending:
            if not q.gate():
                break
            set_phase("downloading", queue_text())
            item = pending.popleft()
            if not claim_path(item):
                postpone_busy(item)
                continue
            try:
                result = download_item(item)
            finally:
                release_path(item)
            if result == 'failed':
                failed_files.append(item['dest_path'].name)
            elif result == 'stopped':
                break
            continue

        # Очередь пуста: проверяем, не добавили ли в папки новые файлы.
        # Пока ждём лимит, делаем это только при свободном слоте, чтобы не мешать другим очередям
        has_deferred = bool(q.deferred)
        rescan_due = now - last_rescan >= RESCAN_INTERVAL and not st["is_paused"]
        if not has_deferred or (rescan_due and q.try_slot()):
            found = 0
            for raw_url in list(link_folders):
                if st["should_stop"]:
                    break
                try:
                    found += add_link(raw_url)
                except Exception as e:
                    add_log(f"⚠️ Не удалось перепроверить ссылку: {e}", q)
            last_rescan = time.time()
            if found:
                set_phase("downloading", queue_text())
                continue
            if not has_deferred:
                break

        # Остались только отложенные файлы: слот отдаём другим очередям
        q.release_slot()
        with lock:
            if q.deferred:
                st["phase"] = "limit" if any(i['quota'] for i in q.deferred) else "deferred"
                if not st["is_paused"]:
                    st["status_text"] = waiting_text(q)
                    st["eta_str"] = "--:--"
            st["current_file"] = ""
            st["file_percent"] = 0.0
            st["notice"] = ""
            st["speed_str"] = "0 MB/s"
            st["speed_mbs"] = 0.0
            soonest = min((i['next_try'] for i in q.deferred), default=0)
        q.wake.wait(min(5, max(0.05, soonest - time.time())))
        q.wake.clear()

    return failed_files, failed_links, skipped_docs

def download_worker(q):
    st = q.state
    add_log(f"🚀 Запуск очереди скачивания. Папка назначения: {q.dest}", q)
    if q.proxy:
        add_log("🌐 Google Drive: через прокси из настроек", q)

    failed_files, failed_links, skipped_docs = [], [], []
    crashed = False
    try:
        failed_files, failed_links, skipped_docs = run_queue(q)
    except Exception as e:
        crashed = True
        add_log(f"❌ Сбой программы: {e}", q)
    q.release_slot()

    problems = []
    if failed_files:
        problems.append(f"не скачано файлов: {len(failed_files)}")
    if failed_links:
        problems.append(f"не открылось ссылок: {len(failed_links)}")
    if skipped_docs:
        problems.append(f"пропущено документов Google: {len(skipped_docs)}")

    quitting = app_state["quitting"]
    with lock:
        st["is_running"] = False
        if not quitting:  # Очередь на паузе останется на паузе и после перезапуска
            st["is_paused"] = False
        if crashed:
            st["phase"] = "crashed"
            st["status_text"] = "Сбой программы, подробности в журнале очереди"
        elif st["should_stop"]:
            st["phase"] = "stopped"
            st["status_text"] = "Загрузка остановлена"
        elif problems:
            st["phase"] = "problems"
            st["status_text"] = "Завершено с ошибками: " + ", ".join(problems) + ". Нажмите «Запустить снова», чтобы докачать"
        else:
            st["phase"] = "done"
            st["status_text"] = "Загрузка завершена!"
        st["current_file"] = ""
        st["notice"] = ""
        st["speed_str"] = "0 MB/s"
        st["speed_mbs"] = 0.0
        st["eta_str"] = "--:--"
        # Незавершённую очередь продолжим при следующем запуске, если её не остановили кнопкой «Стоп»
        if not quitting and not crashed:
            q.keep_in_session = False
    save_queues()

    if st["should_stop"] and not quitting:
        add_log("⏹️ Загрузка остановлена пользователем.", q)
    if failed_files:
        add_log(f"⚠️ Не скачано файлов: {len(failed_files)}: " + ", ".join(failed_files[:20]) + (" …" if len(failed_files) > 20 else ""), q)
    add_log("🏁 Работа загрузчика завершена.", q)

# ==========================================
# УПРАВЛЕНИЕ ОЧЕРЕДЯМИ
# ==========================================

def find_queue(queue_id):
    with lock:
        return next((q for q in queues if q.id == queue_id), None)

def duplicate_of(urls, dest, exclude=None):
    """Вызывать под lock. Та же ссылка в ту же папку дважды только мешала бы сама себе."""
    wanted = {u.strip() for u in urls}
    for q in queues:
        if q is not exclude and (q.keep_in_session or q.state["is_running"]) \
                and path_key(q.dest) == path_key(dest) and wanted & {u.strip() for u in q.urls}:
            return q
    return None

def add_queue(urls, dest, token=None, proxy='', flat=False):
    """Добавляет очередь и запускает её. None: такая очередь уже есть."""
    q = DownloadQueue(urls, dest, token, proxy, flat)
    with lock:
        if duplicate_of(urls, dest):
            return None
        q.state["is_running"] = True  # Двойной клик не должен запустить две очереди
        queues.append(q)
    q.start()
    save_queues()
    return q

def pause_queue(q):
    with slot_changed:
        st = q.state
        if not st["is_running"] or st["should_stop"]:
            return False
        st["is_paused"] = True
        st["status_text"] = "Загрузка на паузе ⏸️"
        st["speed_str"] = "0 MB/s"
        st["speed_mbs"] = 0.0
        st["eta_str"] = "Пауза"
        # Слот освобождается сразу: другая очередь не ждёт, пока этот поток дочитает кусок
        q.has_slot = False
        q.wants_slot = False
        slot_changed.notify_all()
    add_log("⏸️ Загрузка поставлена на паузу.", q)
    save_queues()
    return True

def resume_queue(q):
    with slot_changed:
        st = q.state
        if not st["is_paused"] or not st["is_running"]:
            return False
        st["is_paused"] = False
        st["status_text"] = "Загрузка продолжается ▶️"
        st["eta_str"] = "--:--"
        slot_changed.notify_all()
    q.wake.set()
    add_log("▶️ Загрузка продолжена.", q)
    save_queues()
    return True

def stop_queue(q):
    with slot_changed:
        if not q.state["is_running"]:
            return False
        q.state["should_stop"] = True
        q.keep_in_session = False
        slot_changed.notify_all()
    q.wake.set()
    add_log("⏹️ Команда остановки получена.", q)
    save_queues()
    return True

def retry_deferred(q):
    """«Повторить сейчас»: отложенные файлы проверяются обычным скачиванием. Возвращает их число."""
    with lock:
        items = list(q.deferred)
        for item in items:
            if not item['manual']:
                item['auto_try'] = item['next_try']
                item['manual'] = True
            item['next_try'] = 0
            item['file']['note'] = 'Повтор запрошен: проверим настоящим скачиванием'
    q.wake.set()
    if items:
        add_log(f"🔁 Повторить сейчас: файлов {len(items)}", q)
    return len(items)

def restart_queue(q):
    """Запускает заново остановленную, завершённую с ошибками или не начатую очередь."""
    problem = check_proxy(q.proxy)
    if problem:
        return problem
    try:
        q.dest.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return f"Нет папки {q.dest}: {e}"
    with lock:
        if q.state["is_running"]:
            return "Очередь ещё работает"
        if duplicate_of(q.urls, q.dest, exclude=q):
            return "Эта ссылка уже качается в эту папку"
        q.state["is_running"] = True  # Двойной клик не должен запустить её дважды
    q.start()
    save_queues()
    return None

def remove_queue(q):
    with lock:
        if q.state["is_running"]:
            return False
        queues.remove(q)
    save_queues()
    return True

def set_max_active(value):
    with slot_changed:
        app_state["max_active"] = value
        slot_changed.notify_all()
    update_session(max_active=value)

def stop_all_for_quit():
    """Выключение программы: незавершённые очереди остаются в session.json."""
    with slot_changed:
        app_state["quitting"] = True
        for q in queues:
            if q.state["is_running"]:
                q.state["should_stop"] = True
            q.wake.set()
        slot_changed.notify_all()

# ==========================================
# ВЕБ-СЕРВЕР И API
# ==========================================

INDEX_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>YDisk Downloader</title>
  <style>
    :root {
      --bg: #0f1115;
      --card: #171a21;
      --card-2: #1e222b;
      --border: #2a303b;
      --accent: #ff6b4a;
      --accent-hover: #ff8566;
      --text: #e6e8ee;
      --muted: #8b93a3;
      --green: #3fb950;
      --yellow: #e3b341;
      --red: #f85149;
      --blue: #58a6ff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg); color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 15px; line-height: 1.45; padding: 28px 16px 60px;
    }
    .container { width: 100%; max-width: 820px; margin: 0 auto; display: flex; flex-direction: column; gap: 20px; }
    .hidden { display: none !important; }

    /* Шапка */
    .header { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-icon {
      width: 40px; height: 40px; border-radius: 10px; flex: none;
      background: linear-gradient(180deg, #ff8c5a, #e84834);
      display: grid; place-items: center; font-size: 20px; color: #fff;
    }
    .brand h1 { font-size: 20px; font-weight: 700; line-height: 1.2; }
    .brand p { font-size: 13px; color: var(--muted); }
    .header-right { display: flex; align-items: center; gap: 10px; }
    .version { font-size: 11px; color: var(--muted); }

    /* Карточки и формы */
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 20px; }
    .card-title { font-size: 16px; font-weight: 700; margin-bottom: 14px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .step { display: grid; grid-template-columns: 34px 1fr; gap: 12px; margin-bottom: 14px; }
    .step-num {
      width: 30px; height: 30px; border-radius: 50%; background: var(--card-2); border: 1px solid var(--border);
      color: var(--text); font-weight: 700; font-size: 14px; display: grid; place-items: center; margin-top: 1px;
    }
    .step-body { display: flex; flex-direction: column; gap: 8px; min-width: 0; }
    .step-label { font-size: 15px; font-weight: 600; }
    .hint { font-size: 13px; color: var(--muted); }
    .error-text { font-size: 13px; color: var(--red); }

    textarea, input[type="text"], input[type="password"], select {
      width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 10px;
      padding: 11px 14px; color: var(--text); font-size: 14px; outline: none; transition: border-color .2s;
      font-family: inherit;
    }
    textarea:focus, input:focus, select:focus { border-color: var(--accent); }
    textarea { resize: vertical; min-height: 58px; font-family: "SFMono-Regular", Consolas, monospace; font-size: 13px; }
    textarea.invalid, input.invalid { border-color: var(--red); }
    .dest-row { display: flex; gap: 8px; }
    .dest-row input { flex: 1; min-width: 0; }
    .check-row { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--muted); cursor: pointer; }
    .check-row input { width: 15px; height: 15px; accent-color: var(--accent); }

    button {
      font: inherit; font-weight: 600; font-size: 14px; border: none; border-radius: 10px; padding: 10px 16px;
      cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: all .15s; white-space: nowrap;
    }
    button:disabled { opacity: .4; cursor: not-allowed; }
    .btn-primary { background: var(--accent); color: #fff; font-size: 16px; padding: 13px 26px; }
    .btn-primary:hover:not(:disabled) { background: var(--accent-hover); transform: translateY(-1px); }
    .btn-secondary { background: var(--card-2); color: var(--text); border: 1px solid var(--border); }
    .btn-secondary:hover:not(:disabled) { border-color: var(--muted); }
    .btn-ghost { background: transparent; color: var(--muted); border: 1px solid var(--border); font-size: 13px; padding: 7px 12px; }
    .btn-ghost:hover:not(:disabled) { color: var(--text); border-color: var(--muted); }
    .btn-ghost.danger:hover:not(:disabled) { color: var(--red); border-color: var(--red); }
    .btn-green { background: rgba(63,185,80,.15); color: var(--green); border: 1px solid rgba(63,185,80,.35); }
    .btn-green:hover:not(:disabled) { background: rgba(63,185,80,.25); }
    .btn-yellow { background: rgba(227,179,65,.15); color: var(--yellow); border: 1px solid rgba(227,179,65,.35); }
    .btn-yellow:hover:not(:disabled) { background: rgba(227,179,65,.25); }
    .btn-red { background: rgba(248,81,73,.12); color: var(--red); border: 1px solid rgba(248,81,73,.35); }
    .btn-red:hover:not(:disabled) { background: rgba(248,81,73,.22); }

    .submit-row { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }

    details.advanced { border-top: 1px solid var(--border); margin-top: 16px; padding-top: 12px; }
    details.advanced summary { cursor: pointer; color: var(--muted); font-size: 14px; list-style: none; display: flex; align-items: center; gap: 6px; }
    details.advanced summary::-webkit-details-marker { display: none; }
    details.advanced summary::before { content: "▸"; font-size: 12px; transition: transform .15s; }
    details.advanced[open] summary::before { transform: rotate(90deg); }
    .advanced-body { display: grid; gap: 16px; margin-top: 16px; }
    .field { display: flex; flex-direction: column; gap: 6px; }
    .field label { font-size: 14px; font-weight: 600; }
    .field select { width: auto; min-width: 90px; }

    /* Список загрузок */
    .section-head { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; flex-wrap: wrap; }
    .section-head h2 { font-size: 17px; font-weight: 700; }
    .summary { font-size: 13px; color: var(--muted); }
    .empty { text-align: center; color: var(--muted); padding: 36px 20px; border: 1px dashed var(--border); border-radius: 14px; font-size: 14px; }

    .queue { display: flex; flex-direction: column; gap: 9px; padding: 14px 16px; border-left: 4px solid var(--border); }
    .queue.run { border-left-color: var(--green); }
    .queue.warn { border-left-color: var(--yellow); }
    .queue.bad { border-left-color: var(--red); }
    .queue.ok { border-left-color: var(--green); }
    .q-head { display: flex; align-items: center; gap: 10px; }
    .q-title { font-size: 15px; font-weight: 700; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .q-dest { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .q-actions { display: flex; gap: 4px; flex: none; }
    .q-actions button {
      width: 30px; height: 30px; padding: 0; justify-content: center; font-size: 14px; border-radius: 8px;
      background: transparent; color: var(--muted); border: 1px solid transparent;
    }
    .q-actions button:hover:not(:disabled) { background: var(--card-2); color: var(--text); border-color: var(--border); }
    .q-actions button.danger:hover:not(:disabled) { color: var(--red); }
    .pill {
      font-size: 12px; font-weight: 600; padding: 4px 10px; border-radius: 999px; white-space: nowrap; flex: none;
      background: var(--card-2); color: var(--muted); border: 1px solid var(--border);
    }
    .pill.run { background: rgba(63,185,80,.14); color: var(--green); border-color: rgba(63,185,80,.3); }
    .pill.ok { background: rgba(63,185,80,.14); color: var(--green); border-color: rgba(63,185,80,.3); }
    .pill.warn { background: rgba(227,179,65,.14); color: var(--yellow); border-color: rgba(227,179,65,.3); }
    .pill.bad { background: rgba(248,81,73,.12); color: var(--red); border-color: rgba(248,81,73,.3); }

    .bar { width: 100%; height: 6px; background: var(--bg); border-radius: 999px; overflow: hidden; }
    .bar > div { height: 100%; width: 0; border-radius: 999px; background: linear-gradient(90deg, var(--accent), #ffa756); transition: width .4s ease; }
    .queue.ok .bar > div { background: var(--green); }
    .meta { font-size: 13px; color: var(--muted); display: flex; flex-wrap: wrap; gap: 2px 6px; align-items: baseline; }
    .meta b { color: var(--text); font-weight: 600; }
    .meta .pct { font-size: 15px; font-weight: 700; color: var(--text); margin-right: 4px; }
    .current { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .notice { font-size: 12px; color: var(--muted); line-height: 1.4; }
    .notice.warn { color: var(--yellow); }
    .notice.bad { color: var(--red); }
    a.ext { color: var(--blue); text-decoration: none; white-space: nowrap; }
    a.ext:hover { text-decoration: underline; }

    details.sub { border-top: 1px solid var(--border); padding-top: 8px; }
    .queue details.sub { border-top: none; padding-top: 0; }
    details.sub summary { cursor: pointer; font-size: 13px; color: var(--muted); list-style: none; }
    details.sub summary::-webkit-details-marker { display: none; }
    details.sub summary::before { content: "▸ "; font-size: 11px; }
    details.sub[open] summary::before { content: "▾ "; }
    .folders { display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }
    .folder { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px; }
    .folder-head { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
    .folder-name { font-size: 14px; font-weight: 600; word-break: break-word; }
    .folder-info { font-size: 12px; color: var(--muted); margin-top: 2px; }
    .folder .bar { height: 4px; border: none; }
    details.files summary { cursor: pointer; font-size: 12px; color: var(--muted); }
    .file-rows { max-height: 220px; overflow-y: auto; margin-top: 6px; display: flex; flex-direction: column; gap: 2px; }
    .file-row { display: flex; justify-content: space-between; align-items: baseline; gap: 10px; font-size: 12px; padding: 3px 6px; border-radius: 6px; }
    .file-row.active { background: rgba(88,166,255,.1); }
    .file-row.done { color: var(--muted); }
    .file-row.deferred { color: var(--yellow); }
    .file-row.failed { color: var(--red); }
    .file-name { flex: 1 0 35%; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .file-info { color: var(--muted); text-align: right; }
    .terminal {
      margin-top: 8px; background: #0a0d13; border: 1px solid var(--border); border-radius: 10px; padding: 12px;
      font-family: "SFMono-Regular", Consolas, monospace; font-size: 12px; line-height: 1.6; color: var(--muted);
      max-height: 220px; overflow-y: auto; white-space: pre-wrap; word-break: break-word;
    }
    @media (max-width: 560px) {
      .card { padding: 16px; }
      .dest-row { flex-direction: column; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="brand">
        <div class="brand-icon">⬇</div>
        <div>
          <h1>YDisk Downloader</h1>
          <p>Скачивает папки с Яндекс Диска и Google Drive целиком</p>
        </div>
      </div>
      <div class="header-right">
        <span class="version">v1.5</span>
        <button type="button" id="quitBtn" class="btn-ghost danger" onclick="quitApp()" title="Остановить программу. Незавершённые загрузки продолжатся при следующем запуске">⏻ Выключить</button>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Новая загрузка</div>

      <div class="step">
        <div class="step-num">1</div>
        <div class="step-body">
          <div class="step-label">Вставьте ссылку на папку или файл</div>
          <textarea id="urlsInput" placeholder="https://disk.yandex.ru/d/…&#10;https://drive.google.com/drive/folders/…"></textarea>
          <div class="hint">Яндекс Диск или Google Drive. Если ссылок несколько, каждая с новой строки — они скачаются в одну папку.</div>
          <div class="error-text hidden" id="urlsError"></div>
        </div>
      </div>

      <div class="step">
        <div class="step-num">2</div>
        <div class="step-body">
          <div class="step-label">Куда сохранить</div>
          <div class="dest-row">
            <input type="text" id="destInput" value="" placeholder="/Volumes/SSD/Проект">
            <button type="button" id="chooseBtn" class="btn-secondary" onclick="chooseFolder()">📁 Выбрать папку…</button>
          </div>
          <label class="check-row"><input type="checkbox" id="flatInput"> Класть файлы прямо сюда, без подпапки с именем ссылки</label>
          <div class="error-text hidden" id="destError"></div>
        </div>
      </div>

      <div class="step" style="margin-bottom: 0;">
        <div class="step-num">3</div>
        <div class="step-body">
          <div class="submit-row">
            <button id="startBtn" class="btn-primary" onclick="startDownload()">⬇️ Скачать</button>
            <span class="hint">Можно добавлять новые ссылки, пока идёт загрузка</span>
          </div>
        </div>
      </div>

      <details class="advanced">
        <summary>Дополнительно: токен Яндекса, прокси, число одновременных загрузок</summary>
        <div class="advanced-body">
          <div class="field">
            <label for="tokenInput">Токен Яндекса</label>
            <input type="password" id="tokenInput" placeholder="y0__wgB…" autocomplete="off">
            <div class="hint">Нужен, только если Яндекс пишет «Превышен лимит скачивания». Как получить — в инструкции.</div>
          </div>
          <div class="field">
            <label for="proxyInput">Прокси для Google Drive</label>
            <input type="text" id="proxyInput" placeholder="socks5h://127.0.0.1:1080" autocomplete="off" spellcheck="false">
            <div class="hint">Пусто — соединение напрямую. Яндекс всегда качается напрямую.</div>
          </div>
          <div class="field">
            <label for="maxActive">Сколько загрузок идёт одновременно</label>
            <select id="maxActive" onchange="saveMaxActive()"></select>
            <div class="hint">Остальные ждут своей очереди. На внешнем диске больше двух обычно только мешают друг другу.</div>
          </div>
        </div>
      </details>
    </div>

    <div class="section-head">
      <h2>Загрузки</h2>
      <span class="summary" id="summary">Пока ничего не качается</span>
    </div>
    <div id="queuesList" style="display: flex; flex-direction: column; gap: 16px;"></div>
    <div id="emptyNote" class="empty">Здесь появятся ваши загрузки.<br>Вставьте ссылку выше и нажмите «Скачать».</div>

    <div class="card hidden" id="appLogCard">
      <details class="sub" open>
        <summary>Журнал программы</summary>
        <div class="terminal" id="appLogs"></div>
      </details>
    </div>
  </div>

  <script>
    let pollTimer = null;
    let anyRunning = false;
    const cards = new Map();  // id очереди → элементы её карточки

    // Подпись, цвет карточки и пояснение для каждого состояния очереди
    const STATUS = {
      waiting:     ["Ждёт своей очереди", "",     "Начнётся, когда освободится место. Число одновременных загрузок — в «Дополнительно»."],
      scanning:    ["Проверяю ссылку",     "run",  ""],
      downloading: ["Скачивается",         "run",  ""],
      limit:       ["Ждёт лимита Google",  "warn", "Google временно закрыл файл: его скачивали слишком часто. Лимит снимается примерно через сутки, программа повторит сама. Чтобы не ждать: откройте файл в Google под своим аккаунтом, нажмите «Файл → Создать копию» и вставьте ссылку на копию выше."],
      deferred:    ["Часть файлов отложена", "warn", "Некоторые файлы не отдались с первого раза. Программа вернётся к ним позже."],
      paused:      ["На паузе",            "warn", ""],
      stopping:    ["Останавливается",     "",     ""],
      stopped:     ["Остановлена",         "",     "Нажмите «Продолжить», чтобы докачать с того же места."],
      done:        ["Готово",              "ok",   ""],
      problems:    ["Готово, но с ошибками", "bad", "Часть файлов не скачалась. Нажмите «Продолжить», чтобы попробовать ещё раз."],
      crashed:     ["Сбой программы",      "bad",  "Подробности в журнале очереди. Нажмите «Продолжить», чтобы повторить."],
      blocked:     ["Нет папки или диска", "bad",  "Папка назначения недоступна. Подключите диск и нажмите «Проверить папку»."],
    };
    const ICON = { waiting: "⏳", scanning: "🔍", downloading: "⬇️", limit: "⏳", deferred: "⏳", paused: "⏸️",
                   stopping: "⏹️", stopped: "⏹️", done: "✅", problems: "⚠️", crashed: "❌", blocked: "🔌" };

    function formatSize(bytes) {
      if (!bytes) return "";
      return bytes >= 1024**3 ? `${(bytes / 1024**3).toFixed(2)} ГБ` : `${(bytes / 1024**2).toFixed(1)} МБ`;
    }

    function fileRow(f) {
      const labels = { queued: "в очереди", active: "⬇️ скачивается", done: "✅", deferred: "⏳ отложен", failed: "❌ ошибка" };
      const info = [labels[f.status] || f.status, f.note, formatSize(f.size)].filter(Boolean).join(" · ");
      const link = f.url && (f.status === "deferred" || f.status === "failed")
        ? ` <a class="ext" href="${esc(f.url)}" target="_blank" rel="noopener" title="Открыть страницу файла в Google">открыть в Google ↗</a>` : "";
      return `<div class="file-row ${esc(f.status)}"><span class="file-name">${esc(f.name)}</span><span class="file-info">${esc(info)}${link}</span></div>`;
    }

    // Имена файлов приходят из чужих папок — экранируем перед вставкой в HTML
    function esc(s) {
      return String(s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
    }

    document.addEventListener("DOMContentLoaded", () => {
      fetch("/api/default_dest")
        .then(r => r.json())
        .then(d => {
          if (d.path) document.getElementById("destInput").value = d.path;
          document.getElementById("proxyInput").value = d.proxy || "";
          document.getElementById("flatInput").checked = !!d.flat;
        });
      ["urlsInput", "destInput"].forEach(id => {
        document.getElementById(id).addEventListener("input", () => showError(id, ""));
      });
      pollStatus();
      pollTimer = setInterval(pollStatus, 1000);
    });

    function postJSON(url, data) {
      return fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data || {})
      });
    }

    function showError(inputId, message) {
      const input = document.getElementById(inputId);
      const box = document.getElementById(inputId.replace("Input", "Error"));
      input.classList.toggle("invalid", !!message);
      box.textContent = message;
      box.classList.toggle("hidden", !message);
    }

    function chooseFolder() {
      const btn = document.getElementById("chooseBtn");
      const input = document.getElementById("destInput");
      btn.disabled = true;
      postJSON("/api/choose_folder", { current: input.value.trim() })
        .then(r => r.json())
        .then(res => {
          if (res.status === "ok" && res.path) { input.value = res.path; showError("destInput", ""); }
          else if (res.status === "error") showError("destInput", res.message + " Путь можно вписать вручную.");
        })
        .catch(() => showError("destInput", "Не удалось открыть окно выбора папки. Путь можно вписать вручную."))
        .finally(() => { btn.disabled = false; });
    }

    function startDownload() {
      const urlsInput = document.getElementById("urlsInput");
      const urls = urlsInput.value.split("\\n").map(s => s.trim()).filter(Boolean);
      const dest = document.getElementById("destInput").value.trim();
      const token = document.getElementById("tokenInput").value.trim();
      const proxy = document.getElementById("proxyInput").value.trim();
      const flat = document.getElementById("flatInput").checked;

      let ok = true;
      if (!urls.length) { showError("urlsInput", "Вставьте хотя бы одну ссылку."); ok = false; }
      if (!dest) { showError("destInput", "Укажите папку, куда сохранять файлы."); ok = false; }
      if (!ok) return;

      const btn = document.getElementById("startBtn");
      btn.disabled = true;
      postJSON("/api/start", { urls, dest, token, proxy, flat }).then(r => r.json()).then(res => {
        if (res.status === "started") {
          urlsInput.value = "";
          pollStatus();
        } else {
          showError("urlsInput", "Не получилось добавить: " + res.message);
        }
      }).catch(() => showError("urlsInput", "Нет связи с программой. Запущена ли она?"))
        .finally(() => { btn.disabled = false; });
    }

    function queueAction(id, action) {
      postJSON("/api/" + action, { id })
        .then(r => r.json())
        .then(res => {
          if (res.status === "error") alert(res.message);
          pollStatus();
        });
    }

    function saveMaxActive() {
      postJSON("/api/settings", { max_active: Number(document.getElementById("maxActive").value) });
    }

    function quitApp() {
      const question = anyRunning
        ? "Идёт загрузка. Выключить программу? Незавершённые загрузки продолжатся при следующем запуске."
        : "Выключить программу?";
      if (!confirm(question)) return;
      clearInterval(pollTimer);
      postJSON("/api/shutdown").finally(() => {
        document.body.innerHTML = '<div style="margin: 80px auto; text-align: center; color: #8b93a3; font-size: 15px;">YDisk Downloader выключен. Вкладку можно закрыть.</div>';
      });
    }

    function createCard(id) {
      const el = document.createElement("div");
      el.className = "card queue";
      el.innerHTML = `
        <div class="q-head">
          <div class="q-title" data-ref="title"></div>
          <div class="pill" data-ref="badge"></div>
          <div class="q-actions">
            <button data-action="pause" title="Пауза">⏸</button>
            <button data-action="resume" title="Продолжить">▶</button>
            <button data-action="retry" title="Повторить отложенные файлы сейчас. Если лимит ещё действует, они останутся отложенными">🔁</button>
            <button data-action="stop" class="danger" title="Стоп">⏹</button>
            <button data-action="restart" title="Продолжить">▶</button>
            <button data-action="remove" class="danger" title="Убрать из списка. Файлы на диске останутся">✕</button>
          </div>
        </div>
        <div class="bar"><div data-ref="overallFill"></div></div>
        <div class="meta" data-ref="meta"></div>
        <div class="current hidden" data-ref="current"></div>
        <div class="notice hidden" data-ref="notice"></div>
        <details class="sub">
          <summary>Подробнее</summary>
          <div class="q-dest" data-ref="dest" style="margin-top: 8px; white-space: normal; word-break: break-all;"></div>
          <div class="folders" data-ref="folders"></div>
          <details class="sub" style="margin-top: 10px;">
            <summary>Журнал этой загрузки</summary>
            <div class="terminal" data-ref="log"></div>
          </details>
        </details>`;
      const refs = { root: el, lastFolders: "", buttons: {} };
      el.querySelectorAll("[data-ref]").forEach(n => { refs[n.dataset.ref] = n; });
      el.querySelectorAll("[data-action]").forEach(btn => {
        refs.buttons[btn.dataset.action] = btn;
        btn.addEventListener("click", () => {
          if (btn.dataset.action === "stop" && !confirm("Остановить загрузку? Скачанное останется на диске, но после перезапуска программы она сама не продолжится.")) return;
          queueAction(id, btn.dataset.action);
        });
      });
      document.getElementById("queuesList").appendChild(el);
      return refs;
    }

    function folderCard(f, idx, q, openLists) {
      const pct = f.total_bytes ? ((f.done_bytes / f.total_bytes) * 100).toFixed(0) : (f.total_files ? ((f.done_files / f.total_files) * 100).toFixed(0) : 0);
      const problems = [];
      if (f.deferred) problems.push(`⏳ отложено ${f.deferred}`);
      if (f.failed) problems.push(`❌ ошибок ${f.failed}`);
      if (f.skipped) problems.push(`пропущено документов Google: ${f.skipped}`);
      const isDone = f.status === "done" || f.done_files >= f.total_files;
      const isActive = f.files.some(x => x.status === "active");
      let cls = "", text = "В очереди";
      if (isDone) { cls = "ok"; text = "✅ Готово"; }
      else if (!q.is_running) { text = `${pct}% · не завершено`; }
      else if (isActive) { cls = "run"; text = `⬇️ ${pct}%`; }
      else if (f.deferred) { cls = "warn"; text = `⏳ ${pct}% · отложено`; }
      else if (f.done_files > 0) { text = `${pct}%`; }
      return `
        <div class="folder">
          <div class="folder-head">
            <div style="min-width: 0;">
              <div class="folder-name">${esc(f.name)}</div>
              <div class="folder-info">${f.done_files} из ${f.total_files} файлов · ${f.done_gb} / ${f.total_gb} ГБ${problems.length ? " · " + esc(problems.join(", ")) : ""}</div>
            </div>
            <span class="pill ${cls}">${text}</span>
          </div>
          <div class="bar"><div style="width: ${pct}%; ${isDone ? "background: var(--green);" : ""}"></div></div>
          <details class="files" data-idx="${idx}"${openLists.has(String(idx)) ? " open" : ""}>
            <summary>Список файлов (${f.total_files})</summary>
            <div class="file-rows">${f.files.map(fileRow).join("")}</div>
          </details>
        </div>`;
    }

    function renderFolders(refs, q) {
      const key = JSON.stringify([q.folders, q.is_running]);
      if (key === refs.lastFolders) return;
      refs.lastFolders = key;
      // Карточки перерисовываются заново: сохраняем раскрытые списки и их прокрутку
      const openLists = new Map();
      refs.folders.querySelectorAll("details[open]").forEach(d => openLists.set(d.dataset.idx, d.querySelector(".file-rows").scrollTop));
      refs.folders.innerHTML = q.folders.map((f, idx) => folderCard(f, idx, q, openLists)).join("");
      refs.folders.querySelectorAll("details[open]").forEach(d => {
        d.querySelector(".file-rows").scrollTop = openLists.get(d.dataset.idx) || 0;
      });
    }

    function renderQueue(q) {
      let refs = cards.get(q.id);
      if (!refs) {
        refs = createCard(q.id);
        cards.set(q.id, refs);
      }
      const name = q.folders.length ? q.folders.map(f => f.name).join(", ") : q.urls[0];
      refs.title.textContent = name;
      refs.title.title = name;
      refs.dest.textContent = "📁 " + q.dest + (q.urls.length > 1 ? ` · ссылок: ${q.urls.length}` : "");

      const [label, cls, explain] = STATUS[q.display] || [q.display, "", ""];
      refs.root.className = "card queue " + cls;
      refs.badge.className = "pill " + cls;
      refs.badge.textContent = `${ICON[q.display] || ""} ${label}`.trim();

      const stopping = q.display === "stopping";
      const b = refs.buttons;
      b.pause.classList.toggle("hidden", !q.is_running || q.is_paused || stopping);
      b.resume.classList.toggle("hidden", !q.is_running || !q.is_paused || stopping);
      b.retry.classList.toggle("hidden", !q.is_running || !q.deferred_files || stopping);
      b.retry.disabled = q.is_paused;
      b.stop.classList.toggle("hidden", !q.is_running || stopping);
      b.restart.classList.toggle("hidden", q.is_running || q.display === "done");
      b.restart.title = q.display === "blocked" ? "Проверить папку и продолжить" : "Продолжить";
      b.remove.classList.toggle("hidden", q.is_running);

      // Одна строка фактов под полосой
      refs.overallFill.style.width = `${q.overall_percent}%`;
      const meta = [`<span class="pct">${q.overall_percent.toFixed(q.overall_percent >= 99.95 ? 0 : 1)}%</span>`];
      if (q.total_files) meta.push(`<b>${q.completed_list.length}</b> из ${q.total_files} файлов`);
      if (q.total_bytes) {
        const unit = q.total_bytes >= 1024**3 ? 1024**3 : 1024**2;
        const digits = q.total_bytes >= 10 * unit ? 0 : 1;
        meta.push(`<b>${(q.downloaded_bytes / unit).toFixed(digits)}</b> из ${(q.total_bytes / unit).toFixed(digits)} ${unit === 1024**3 ? "ГБ" : "МБ"}`);
      }
      if (q.display === "downloading" && q.speed_mbs > 0) {
        meta.push(`<b>${q.speed_str}</b>`);
        if (q.eta_str && q.eta_str !== "--:--") meta.push(`осталось <b>${esc(q.eta_str)}</b>`);
      }
      refs.meta.innerHTML = meta.join("<span style='opacity:.4'>·</span>");

      const hasCurrent = q.is_running && q.current_file && !q.is_paused;
      refs.current.classList.toggle("hidden", !hasCurrent);
      if (hasCurrent) {
        refs.current.textContent = `⬇ ${q.current_file} · ${q.file_percent.toFixed(0)}%`;
      }

      // Пояснение: что происходит и что делать. Технические детали сервера — только если они есть
      const parts = [];
      if (explain) parts.push(explain);
      if (q.notice) parts.push(q.notice);
      if (q.status_text && !["downloading", "done", "scanning", "paused"].includes(q.display) && !parts.includes(q.status_text)) parts.push(q.status_text);
      refs.notice.textContent = parts.join(" ");
      if (q.display === "limit") {
        // Первый файл с лимитом: кнопка, чтобы сразу открыть его и сделать копию
        const quotaFiles = q.folders.flatMap(f => f.files).filter(x => x.quota && x.url);
        if (quotaFiles.length) {
          refs.notice.innerHTML = esc(parts.join(" ")) +
            ` <a class="ext" href="${esc(quotaFiles[0].url)}" target="_blank" rel="noopener">Открыть ${quotaFiles.length > 1 ? "первый из " + quotaFiles.length + " файлов" : "файл"} в Google ↗</a>`;
        }
      }
      refs.notice.className = "notice " + (cls === "bad" || cls === "warn" ? cls : "");
      refs.notice.classList.toggle("hidden", !parts.length);

      renderFolders(refs, q);
      const text = q.logs.join("\\n");
      if (refs.log.textContent !== text) {
        refs.log.textContent = text;
        refs.log.scrollTop = refs.log.scrollHeight;
      }
    }

    function renderSummary(data) {
      const count = name => data.queues.filter(q => q.display === name).length;
      const parts = [];
      const active = count("downloading") + count("scanning");
      if (active) parts.push(`качается ${active} из ${data.max_active} возможных`);
      if (count("waiting")) parts.push(`ждут: ${count("waiting")}`);
      if (count("limit") + count("deferred")) parts.push(`отложено: ${count("limit") + count("deferred")}`);
      if (count("paused")) parts.push(`на паузе: ${count("paused")}`);
      if (count("done")) parts.push(`готово: ${count("done")}`);
      const el = document.getElementById("summary");
      el.textContent = parts.length ? parts.join(" · ") : (data.queues.length ? "Все загрузки остановлены" : "Пока ничего не качается");
    }

    function pollStatus() {
      fetch("/api/status")
        .then(r => r.json())
        .then(data => {
          anyRunning = data.queues.some(q => q.is_running);
          const select = document.getElementById("maxActive");
          if (!select.options.length) {
            for (let i = 1; i <= data.max_active_limit; i++) select.add(new Option(String(i), String(i)));
          }
          if (document.activeElement !== select) select.value = String(data.max_active);

          const alive = new Set(data.queues.map(q => q.id));
          cards.forEach((refs, id) => {
            if (!alive.has(id)) {
              refs.root.remove();
              cards.delete(id);
            }
          });
          data.queues.forEach(renderQueue);
          document.getElementById("emptyNote").classList.toggle("hidden", data.queues.length > 0);
          renderSummary(data);

          document.getElementById("appLogCard").classList.toggle("hidden", !data.logs.length);
          document.getElementById("appLogs").textContent = data.logs.join("\\n");
        })
        .catch(() => {
          // Сервер выключен или перезапускается
          document.getElementById("summary").textContent = "🔌 Нет связи с программой";
        });
    }
  </script>
</body>
</html>
"""

ALLOWED_HOSTS = {f"{HOST}:{PORT}", f"localhost:{PORT}"}

class RequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, message, status=400):
        self._send_json({"status": "error", "message": message}, status)

    def _is_trusted(self):
        # Защита от DNS rebinding: страница должна быть открыта именно с нашего адреса
        if self.headers.get('Host', '') not in ALLOWED_HOSTS:
            return False
        # Защита от запросов с чужих сайтов: Origin, если он есть, должен совпадать с нашим
        origin = self.headers.get('Origin')
        if origin and urllib.parse.urlparse(origin).netloc not in ALLOWED_HOSTS:
            return False
        return True

    def do_GET(self):
        if not self._is_trusted():
            self.send_error(403)
            return
        if self.path in ('/', '/index.html'):
            body = INDEX_HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/api/default_dest':
            default_dir = Path.home() / "Downloads" / "Yandex_Disk"
            session = load_session()
            self._send_json({
                "path": session.get('dest') or str(default_dir),
                "proxy": session.get('proxy') or '',
                "flat": bool(session.get('flat')),
            })
        elif self.path == '/api/status':
            # Сериализуем под замком, а отправляем без него, чтобы не тормозить загрузку
            with lock:
                snapshot = json.loads(json.dumps({
                    "queues": [q.snapshot() for q in queues],
                    "max_active": app_state["max_active"],
                    "max_active_limit": MAX_ACTIVE_LIMIT,
                    "logs": app_state["logs"],
                }, ensure_ascii=False))
            self._send_json(snapshot)
        else:
            self.send_error(404)

    def do_POST(self):
        # Чужой сайт может отправить простой POST без preflight, но не с application/json
        if not self._is_trusted() or not self.headers.get('Content-Type', '').startswith('application/json'):
            self.send_error(403)
            return
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            payload = json.loads(self.rfile.read(content_length).decode('utf-8') or '{}')
        except (ValueError, UnicodeDecodeError):
            payload = None
        if not isinstance(payload, dict):
            self._send_error_json("Некорректный запрос")
            return

        if self.path == '/api/start':
            self._start_queue(payload)
        elif self.path in ('/api/pause', '/api/resume', '/api/stop', '/api/retry', '/api/restart', '/api/remove'):
            q = find_queue(payload.get('id'))
            if q is None:
                self._send_error_json("Такой очереди нет", 404)
            else:
                self._queue_action(self.path.rsplit('/', 1)[1], q)
        elif self.path == '/api/settings':
            value = payload.get('max_active')
            if not valid_max_active(value):
                self._send_error_json(f"Число очередей: от 1 до {MAX_ACTIVE_LIMIT}")
                return
            set_max_active(value)
            self._send_json({"status": "ok", "max_active": value})
        elif self.path == '/api/shutdown':
            stop_all_for_quit()
            add_log("⏻ Программа выключается.")
            self._send_json({"status": "bye"})
            # shutdown() ждёт конца serve_forever, поэтому зовём его из отдельного потока
            threading.Thread(target=self.server.shutdown, daemon=True).start()
        elif self.path == '/api/choose_folder':
            # Второе окно поверх первого не открываем
            if not dialog_lock.acquire(blocking=False):
                self._send_json({"status": "error", "message": "Окно выбора папки уже открыто"}, 409)
                return
            try:
                path = choose_folder_dialog(str(payload.get('current', '')))
            except Exception as e:
                self._send_json({"status": "error", "message": f"Не удалось открыть окно выбора папки: {e}"}, 500)
                return
            finally:
                dialog_lock.release()
            self._send_json({"status": "ok", "path": path})
        else:
            self.send_error(404)

    def _start_queue(self, payload):
        raw_urls = payload.get('urls', [])
        urls = [u.strip() for u in raw_urls if isinstance(u, str) and u.strip()] if isinstance(raw_urls, list) else []
        dest_str = str(payload.get('dest', '')).strip()
        token = str(payload.get('token', '')).strip() or None
        proxy = str(payload.get('proxy', '')).strip()
        flat = bool(payload.get('flat'))
        proxy_error = check_proxy(proxy)
        if proxy_error:
            self._send_error_json(proxy_error)
            return
        if not urls or not dest_str:
            self._send_error_json("Нужны ссылки и папка для сохранения")
            return
        try:
            dest_path = Path(dest_str).expanduser().resolve()
            dest_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self._send_error_json(f"Не удалось создать папку: {e}")
            return

        q = add_queue(urls, dest_path, token, proxy, flat)
        if q is None:
            self._send_error_json("Эта ссылка уже качается в эту папку")
            return
        update_session(dest=str(dest_path), proxy=proxy, flat=flat)
        self._send_json({"status": "started", "id": q.id})

    def _queue_action(self, action, q):
        if action == 'pause':
            done = pause_queue(q)
            error = None if done else "Нечего ставить на паузу"
        elif action == 'resume':
            done = resume_queue(q)
            error = None if done else "Очередь не на паузе"
        elif action == 'stop':
            done = stop_queue(q)
            error = None if done else "Очередь уже не работает"
        elif action == 'retry':
            if q.state["is_paused"]:
                error = "Очередь на паузе: сначала нажмите «Продолжить»"
            else:
                error = None if retry_deferred(q) else "Нет отложенных файлов"
        elif action == 'restart':
            error = restart_queue(q)
        else:
            error = None if remove_queue(q) else "Сначала остановите очередь"

        if error:
            self._send_error_json(error)
        else:
            self._send_json({"status": "ok"})

def resume_unfinished():
    """Продолжает очереди, прерванные выключением программы, сбоем или перезагрузкой."""
    session = load_session()
    if valid_max_active(session.get('max_active')):
        app_state["max_active"] = session['max_active']

    restored = []
    for entry in session_queue_entries(session):
        proxy = entry.get('proxy') or ''
        q = DownloadQueue(entry['urls'], entry['dest'], None, proxy, bool(entry.get('flat')),
                          queue_id=entry.get('id'), paused=bool(entry.get('paused')),
                          had_token=bool(entry.get('had_token')))
        problem = check_proxy(proxy)
        if not problem:
            try:
                q.dest.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                # Например, не подключён внешний диск. Запись в настройках остаётся до следующего запуска
                problem = f"нет папки {q.dest}: {e}"
        restored.append((q, problem))
    with lock:
        queues.extend(q for q, _ in restored)

    # Сначала все очереди в списке, потом запуск: слоты выдаются в порядке добавления
    for q, problem in restored:
        if problem:
            q.block(problem)
            continue
        q.start()
        if q.had_token:
            add_log("⚠️ Токен Яндекса не сохраняется. Если файлы не скачаются, добавьте ссылку заново с токеном.", q)
    save_queues()

def run_server():
    server = ThreadingHTTPServer((HOST, PORT), RequestHandler)
    resume_unfinished()
    url = f"http://{HOST}:{PORT}"
    print(f"\n========================================================")
    print(f"🚀 YDisk Downloader запущен!")
    print(f"🌐 Откройте в браузере: {url}")
    print(f"========================================================\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановка сервера...")
    finally:
        server.server_close()
    print("👋 YDisk Downloader выключен.")

if __name__ == '__main__':
    run_server()
