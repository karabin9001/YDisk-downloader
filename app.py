#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yandex Turbo Downloader — Локальный веб-инструмент пакетного скачивания файлов с Яндекс.Диска.
Работает на Mac и Windows, скачивает напрямую с CDN на максимальной скорости с автодокачкой,
поддерживает кнопки Старт / Пауза / Стоп.
"""

import os
import sys
import io
import time
import json
import threading
import urllib.parse
import webbrowser
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# Настройка безопасной кодировки консоли
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

try:
    import requests
except ImportError:
    print("Установка библиотеки requests...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

HOST = "127.0.0.1"
PORT = 7860

# Глобальное состояние загрузчика
state = {
    "is_running": False,
    "is_paused": False,
    "should_stop": False,
    "status_text": "Готов к работе",
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

def add_log(msg: str):
    timestamp = time.strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    with lock:
        state["logs"].append(formatted)
        if len(state["logs"]) > 200:
            state["logs"].pop(0)

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
    raise Exception(f"Яндекс не выдал ссылку на файл (HTTP {r.status_code}). {reason}".strip())

def wait_while_paused():
    while state["is_paused"] and not state["should_stop"]:
        time.sleep(0.3)

def mark_file_done(item, status):
    with lock:
        state["completed_list"].append({"name": item['dest_path'].name, "size": item['size'], "status": status})
        fm = state["folders"][item['folder_idx']]
        fm['done_files'] += 1
        fm['done_gb'] = round(fm['done_gb'] + (item['size'] / (1024**3)), 2)
        fm['status'] = 'done' if fm['done_files'] >= fm['total_files'] else 'active'

def download_worker(urls: list, dest_dir: Path, token: str = None):
    with lock:
        state["is_running"] = True
        state["is_paused"] = False
        state["should_stop"] = False
        state["status_text"] = "Сканирование ссылок на Яндекс Диске..."
        state["completed_list"] = []
        state["folders"] = []
        state["logs"] = []
        state["downloaded_bytes"] = 0
        state["overall_percent"] = 0.0

    add_log(f"🚀 Запуск очереди скачивания. Папка назначения: {dest_dir}")

    all_files_queue = []
    folders_meta = []

    for raw_url in urls:
        raw_url = raw_url.strip()
        if not raw_url:
            continue
        if state["should_stop"]:
            break
        add_log(f"🔍 Сканирование ссылки: {raw_url}")
        try:
            root_info = get_public_resources(raw_url, limit=1, token=token)
            root_name = root_info.get('name') or 'Яндекс_Файлы'

            if root_info.get('type') == 'file':
                # Ссылка ведёт на один файл, а не на папку
                target_folder = dest_dir
                files = [{'name': root_name, 'path': '', 'size': root_info.get('size', 0)}]
                dirs = []
            else:
                target_folder = dest_dir / safe_rel_path(root_name)
                dirs = []
                files = scan_folder_recursive(raw_url, token=token, dirs=dirs)

            target_folder.mkdir(parents=True, exist_ok=True)
            # Создаём и пустые подпапки, чтобы структура совпадала с Диском
            for d in dirs:
                (target_folder / safe_rel_path(d)).mkdir(parents=True, exist_ok=True)

            add_log(f"📁 '{root_name}': найдено {len(files)} файлов")

            folder_idx = len(folders_meta)
            folders_meta.append({
                "name": root_name,
                "url": raw_url,
                "total_files": len(files),
                "done_files": 0,
                "total_gb": round(sum(f['size'] for f in files) / (1024**3), 2),
                "done_gb": 0.0,
                "status": "wait"
            })

            for f in files:
                dest = target_folder / (safe_rel_path(f['path']) if f['path'] else safe_rel_path(root_name))
                all_files_queue.append({
                    "public_key": raw_url,
                    "folder_idx": folder_idx,
                    "remote_path": f['path'],
                    "dest_path": dest,
                    "name": f['name'],
                    "size": f['size']
                })
        except Exception as e:
            add_log(f"❌ Ошибка проверки ссылки {raw_url}: {e}")

    total_files = len(all_files_queue)
    total_bytes = sum(f['size'] for f in all_files_queue)

    with lock:
        state["total_files"] = total_files
        state["total_bytes"] = total_bytes
        state["folders"] = folders_meta
        state["status_text"] = f"Скачивание файлов (всего {total_files} шт, {total_bytes / (1024**3):.2f} GB)"

    add_log(f"📦 Итого в очереди: {total_files} файлов ({total_bytes / (1024**3):.2f} GB)")

    downloaded_total_bytes = 0
    failed_files = []
    max_attempts = 4

    def update_overall():
        with lock:
            state["downloaded_bytes"] = downloaded_total_bytes
            state["overall_percent"] = (downloaded_total_bytes / total_bytes * 100) if total_bytes else 100

    for idx, item in enumerate(all_files_queue, 1):
        wait_while_paused()
        if state["should_stop"]:
            add_log("⏹️ Загрузка остановлена пользователем.")
            break

        dest = item['dest_path']
        expected_size = item['size']

        with lock:
            state["current_file_idx"] = idx
            state["current_file"] = dest.name
            state["file_total_mb"] = expected_size / (1024 * 1024)
            state["file_percent"] = 0.0

        # Если уже скачан полностью
        if dest.exists() and dest.stat().st_size == expected_size:
            downloaded_total_bytes += expected_size
            add_log(f"[{idx}/{total_files}] ⏭️ Уже скачан: {dest.name}")
            mark_file_done(item, "Уже был")
            update_overall()
            continue

        # Скачивание файла
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_path = dest.with_name(dest.name + ".part")
        start_byte = 0
        if temp_path.exists():
            start_byte = temp_path.stat().st_size
            if expected_size and start_byte > expected_size:
                temp_path.unlink()
                start_byte = 0

        # Сколько байт этого файла уже учтено в общем прогрессе
        file_counted = start_byte
        downloaded_total_bytes += start_byte

        add_log(f"[{idx}/{total_files}] ⬇️ Скачивание: {dest.name} ({expected_size / (1024*1024):.1f} MB)")
        success = False
        attempt = 0

        while attempt < max_attempts:
            wait_while_paused()
            if state["should_stop"]:
                break

            paused_mid_file = False
            try:
                if not (expected_size and start_byte == expected_size):
                    # Ссылка CDN живёт недолго, поэтому берём новую на каждую попытку
                    href = get_download_link(item['public_key'], item['remote_path'], token=token)

                    headers = {}
                    if start_byte > 0:
                        headers['Range'] = f'bytes={start_byte}-'

                    t_start = time.time()
                    bytes_in_session = 0

                    with requests.get(href, headers=headers, stream=True, timeout=30) as r:
                        if r.status_code not in (200, 206):
                            raise Exception(f"HTTP статус: {r.status_code}")

                        if r.status_code == 206:
                            mode = 'ab'
                            current_file_downloaded = start_byte
                        else:
                            # Сервер отдал файл с начала: убираем из прогресса то, что считали раньше
                            mode = 'wb'
                            current_file_downloaded = 0
                            start_byte = 0
                            downloaded_total_bytes -= file_counted
                            file_counted = 0

                        with open(temp_path, mode) as f:
                            for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                                if state["should_stop"]:
                                    break
                                if state["is_paused"]:
                                    # Рвём соединение: за долгую паузу CDN всё равно его закроет
                                    paused_mid_file = True
                                    break
                                if not chunk:
                                    continue
                                f.write(chunk)
                                c_len = len(chunk)
                                current_file_downloaded += c_len
                                bytes_in_session += c_len
                                downloaded_total_bytes += c_len
                                file_counted += c_len

                                elapsed = time.time() - t_start
                                spd = bytes_in_session / elapsed if elapsed > 0 else 0
                                spd_mb = spd / (1024 * 1024)
                                remain_bytes = max(0, total_bytes - downloaded_total_bytes)
                                eta = remain_bytes / spd if spd > 0 else None
                                pct = (current_file_downloaded / expected_size * 100) if expected_size > 0 else 0

                                with lock:
                                    state["file_percent"] = pct
                                    state["file_downloaded_mb"] = current_file_downloaded / (1024 * 1024)
                                    state["speed_mbs"] = spd_mb
                                    state["speed_str"] = f"{spd_mb:.1f} MB/s"
                                    state["eta_str"] = format_eta(eta)
                                update_overall()

                if state["should_stop"]:
                    break

                start_byte = temp_path.stat().st_size if temp_path.exists() else 0
                if paused_mid_file:
                    # Пауза не считается неудачной попыткой
                    continue

                if expected_size and start_byte != expected_size:
                    raise Exception(f"Соединение оборвалось: получено {start_byte} из {expected_size} байт")

                if not temp_path.exists():
                    # Пустой файл: сервер мог не прислать ни одного байта
                    temp_path.touch()
                os.replace(temp_path, dest)
                add_log(f"[{idx}/{total_files}] ✅ Готово: {dest.name}")
                mark_file_done(item, "Скачан")
                success = True
                break

            except Exception as e:
                attempt += 1
                add_log(f"   ⚠️ Сбой (попытка {attempt}/{max_attempts}): {e}")
                if temp_path.exists():
                    start_byte = temp_path.stat().st_size
                if attempt < max_attempts:
                    time.sleep(3 * attempt)

        if not success and not state["should_stop"]:
            failed_files.append(dest.name)
            add_log(f"[{idx}/{total_files}] ❌ Не удалось скачать: {dest.name}")

    with lock:
        state["is_running"] = False
        state["is_paused"] = False
        if state["should_stop"]:
            state["status_text"] = "Загрузка остановлена"
        elif failed_files:
            state["status_text"] = f"Завершено с ошибками: не скачано {len(failed_files)} файл(ов). Нажмите «Старт» ещё раз, чтобы докачать"
        else:
            state["status_text"] = "Загрузка завершена!"
        state["current_file"] = ""
        state["speed_str"] = "0 MB/s"
        state["eta_str"] = "--:--"

    if failed_files:
        add_log(f"⚠️ Не скачано файлов: {len(failed_files)}: " + ", ".join(failed_files[:20]) + (" …" if len(failed_files) > 20 else ""))
    add_log("🏁 Работа загрузчика завершена.")

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
      --bg: #0d1117;
      --card: #161b22;
      --border: #30363d;
      --accent: #ff6b4a;
      --accent-hover: #ff8566;
      --text: #c9d1d9;
      --text-white: #f0f6fc;
      --muted: #8b949e;
      --green: #238636;
      --green-hover: #2ea043;
      --yellow: #d29922;
      --yellow-hover: #e3b341;
      --red: #da3633;
      --red-hover: #f85149;
      --blue: #58a6ff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 24px; min-height: 100vh; display: flex; flex-direction: column; align-items: center; }
    .container { width: 100%; max-width: 860px; display: flex; flex-direction: column; gap: 16px; }

    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; }
    .header h1 { font-size: 22px; font-weight: 700; color: var(--text-white); display: flex; align-items: center; gap: 8px; }
    .header .badge { font-size: 11px; background: rgba(255, 107, 74, 0.15); color: var(--accent); padding: 4px 8px; border-radius: 6px; font-weight: 600; }

    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; display: flex; flex-direction: column; gap: 14px; }
    .field-group { display: flex; flex-direction: column; gap: 6px; }
    label { font-size: 12px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
    textarea, input[type="text"], input[type="password"] {
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px 14px;
      color: var(--text-white);
      font-size: 13px;
      outline: none;
      transition: border-color 0.2s;
    }
    textarea:focus, input[type="text"]:focus, input[type="password"]:focus { border-color: var(--accent); }
    textarea { resize: vertical; min-height: 65px; font-family: monospace; }

    .toggle-section { display: flex; align-items: center; gap: 8px; font-size: 13px; cursor: pointer; color: var(--muted); }
    .toggle-section:hover { color: var(--text); }
    .hidden { display: none !important; }

    .controls-row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    button {
      padding: 11px 22px;
      font-size: 14px;
      font-weight: 700;
      border: none;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    button:disabled { opacity: 0.35; cursor: not-allowed; filter: grayscale(0.5); }
    
    .btn-start { background: var(--green); color: #fff; }
    .btn-start:hover:not(:disabled) { background: var(--green-hover); transform: translateY(-1px); }
    
    .btn-pause { background: var(--yellow); color: #0d1117; }
    .btn-pause:hover:not(:disabled) { background: var(--yellow-hover); transform: translateY(-1px); }
    
    .btn-stop { background: var(--red); color: #fff; }
    .btn-stop:hover:not(:disabled) { background: var(--red-hover); transform: translateY(-1px); }

    .status-badge {
      margin-left: auto;
      font-size: 13px;
      font-weight: 600;
      padding: 6px 12px;
      border-radius: 20px;
      background: rgba(139,148,158,0.15);
      color: var(--muted);
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .status-badge.running { background: rgba(35,134,54,0.2); color: #3fb950; }
    .status-badge.paused { background: rgba(210,153,34,0.2); color: #e3b341; }

    .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .stat-card { background: #0d1117; border: 1px solid var(--border); border-radius: 8px; padding: 12px; text-align: center; }
    .stat-val { font-size: 18px; font-weight: 700; color: var(--text-white); font-family: monospace; }
    .stat-lbl { font-size: 11px; color: var(--muted); margin-top: 2px; }

    .progress-box { display: flex; flex-direction: column; gap: 8px; }
    .progress-header { display: flex; justify-content: space-between; font-size: 13px; font-weight: 600; color: var(--text-white); }
    .progress-bar-bg { width: 100%; height: 14px; background: #0d1117; border: 1px solid var(--border); border-radius: 7px; overflow: hidden; }
    .progress-bar-fill { height: 100%; background: linear-gradient(90deg, #ff6b4a, #ffa756); width: 0%; transition: width 0.3s; }

    .terminal { background: #0a0d13; border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-family: "SFMono-Regular", Consolas, monospace; font-size: 12px; line-height: 1.6; color: var(--muted); max-height: 200px; overflow-y: auto; white-space: pre-wrap; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>🚀 YDisk Downloader</h1>
      <span class="badge">v1.1 Controls Edition</span>
    </div>

    <div class="card">
      <div class="field-group">
        <label>Ссылки на Яндекс.Диск (по одной на строке):</label>
        <textarea id="urlsInput" placeholder="https://disk.yandex.ru/d/..."></textarea>
      </div>

      <div class="field-group">
        <label>Куда сохранить на компьютере:</label>
        <input type="text" id="destInput" value="">
      </div>

      <div class="toggle-section" onclick="toggleToken()">
        <span>🔑 Опционально: Добавить токен Яндекс (для обхода лимитов) ▾</span>
      </div>
      <div id="tokenGroup" class="field-group hidden">
        <label>OAuth Токен Яндекса:</label>
        <input type="password" id="tokenInput" placeholder="y0__wgB..." autocomplete="off">
      </div>

      <div class="controls-row">
        <!-- Кнопка Старт / Продолжить -->
        <button id="startBtn" class="btn-start" onclick="startDownload()">
          <span>▶️ Старт</span>
        </button>

        <!-- Кнопка Пауза -->
        <button id="pauseBtn" class="btn-pause" onclick="togglePause()" disabled>
          <span id="pauseBtnText">⏸️ Пауза</span>
        </button>

        <!-- Кнопка Стоп -->
        <button id="stopBtn" class="btn-stop" onclick="stopDownload()" disabled>
          <span>⏹️ Стоп</span>
        </button>

        <div id="statusBadge" class="status-badge">
          <span>⚪ Готов</span>
        </div>
      </div>
    </div>

    <div class="card" id="statusCard">
      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-val" id="statFiles">0 / 0</div>
          <div class="stat-lbl">Файлов скачано</div>
        </div>
        <div class="stat-card">
          <div class="stat-val" id="statSpeed">0 MB/s</div>
          <div class="stat-lbl">Скорость</div>
        </div>
        <div class="stat-card">
          <div class="stat-val" id="statEta">--:--</div>
          <div class="stat-lbl">Осталось времени</div>
        </div>
        <div class="stat-card">
          <div class="stat-val" id="statVolume">0 / 0 GB</div>
          <div class="stat-lbl">Общий объём</div>
        </div>
      </div>

      <div class="progress-box">
        <div class="progress-header">
          <span id="currentFileName">Ожидание запуска...</span>
          <span id="currentFilePct">0%</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="fileProgressFill"></div>
        </div>
      </div>

      <div class="progress-box">
        <div class="progress-header">
          <span>Общий прогресс очереди</span>
          <span id="overallPct">0%</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="overallProgressFill" style="background: linear-gradient(90deg, #238636, #58a6ff);"></div>
        </div>
      </div>

      <!-- Очередь по папкам -->
      <div id="foldersSection" style="display: none; flex-direction: column; gap: 8px; margin-top: 6px;">
        <div style="font-size: 12px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;">📁 Очередь по ссылкам и папкам</div>
        <div id="foldersList" style="display: flex; flex-direction: column; gap: 8px;"></div>
      </div>

      <div class="terminal" id="termLogs"></div>
    </div>
  </div>

  <script>
    let isCurrentlyPaused = false;

    // Имена файлов приходят из чужих папок — экранируем перед вставкой в HTML
    function esc(s) {
      return String(s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
    }

    document.addEventListener("DOMContentLoaded", () => {
      fetch("/api/default_dest")
        .then(r => r.json())
        .then(d => {
          if (d.path) document.getElementById("destInput").value = d.path;
        });
      setInterval(pollStatus, 1000);
    });

    function postJSON(url, data) {
      return fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data || {})
      });
    }

    function toggleToken() {
      const g = document.getElementById("tokenGroup");
      g.classList.toggle("hidden");
    }

    function startDownload() {
      const urls = document.getElementById("urlsInput").value.split("\\n").map(s => s.trim()).filter(Boolean);
      const dest = document.getElementById("destInput").value.trim();
      const token = document.getElementById("tokenInput").value.trim();

      if (!urls.length) {
        alert("Пожалуйста, укажите хотя бы одну ссылку на Яндекс.Диск!");
        return;
      }
      if (!dest) {
        alert("Пожалуйста, укажите папку для сохранения!");
        return;
      }

      fetch("/api/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls, dest, token })
      }).then(r => r.json()).then(res => {
        if (res.status === "started") {
          updateControlsState(true, false);
        } else {
          alert("Ошибка запуска: " + res.message);
        }
      });
    }

    function togglePause() {
      if (isCurrentlyPaused) {
        postJSON("/api/resume")
          .then(r => r.json())
          .then(() => {
            isCurrentlyPaused = false;
            updateControlsState(true, false);
          });
      } else {
        postJSON("/api/pause")
          .then(r => r.json())
          .then(() => {
            isCurrentlyPaused = true;
            updateControlsState(true, true);
          });
      }
    }

    function stopDownload() {
      postJSON("/api/stop")
        .then(r => r.json())
        .then(() => {
          updateControlsState(false, false);
        });
    }

    function updateControlsState(running, paused) {
      const startBtn = document.getElementById("startBtn");
      const pauseBtn = document.getElementById("pauseBtn");
      const pauseText = document.getElementById("pauseBtnText");
      const stopBtn = document.getElementById("stopBtn");
      const badge = document.getElementById("statusBadge");

      startBtn.disabled = running;
      stopBtn.disabled = !running;
      pauseBtn.disabled = !running;

      if (!running) {
        badge.className = "status-badge";
        badge.innerHTML = "<span>⚪ Готов</span>";
        pauseText.innerText = "⏸️ Пауза";
      } else if (paused) {
        badge.className = "status-badge paused";
        badge.innerHTML = "<span>⏸️ На паузе</span>";
        pauseText.innerText = "▶️ Продолжить";
      } else {
        badge.className = "status-badge running";
        badge.innerHTML = "<span>🟢 Загрузка...</span>";
        pauseText.innerText = "⏸️ Пауза";
      }
    }

    function pollStatus() {
      fetch("/api/status")
        .then(r => r.json())
        .then(data => {
          isCurrentlyPaused = data.is_paused;
          updateControlsState(data.is_running, data.is_paused);

          document.getElementById("statFiles").innerText = `${data.completed_list.length} / ${data.total_files}`;
          document.getElementById("statSpeed").innerText = data.speed_str;
          document.getElementById("statEta").innerText = data.eta_str;
          document.getElementById("statVolume").innerText = `${(data.downloaded_bytes / (1024**3)).toFixed(1)} / ${(data.total_bytes / (1024**3)).toFixed(1)} GB`;

          document.getElementById("currentFileName").innerText = data.current_file || data.status_text;
          document.getElementById("currentFilePct").innerText = `${data.file_percent.toFixed(1)}%`;
          document.getElementById("fileProgressFill").style.width = `${data.file_percent}%`;

          document.getElementById("overallPct").innerText = `${data.overall_percent.toFixed(1)}%`;
          document.getElementById("overallProgressFill").style.width = `${data.overall_percent}%`;

          if (data.folders && data.folders.length > 0) {
            document.getElementById("foldersSection").style.display = "flex";
            const fl = document.getElementById("foldersList");
            fl.innerHTML = data.folders.map(f => {
              const pct = f.total_files ? ((f.done_files / f.total_files) * 100).toFixed(0) : 0;
              let badgeStyle = "background: rgba(139,148,158,0.15); color: var(--muted);";
              let badgeText = "⏳ В очереди";
              if (f.status === "done" || f.done_files >= f.total_files) {
                badgeStyle = "background: rgba(35,134,54,0.2); color: #3fb950; border: 1px solid rgba(63,185,80,0.3);";
                badgeText = "✅ 100% Завершено";
              } else if (f.status === "active" || f.done_files > 0) {
                badgeStyle = "background: rgba(255,107,74,0.2); color: var(--accent); border: 1px solid rgba(255,107,74,0.3);";
                badgeText = `⬇️ ${pct}% Скачивается`;
              }
              const fillStyle = f.done_files >= f.total_files ? "background: #3fb950;" : "background: linear-gradient(90deg, #ff6b4a, #ffa756);";
              return `
                <div style="background: #0d1117; border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; display: flex; flex-direction: column; gap: 6px;">
                  <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                      <div style="font-size: 13px; font-weight: 600; color: var(--text-white);">${esc(f.name)}</div>
                      <div style="font-size: 11px; color: var(--muted); margin-top: 2px;">${f.done_files} из ${f.total_files} файлов • ${f.done_gb} / ${f.total_gb} GB</div>
                    </div>
                    <span style="font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 6px; ${badgeStyle}">${badgeText}</span>
                  </div>
                  <div style="width: 100%; height: 4px; background: #161b22; border-radius: 2px; overflow: hidden;">
                    <div style="height: 100%; ${fillStyle} width: ${pct}%;"></div>
                  </div>
                </div>
              `;
            }).join("");
          }

          const term = document.getElementById("termLogs");
          term.textContent = data.logs.join("\\n");
          term.scrollTop = term.scrollHeight;
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
            self._send_json({"path": str(default_dir)})
        elif self.path == '/api/status':
            # Сериализуем под замком, а отправляем без него, чтобы не тормозить загрузку
            with lock:
                snapshot = json.loads(json.dumps(state, ensure_ascii=False))
            self._send_json(snapshot)
        else:
            self.send_error(404)

    def do_POST(self):
        # Чужой сайт может отправить простой POST без preflight, но не с application/json
        if not self._is_trusted() or not self.headers.get('Content-Type', '').startswith('application/json'):
            self.send_error(403)
            return

        if self.path == '/api/start':
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                payload = json.loads(self.rfile.read(content_length).decode('utf-8') or '{}')
            except (ValueError, UnicodeDecodeError):
                self._send_json({"status": "error", "message": "Некорректный запрос"}, 400)
                return

            with lock:
                if state["is_running"] and state["is_paused"]:
                    state["is_paused"] = False
                    state["status_text"] = "Возобновление загрузки..."
                    resumed = True
                elif state["is_running"]:
                    self._send_json({"status": "error", "message": "Загрузка уже идёт или ещё останавливается"}, 400)
                    return
                else:
                    resumed = False
            if resumed:
                add_log("▶️ Загрузка продолжена.")
                self._send_json({"status": "started"})
                return

            urls = [u for u in payload.get('urls', []) if isinstance(u, str) and u.strip()]
            dest_str = str(payload.get('dest', '')).strip()
            token = str(payload.get('token', '')).strip() or None
            if not urls or not dest_str:
                self._send_json({"status": "error", "message": "Нужны ссылки и папка для сохранения"}, 400)
                return

            try:
                dest_path = Path(dest_str).expanduser().resolve()
                dest_path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                self._send_json({"status": "error", "message": f"Не удалось создать папку: {e}"}, 400)
                return

            with lock:
                # Ставим флаг сразу, чтобы двойной клик не запустил два потока
                if state["is_running"]:
                    self._send_json({"status": "error", "message": "Загрузка уже идёт"}, 400)
                    return
                state["is_running"] = True
            t = threading.Thread(target=download_worker, args=(urls, dest_path, token), daemon=True)
            t.start()
            self._send_json({"status": "started"})

        elif self.path == '/api/pause':
            with lock:
                if not state["is_running"]:
                    self._send_json({"status": "error", "message": "Нечего ставить на паузу"}, 400)
                    return
                state["is_paused"] = True
                state["status_text"] = "Загрузка на паузе ⏸️"
                state["speed_str"] = "0 MB/s"
                state["eta_str"] = "Пауза"
            add_log("⏸️ Загрузка поставлена на паузу.")
            self._send_json({"status": "paused"})

        elif self.path == '/api/resume':
            with lock:
                state["is_paused"] = False
                state["status_text"] = "Загрузка продолжается ▶️"
            add_log("▶️ Загрузка продолжена.")
            self._send_json({"status": "resumed"})

        elif self.path == '/api/stop':
            with lock:
                state["should_stop"] = True
                state["is_paused"] = False
            add_log("⏹️ Команда остановки получена.")
            self._send_json({"status": "stopping"})
        else:
            self.send_error(404)

def run_server():
    server = ThreadingHTTPServer((HOST, PORT), RequestHandler)
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
        server.server_close()

if __name__ == '__main__':
    run_server()
