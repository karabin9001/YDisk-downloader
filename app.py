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

def get_public_resources(public_key, path='', token=None, offset=0, limit=100):
    params = {'public_key': public_key, 'limit': limit, 'offset': offset}
    if path:
        params['path'] = path
    headers = {}
    if token:
        headers['Authorization'] = f'OAuth {token.strip()}'
    r = requests.get('https://cloud-api.yandex.net/v1/disk/public/resources', params=params, headers=headers, timeout=25)
    r.raise_for_status()
    return r.json()

def scan_folder_recursive(public_key, path='', token=None):
    files = []
    offset = 0
    limit = 100
    while True:
        try:
            data = get_public_resources(public_key, path=path, token=token, offset=offset, limit=limit)
        except Exception as e:
            add_log(f"⚠️ Ошибка чтения структуры {path or 'корня'}: {e}")
            break
        embedded = data.get('_embedded', {})
        items = embedded.get('items', [])
        if not items:
            break
        for it in items:
            if it.get('type') == 'dir':
                files.extend(scan_folder_recursive(public_key, it.get('path'), token=token))
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

def get_download_link(public_key, path, token=None):
    params = {'public_key': public_key, 'path': path}
    headers = {}
    if token:
        headers['Authorization'] = f'OAuth {token.strip()}'
    r = requests.get('https://cloud-api.yandex.net/v1/disk/public/resources/download', params=params, headers=headers, timeout=25)
    if r.status_code == 200:
        return r.json().get('href')
    return None

def download_worker(urls: list, dest_dir: Path, token: str = None):
    with lock:
        state["is_running"] = True
        state["is_paused"] = False
        state["should_stop"] = False
        state["status_text"] = "Сканирование ссылок на Яндекс Диске..."
        state["completed_list"] = []
        state["logs"] = []

    add_log(f"🚀 Запуск очереди скачивания. Папка назначения: {dest_dir}")

    all_files_queue = []
    for raw_url in urls:
        raw_url = raw_url.strip()
        if not raw_url:
            continue
        add_log(f"🔍 Сканирование ссылки: {raw_url}")
        try:
            root_info = get_public_resources(raw_url, limit=1, token=token)
            folder_name = root_info.get('name', 'Яндекс_Файлы')
            target_folder = dest_dir / folder_name
            target_folder.mkdir(parents=True, exist_ok=True)

            files = scan_folder_recursive(raw_url, token=token)
            add_log(f"📁 Папка '{folder_name}': найдено {len(files)} файлов")
            for f in files:
                rel = f['path'].lstrip('/')
                all_files_queue.append({
                    "public_key": raw_url,
                    "remote_path": f['path'],
                    "dest_path": target_folder / rel,
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
        state["status_text"] = f"Скачивание файлов (всего {total_files} шт, {total_bytes / (1024**3):.2f} GB)"

    add_log(f"📦 Итого в очереди: {total_files} файлов ({total_bytes / (1024**3):.2f} GB)")

    downloaded_total_bytes = 0

    for idx, item in enumerate(all_files_queue, 1):
        if state["should_stop"]:
            add_log("⏹️ Загрузка остановлена пользователем.")
            break

        while state["is_paused"]:
            if state["should_stop"]:
                break
            time.sleep(0.3)

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
            with lock:
                state["completed_list"].append({"name": dest.name, "size": expected_size, "status": "Уже был"})
                state["overall_percent"] = (downloaded_total_bytes / total_bytes * 100) if total_bytes else 100
            continue

        # Скачивание файла
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_path = dest.with_suffix(dest.suffix + ".part")
        start_byte = 0
        if temp_path.exists():
            start_byte = temp_path.stat().st_size
            if expected_size and start_byte > expected_size:
                temp_path.unlink()
                start_byte = 0

        add_log(f"[{idx}/{total_files}] ⬇️ Скачивание: {dest.name} ({expected_size / (1024*1024):.1f} MB)")
        success = False

        for attempt in range(1, 5):
            if state["should_stop"]:
                break

            while state["is_paused"]:
                if state["should_stop"]:
                    break
                time.sleep(0.3)

            try:
                href = get_download_link(item['public_key'], item['remote_path'], token=token)
                if not href:
                    raise Exception("Не получен прямой href от Яндекс.Диска (возможно, лимит скачивания)")

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
                        mode = 'wb'
                        current_file_downloaded = 0
                        start_byte = 0

                    with open(temp_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                            if state["should_stop"]:
                                break

                            while state["is_paused"]:
                                if state["should_stop"]:
                                    break
                                time.sleep(0.3)

                            if not chunk:
                                continue
                            f.write(chunk)
                            c_len = len(chunk)
                            current_file_downloaded += c_len
                            bytes_in_session += c_len
                            downloaded_total_bytes += c_len

                            now = time.time()
                            elapsed = now - t_start
                            spd = bytes_in_session / elapsed if elapsed > 0 else 0
                            spd_mb = spd / (1024 * 1024)
                            remain_bytes = max(0, expected_size - current_file_downloaded)
                            eta = remain_bytes / spd if spd > 0 else 0
                            pct = (current_file_downloaded / expected_size * 100) if expected_size > 0 else 0
                            ov_pct = (downloaded_total_bytes / total_bytes * 100) if total_bytes > 0 else 0

                            with lock:
                                state["file_percent"] = pct
                                state["file_downloaded_mb"] = current_file_downloaded / (1024 * 1024)
                                state["speed_mbs"] = spd_mb
                                state["speed_str"] = f"{spd_mb:.1f} MB/s"
                                state["eta_str"] = format_eta(eta)
                                state["overall_percent"] = ov_pct
                                state["downloaded_bytes"] = downloaded_total_bytes

                if state["should_stop"]:
                    break

                if temp_path.exists():
                    if dest.exists():
                        dest.unlink()
                    temp_path.rename(dest)
                add_log(f"[{idx}/{total_files}] ✅ Готово: {dest.name}")
                with lock:
                    state["completed_list"].append({"name": dest.name, "size": expected_size, "status": "Скачан"})
                success = True
                break

            except Exception as e:
                add_log(f"   ⚠️ Сбой (попытка {attempt}/4): {e}")
                if temp_path.exists():
                    start_byte = temp_path.stat().st_size
                time.sleep(3)

        if not success and not state["should_stop"]:
            add_log(f"[{idx}/{total_files}] ❌ Не удалось скачать: {dest.name}")

    with lock:
        state["is_running"] = False
        state["is_paused"] = False
        state["status_text"] = "Загрузка завершена!" if not state["should_stop"] else "Загрузка остановлена"
        state["speed_str"] = "0 MB/s"
        state["eta_str"] = "--:--"

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
    textarea, input[type="text"] {
      background: #0d1117;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px 14px;
      color: var(--text-white);
      font-size: 13px;
      outline: none;
      transition: border-color 0.2s;
    }
    textarea:focus, input[type="text"]:focus { border-color: var(--accent); }
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
        <input type="text" id="tokenInput" placeholder="y0__wgB...">
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

      <div class="terminal" id="termLogs"></div>
    </div>
  </div>

  <script>
    let isCurrentlyPaused = false;

    document.addEventListener("DOMContentLoaded", () => {
      fetch("/api/default_dest")
        .then(r => r.json())
        .then(d => {
          if (d.path) document.getElementById("destInput").value = d.path;
        });
      setInterval(pollStatus, 1000);
    });

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
        fetch("/api/resume", { method: "POST" })
          .then(r => r.json())
          .then(() => {
            isCurrentlyPaused = false;
            updateControlsState(true, false);
          });
      } else {
        fetch("/api/pause", { method: "POST" })
          .then(r => r.json())
          .then(() => {
            isCurrentlyPaused = true;
            updateControlsState(true, true);
          });
      }
    }

    function stopDownload() {
      fetch("/api/stop", { method: "POST" })
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

          const term = document.getElementById("termLogs");
          term.innerHTML = data.logs.join("\\n");
          term.scrollTop = term.scrollHeight;
        });
    }
  </script>
</body>
</html>
"""

class RequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
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
            with lock:
                self._send_json(dict(state))
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == '/api/start':
            if state["is_running"] and not state["is_paused"]:
                self._send_json({"status": "error", "message": "Загрузка уже идёт"}, 400)
                return
            if state["is_paused"]:
                with lock:
                    state["is_paused"] = False
                    state["status_text"] = "Возобновление загрузки..."
                add_log("▶️ Загрузка продолжена.")
                self._send_json({"status": "started"})
                return

            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            payload = json.loads(body.decode('utf-8'))

            urls = payload.get('urls', [])
            dest_str = payload.get('dest', '')
            token = payload.get('token', '').strip() or None

            dest_path = Path(dest_str).expanduser().resolve()
            dest_path.mkdir(parents=True, exist_ok=True)

            t = threading.Thread(target=download_worker, args=(urls, dest_path, token), daemon=True)
            t.start()
            self._send_json({"status": "started"})

        elif self.path == '/api/pause':
            with lock:
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
