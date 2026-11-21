#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ydisk — управление YDisk Downloader из командной строки (для агента и скриптов).

Говорит с уже запущенной программой по её HTTP-API. Токен Я.Диска сюда не передаётся:
загрузчик берёт его сам (YANDEX_DISK_TOKEN или YDISK_ENV_FILE), поэтому он не попадает
ни в историю команд, ни в журнал агента.

Где работает загрузчик (--at, или переменная YDISK_URL):
    mac     http://127.0.0.1:7860      — этот компьютер (по умолчанию)
    server  http://192.168.1.77:7860   — домашний сервер
    или полный адрес http://хост:порт

Примеры:
    ydisk status
    ydisk upload "/Volumes/MSI M461 2T/Заказ/Рендер" --to "Рендеры заказов/3909_Подкаст/v1" --order 3909 --wait
    ydisk download https://disk.yandex.ru/d/abc --dest "E:\\Заказ\\Исходники" --at server --order 3909
    ydisk pause 1a2b3c4d          (также resume, stop, top, restart, remove)
    ydisk wait 1a2b3c4d --timeout 7200

Коды выхода: 0 — готово, 1 — завершилось с ошибками / остановлено, 2 — неверная команда или
нет связи с программой, 3 — не дождались за --timeout.
С --json команды печатают один JSON-объект — его удобно разбирать агенту.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

INSTANCES = {'mac': 'http://127.0.0.1:7860', 'server': 'http://192.168.1.77:7860'}
FINAL = {'done', 'problems', 'stopped', 'crashed', 'blocked'}
RUNNING_PHASES = {'waiting', 'scanning', 'checking', 'uploading', 'publishing', 'downloading', 'limit', 'deferred',
                  'paused', 'stopping'}

# Прокси из окружения (например, туннель на сервере) к загрузчику не нужен: он в локальной сети
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class CliError(Exception):
    pass


def base_url(at):
    at = at or os.environ.get('YDISK_URL') or 'mac'
    return INSTANCES.get(at, at).rstrip('/')


def call(base, path, data=None, timeout=15):
    """GET без data, POST с JSON. Возвращает разобранный ответ."""
    body = None if data is None else json.dumps(data, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(base + path, data=body, method='GET' if data is None else 'POST',
                                 headers={'Content-Type': 'application/json'} if body is not None else {})
    try:
        with _opener.open(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode('utf-8'))
        except Exception:
            raise CliError(f"Загрузчик ответил HTTP {e.code} на {path}") from None
    except (urllib.error.URLError, OSError) as e:
        raise CliError(f"Нет связи с YDisk Downloader по адресу {base}: {getattr(e, 'reason', e)}. "
                       "Запущена ли программа?") from None


def all_items(status):
    return [dict(q, kind='download') for q in status.get('queues', [])] + \
           [dict(u, kind='upload') for u in status.get('uploads', [])]


def find_item(base, item_id):
    status = call(base, '/api/status')
    for it in all_items(status):
        if it['id'] == item_id:
            return it
    raise CliError(f"Нет очереди с id {item_id}")


def brief(it):
    """Короткая сводка очереди: то, что нужно агенту, без журналов и списков файлов."""
    out = {
        'id': it['id'], 'kind': it['kind'], 'order': it.get('order') or '', 'display': it.get('display'),
        'running': bool(it.get('is_running')), 'percent': it.get('overall_percent', 0),
        'speed': it.get('speed_str') or '', 'eta': it.get('eta_str') if it.get('eta_str') not in (None, '--:--') else '',
        'status_text': it.get('status_text') or '', 'notice': it.get('notice') or '',
        'total_files': it.get('total_files', 0),
    }
    if it['kind'] == 'upload':
        out.update(sources=it.get('sources'), remote=it.get('remote'), public_url=it.get('public_url') or '',
                   done_files=it.get('done_files', 0) + it.get('skipped_files', 0),
                   skipped_files=it.get('skipped_files', 0), failed=it.get('failed_count', 0),
                   failed_files=[f['name'] for f in it.get('failed', [])][:20], threads=it.get('threads'))
    else:
        out.update(urls=it.get('urls'), dest=it.get('dest'), done_files=len(it.get('completed_list') or []),
                   deferred=it.get('deferred_files', 0))
    return out


def human(b):
    kind = '⬆' if b['kind'] == 'upload' else '⬇'
    where = f"{', '.join(b['sources'])} → {b['remote']}" if b['kind'] == 'upload' else f"{b['urls'][0]} → {b['dest']}"
    parts = [f"{kind} {b['id']}", b['display'] or '', f"{b['percent']}%",
             f"{b['done_files']}/{b['total_files']} файлов"]
    if b['speed']:
        parts.append(b['speed'])
    if b['eta']:
        parts.append(f"осталось {b['eta']}")
    if b['order']:
        parts.append(f"заказ {b['order']}")
    line = ' · '.join(p for p in parts if p) + f"\n   {where}"
    if b.get('public_url'):
        line += f"\n   🔗 {b['public_url']}"
    if b.get('failed'):
        line += f"\n   ❌ не залито: {b['failed']}"
    if b['display'] in ('problems', 'crashed', 'blocked', 'limit', 'deferred') and b['status_text']:
        line += f"\n   {b['status_text']}"
    if b['notice'] and b['display'] not in FINAL:
        line += f"\n   ℹ️ {b['notice']}"
    return line


def emit(args, obj, text):
    print(json.dumps(obj, ensure_ascii=False) if args.json else text)


def wait_for(base, item_id, timeout, quiet=False, every=30):
    """Ждёт конца очереди. Прогресс — в stderr раз в every секунд, чтобы не засорять ответ."""
    deadline = time.time() + timeout if timeout else None
    last = 0
    while True:
        b = brief(find_item(base, item_id))
        if not b['running'] and b['display'] in FINAL:
            return b, (0 if b['display'] == 'done' else 1)
        if deadline and time.time() > deadline:
            return b, 3
        if not quiet and time.time() - last >= every:
            last = time.time()
            print(f"… {b['display']} {b['percent']}% {b['done_files']}/{b['total_files']}"
                  + (f" {b['speed']}" if b['speed'] else '') + (f" осталось {b['eta']}" if b['eta'] else ''),
                  file=sys.stderr, flush=True)
        time.sleep(2)


def cmd_status(args, base):
    status = call(base, '/api/status')
    items = [brief(i) for i in all_items(status)]
    if args.order:
        items = [i for i in items if i['order'] == args.order]
    if args.active:
        items = [i for i in items if i['running']]
    if args.json:
        emit(args, {'url': base, 'items': items}, '')
    else:
        print('\n'.join(human(i) for i in items) if items else f"Очередей нет ({base})")
    return 0


def start_result(args, base, res, what):
    if res.get('status') != 'started':
        raise CliError(f"{what}: {res.get('message') or res}")
    item_id = res['id']
    if not args.wait:
        b = brief(find_item(base, item_id))
        emit(args, b, human(b))
        return 0
    b, code = wait_for(base, item_id, args.timeout, quiet=args.json)
    emit(args, b, human(b))
    return code


def cmd_upload(args, base):
    payload = {'sources': args.sources, 'remote': args.to, 'threads': args.threads, 'order': args.order or ''}
    return start_result(args, base, call(base, '/api/upload/start', payload), 'Заливка не началась')


def cmd_download(args, base):
    payload = {'urls': args.urls, 'dest': args.dest, 'flat': args.flat, 'order': args.order or '',
               'proxy': args.proxy or ''}
    return start_result(args, base, call(base, '/api/start', payload), 'Скачивание не началось')


def cmd_action(args, base):
    it = find_item(base, args.id)
    action = args.command
    if it['kind'] == 'upload':
        path = f'/api/upload/{action}'
    else:
        if action == 'top':
            raise CliError('«Наверх» есть только у заливок')
        path = f'/api/{action}'
    res = call(base, path, {'id': args.id})
    if res.get('status') != 'ok':
        raise CliError(res.get('message') or str(res))
    b = brief(find_item(base, args.id)) if action != 'remove' else {'id': args.id, 'removed': True}
    emit(args, b, human(b) if action != 'remove' else f"Убрано из списка: {args.id}")
    return 0


def cmd_wait(args, base):
    b, code = wait_for(base, args.id, args.timeout, quiet=args.json)
    emit(args, b, human(b))
    return code


def parser():
    p = argparse.ArgumentParser(prog='ydisk', description=__doc__.split('\n\n')[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument('--at', help='mac (по умолчанию), server или http://хост:порт')
    p.add_argument('--json', action='store_true', help='ответ одним JSON-объектом')
    sub = p.add_subparsers(dest='command', required=True)

    s = sub.add_parser('status', help='все очереди')
    s.add_argument('--order', help='только очереди этого заказа')
    s.add_argument('--active', action='store_true', help='только идущие')

    u = sub.add_parser('upload', help='залить папки/файлы на Я.Диск')
    u.add_argument('sources', nargs='+', help='локальные папки и файлы (на той машине, где загрузчик)')
    u.add_argument('--to', required=True, help='папка на Диске, например «Рендеры заказов/3909_Подкаст/v1»')
    u.add_argument('--threads', type=int, default=16)

    d = sub.add_parser('download', help='скачать по ссылкам Я.Диска / Google Drive / ORV Cloud')
    d.add_argument('urls', nargs='+')
    d.add_argument('--dest', required=True, help='папка назначения (на той машине, где загрузчик)')
    d.add_argument('--flat', action='store_true', help='без подпапки с именем ссылки')
    d.add_argument('--proxy', help='прокси только для Google Drive')

    for sp in (u, d):
        sp.add_argument('--order', help='номер/код заказа: по нему Пульт покажет очередь в карточке')
        sp.add_argument('--wait', action='store_true', help='дождаться конца и вывести итог')
        sp.add_argument('--timeout', type=float, default=0, help='сколько ждать с --wait, сек (0 — без ограничения)')

    for name, text in (('pause', 'пауза'), ('resume', 'продолжить после паузы'), ('stop', 'стоп'),
                       ('top', 'заливку наверх, остальные на паузу'), ('restart', 'запустить снова / докачать'),
                       ('remove', 'убрать из списка')):
        a = sub.add_parser(name, help=text)
        a.add_argument('id')

    w = sub.add_parser('wait', help='дождаться конца очереди')
    w.add_argument('id')
    w.add_argument('--timeout', type=float, default=0)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    base = base_url(args.at)
    handlers = {'status': cmd_status, 'upload': cmd_upload, 'download': cmd_download, 'wait': cmd_wait}
    try:
        return handlers.get(args.command, cmd_action)(args, base)
    except CliError as e:
        if args.json:
            print(json.dumps({'error': str(e)}, ensure_ascii=False))
        else:
            print(f"ydisk: {e}", file=sys.stderr)
        return 2


if __name__ == '__main__':
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass
    sys.exit(main())
