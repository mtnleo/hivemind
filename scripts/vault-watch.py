#!/usr/bin/env python3
"""Hivemind vault watcher.

Runs on the Pi as a systemd service. Subscribes via `inotifywait` to file
events under the Obsidian vault, translates host paths to n8n container
paths, and POSTs `{event, path}` to the n8n vault-sync webhook for each
relevant change.

Events watched: close_write (after editor save), moved_to (file landing
at a new path after a move). Move out of the vault is intentionally
ignored — only the destination event drives a sync.

Filters to `.md` files. Non-md (.DS_Store, attachments) skipped.

Install: copy to /home/mtnleo/n8n-compose/vault-watch.py, install
vault-watch.service systemd unit, enable + start.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

HOST_BASE = '/home/mtnleo/n8n-compose/local-files/obsidian-vault'
CONTAINER_BASE = '/files/obsidian-vault'
WEBHOOK = 'https://n8n.mtnleo-n8n.org/webhook/vault-sync'
WATCH_EVENTS = 'close_write,moved_to'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s vault-watch %(levelname)s %(message)s',
    stream=sys.stdout,
)
log = logging.getLogger()


def translate(host_path: str) -> str:
    if host_path.startswith(HOST_BASE):
        return CONTAINER_BASE + host_path[len(HOST_BASE):]
    return host_path


def post(event: str, container_path: str) -> None:
    body = json.dumps({'event': event, 'path': container_path}).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK,
        data=body,
        method='POST',
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'curl/8.7.1',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        log.info('posted %s %s', event, container_path)
    except urllib.error.HTTPError as e:
        log.warning('webhook %s: %s %s', e.code, container_path, e.read()[:200])
    except Exception as e:  # network blip, dns flap, etc.
        log.warning('webhook err: %s for %s', e, container_path)


def main() -> int:
    if not os.path.isdir(HOST_BASE):
        log.error('vault base does not exist: %s', HOST_BASE)
        return 2

    cmd = [
        '/usr/bin/inotifywait',
        '-m', '-r', '-q',
        '--format', '%e|%w%f',
        '-e', WATCH_EVENTS,
        HOST_BASE,
    ]
    log.info('starting watcher on %s', HOST_BASE)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, text=True)

    def _term(signum, frame):
        log.info('signal %d — shutting down', signum)
        try:
            proc.terminate()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if '|' not in line:
            continue
        events, host_path = line.split('|', 1)
        # `events` can be a comma-joined list like CLOSE_WRITE,CLOSE
        primary = events.split(',')[0].lower()
        if not host_path.lower().endswith('.md'):
            continue
        # Skip Obsidian's own metadata dir.
        if '/.obsidian/' in host_path:
            continue
        cpath = translate(host_path)
        post(primary, cpath)

    rc = proc.wait()
    log.info('inotifywait exited rc=%s', rc)
    return rc if rc else 1


if __name__ == '__main__':
    while True:
        try:
            sys.exit(main())
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001 — outer crash guard
            log.exception('crashed: %s — restarting in 5s', e)
            time.sleep(5)
