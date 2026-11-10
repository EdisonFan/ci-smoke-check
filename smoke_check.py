#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scheduled endpoint smoke checks.

Collects candidate endpoints from configured feeds, verifies each one against
a configurable probe target (always through the candidate itself - never a
direct request), and records successful checks into the results store.

Every environment-specific value is supplied via env vars / repository
secrets, so this repository contains no deployment details.
"""
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def env(name, default=''):
    return os.environ.get(name, default).strip()


SB_URL = env('SB_URL').rstrip('/')
SB_KEY = env('SB_KEY')
PROBE_URL = env('PROBE_URL')
OK_CODE = env('OK_CODE')
try:
    SOURCES = json.loads(env('SOURCES', '[]'))
except ValueError:
    SOURCES = []
try:
    PROBE_HEADERS = json.loads(env('PROBE_HEADERS', '{}'))
except ValueError:
    PROBE_HEADERS = {}
try:
    _b = json.loads(env('PROBE_BODY', 'null'))
    # 多个请求体用于错峰，避免密集探活集中在同一个查询上
    PROBE_BODIES = _b if isinstance(_b, list) else ([_b] if _b else [])
except ValueError:
    PROBE_BODIES = []

TIMEOUT = float(env('TIMEOUT', '6'))
WORKERS = int(env('WORKERS', '32'))
MAX_CANDIDATES = int(env('MAX_CANDIDATES', '500'))
MAX_SECONDS = float(env('MAX_SECONDS', '240'))
MAX_RECORDS = int(env('MAX_RECORDS', '80'))
BATCH = 40

ADDR = re.compile(r'^\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}$')
UA = 'ci-smoke-check/1.0'


def log(msg):
    print('[%s] %s' % (time.strftime('%H:%M:%S'), msg), flush=True)


def parse_feed(body):
    """Accept plain ip:port lists or JSON feeds with ip/port fields."""
    out = []
    stripped = body.lstrip()
    if stripped.startswith(('{', '[')):
        try:
            data = json.loads(body)
            rows = data.get('data', []) if isinstance(data, dict) else data
            for row in rows:
                if isinstance(row, dict) and row.get('ip') and row.get('port'):
                    out.append('http://%s:%s' % (row['ip'], row['port']))
        except (ValueError, TypeError):
            pass
        return out
    for line in body.splitlines():
        s = line.strip()
        if s.startswith('http://') and s.count('://') == 1:
            out.append(s)
        elif ADDR.match(s):
            out.append('http://' + s)
    return out


def fetch_feeds():
    found, seen = [], set()
    for url in SOURCES:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read().decode('utf-8', 'replace')
            n = 0
            for ep in parse_feed(body):
                if ep not in seen:
                    seen.add(ep)
                    found.append(ep)
                    n += 1
            log('feed ok: +%d entries' % n)
        except Exception as e:
            log('feed failed: %s' % type(e).__name__)
    random.shuffle(found)
    return found


def check(ep):
    """Probe through ep. A blank endpoint is a hard error (no direct access)."""
    if not ep:
        raise RuntimeError('blank endpoint: direct access is not allowed')
    t0 = time.time()
    try:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({'http': ep, 'https': ep}))
        body = random.choice(PROBE_BODIES) if PROBE_BODIES else None
        data = json.dumps(body).encode() if body is not None else None
        headers = {'User-Agent': UA, 'Accept': '*/*'}
        headers.update(PROBE_HEADERS)
        if data:
            headers.setdefault('Content-Type', 'application/json')
        req = urllib.request.Request(
            PROBE_URL, data=data, headers=headers,
            method='POST' if data else 'GET')
        with opener.open(req, timeout=TIMEOUT) as r:
            text = r.read(262144).decode('utf-8', 'replace')
        if OK_CODE:
            try:
                ok = str(json.loads(text).get('code')) == OK_CODE
            except (ValueError, AttributeError):
                # 大响应可能被截断：退回文本匹配，避免把可用结果误判为失败
                ok = bool(re.search(r'"code"\s*:\s*"?%s"?'
                                    % re.escape(OK_CODE), text))
        else:
            ok = True
        return ep, ok, int((time.time() - t0) * 1000)
    except RuntimeError:
        raise
    except Exception:
        return ep, False, None


def record(rows):
    if not rows:
        return 0
    body = json.dumps({'p_rows': rows}).encode()
    req = urllib.request.Request(
        SB_URL + '/rest/v1/rpc/record_checks', data=body,
        headers={'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY,
                 'Content-Type': 'application/json', 'Prefer': 'return=minimal'},
        method='POST')
    with urllib.request.urlopen(req, timeout=30) as r:
        r.read()
    return len(rows)


def main():
    missing = [k for k, v in
               [('SB_URL', SB_URL), ('SB_KEY', SB_KEY), ('PROBE_URL', PROBE_URL)]
               if not v]
    if missing or not SOURCES:
        log('configuration not ready, skipping this round')
        return 0
    t0 = time.time()
    candidates = fetch_feeds()
    if not candidates:
        log('no candidates collected, nothing to do')
        return 0
    candidates = candidates[:MAX_CANDIDATES]
    log('candidates=%d timeout=%.1fs workers=%d budget=%.0fs'
        % (len(candidates), TIMEOUT, WORKERS, MAX_SECONDS))

    good, checked = [], 0
    i = 0
    while (i < len(candidates) and len(good) < MAX_RECORDS
           and time.time() - t0 < MAX_SECONDS):
        batch = candidates[i:i + BATCH]
        i += BATCH
        checked += len(batch)
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for ep, ok, lat in ex.map(check, batch):
                if ok:
                    good.append({'endpoint': ep, 'latency_ms': lat,
                                 'source': 'scheduled'})
    log('checked=%d ok=%d elapsed=%.0fs' % (checked, len(good), time.time() - t0))
    try:
        n = record(good[:MAX_RECORDS])
        log('recorded=%d' % n)
    except urllib.error.HTTPError as e:
        log('store error HTTP %d: %s'
            % (e.code, e.read().decode('utf-8', 'replace')[:200]))
        return 1
    except Exception as e:
        log('store error: %s' % type(e).__name__)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
