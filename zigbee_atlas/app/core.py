"""Passive Zigbee2MQTT map engine. No radio commands or credentials."""
import argparse
import bisect
from collections import defaultdict
from contextlib import contextmanager
import datetime as dt
import fcntl
import hashlib
import heapq
import html
import json
import math
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
from zoneinfo import ZoneInfo
from log_reader import metadata, read_paths
HERE = Path(__file__).resolve().parent
ZONE = ZoneInfo('Europe/Prague')
SCHEMA = 1

def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, separators=(',', ':')))
    os.replace(tmp, path)

def connect(root):
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / 'history.sqlite')
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('\n        CREATE TABLE IF NOT EXISTS events(key TEXT PRIMARY KEY,t INTEGER,k TEXT,payload TEXT);\n        CREATE INDEX IF NOT EXISTS events_time ON events(t);\n        CREATE TABLE IF NOT EXISTS devices(id TEXT PRIMARY KEY,payload TEXT);\n        CREATE TABLE IF NOT EXISTS coverage(key TEXT PRIMARY KEY,start INTEGER,end INTEGER,payload TEXT);\n        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);\n        CREATE TABLE IF NOT EXISTS identities(entity TEXT,k TEXT,PRIMARY KEY(entity,k));\n        CREATE TABLE IF NOT EXISTS addresses(nwk INTEGER,t INTEGER,id TEXT,PRIMARY KEY(nwk,t,id));\n    ')
    if 'entity' not in {r[1] for r in db.execute('PRAGMA table_info(events)')}:
        db.execute('ALTER TABLE events ADD COLUMN entity TEXT')
        for key, payload in db.execute('SELECT key,payload FROM events WHERE entity IS NULL').fetchall():
            event = json.loads(payload)
            db.execute('UPDATE events SET entity=? WHERE key=?', (entity_key(event), key))
        db.execute('INSERT OR IGNORE INTO identities SELECT entity,k FROM events GROUP BY entity,k')
        db.commit()
    db.execute('CREATE INDEX IF NOT EXISTS events_entity_time ON events(entity,k,t,key)')
    if not db.execute("SELECT value FROM settings WHERE key='addresses_initialized'").fetchone():
        for row in db.execute("SELECT payload FROM events WHERE k IN ('route','join') ORDER BY t").fetchall():
            remember_address(db, json.loads(row[0]))
        db.execute("INSERT OR REPLACE INTO settings VALUES('addresses_initialized','1')")
        db.commit()
    return db

def entity_key(event):
    for key in ('id', 'name', 'nwk'):
        if key in event:
            return key + ':' + str(event[key])
    return 'system'

def remember_address(db, event):
    old = db.execute('SELECT id FROM addresses WHERE nwk=? AND t<=? ORDER BY t DESC LIMIT 1', (event['nwk'], event['t'])).fetchone()
    if not old or old[0] != event['id']:
        db.execute('INSERT OR IGNORE INTO addresses VALUES(?,?,?)', (event['nwk'], event['t'], event['id']))

def ingest(db, data):
    before = db.total_changes
    dirty = []
    with db:
        for device in data.get('devices', []):
            encoded = json.dumps(device)
            db.execute('INSERT INTO devices VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload WHERE devices.payload<>excluded.payload', (device['id'], encoded))
        for event in data.get('events', []):
            encoded = json.dumps(event, sort_keys=True)
            key = hashlib.sha256(encoded.encode()).hexdigest()
            if event['k'] == 'seen':
                key = f"seen:{event['t'] // 60}:{event.get('name', event.get('nwk'))}:{event.get('point', 'legacy')}"
            comparison = '<' if event.get('point') == 'first' else '>'
            entity = entity_key(event)
            cursor = db.execute('INSERT INTO events(key,t,k,payload,entity) VALUES(?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET t=excluded.t,payload=excluded.payload WHERE excluded.t' + comparison + 'events.t', (key, event['t'], event['k'], encoded, entity))
            if cursor.rowcount:
                dirty.append(event['t'])
            db.execute('INSERT OR IGNORE INTO identities VALUES(?,?)', (entity, event['k']))
            if event['k'] in ('route', 'join'):
                remember_address(db, event)
        for span in data.get('coverage', []):
            encoded = json.dumps(span, sort_keys=True)
            cursor = db.execute('INSERT OR IGNORE INTO coverage VALUES(?,?,?,?)', (hashlib.sha256(encoded.encode()).hexdigest(), span['start'], span['end'], encoded))
            if cursor.rowcount:
                dirty.append(span['start'])
        if dirty:
            old = db.execute("SELECT value FROM settings WHERE key='dirty_since'").fetchone()
            db.execute("INSERT OR REPLACE INTO settings VALUES('dirty_since',?)", (str(min(dirty + [int(old[0])] if old else dirty)),))
    return db.total_changes - before

class History:

    def __init__(self, db, since=None):
        self.devices = {row[0]: json.loads(row[1]) for row in db.execute('SELECT * FROM devices')}
        self.byname = {d['name']: key for key, d in self.devices.items()}
        if since is None:
            rows = list(db.execute('SELECT payload FROM events ORDER BY t,k,key'))
            spans = db.execute('SELECT payload FROM coverage ORDER BY start')
        else:
            rows = list(db.execute('SELECT payload FROM events WHERE t>=? ORDER BY t,k,key', (since,)))
            for entity, kind in db.execute('SELECT entity,k FROM identities').fetchall():
                old = db.execute('SELECT payload FROM events WHERE entity=? AND k=? AND t<? ORDER BY t DESC,key DESC LIMIT 1', (entity, kind, since)).fetchone()
                if old:
                    rows.append(old)
            spans = db.execute('SELECT payload FROM coverage WHERE end>=? ORDER BY start', (since,))
        self.events = sorted((json.loads(row[0]) for row in rows), key=lambda e: (e['t'], e['k'], json.dumps(e, sort_keys=True)))
        self.spans = [json.loads(row[0]) for row in spans]
        self.addresses = defaultdict(list)
        for nwk, when, ieee in db.execute('SELECT nwk,t,id FROM addresses ORDER BY nwk,t,id'):
            self.addresses[nwk].append((when, ieee))
        for observations in self.addresses.values():
            observations.sort()
        self.coordinator = next((key for key, d in self.devices.items() if d['role'] == 'Coordinator'), 'coordinator')
        self.devices.setdefault(self.coordinator, {'id': self.coordinator, 'name': 'Koordinátor', 'role': 'Coordinator', 'nwk': 0})
        self.fallback = {d['nwk']: key for key, d in self.devices.items() if d.get('nwk') is not None}
        self.routes = defaultdict(list)
        self.joins = defaultdict(list)
        self.seen = defaultdict(list)
        self.availability = defaultdict(list)
        self.notifications = []
        self.coverage_cache = {}
        previous_route = {}
        previous_parent = {}
        for raw in self.events:
            event = dict(raw)
            key = event.get('id') or self.byname.get(event.get('name'))
            if not key and event.get('nwk') is not None:
                key = self.resolve(event['nwk'], event['t'])
            if event['k'] in ('system', 'note'):
                self.notifications.append({'t': event['t'], 'type': 'manual' if event['k'] == 'note' else 'system', 'text': event['message'], **({'id': key} if key else {})})
                continue
            if not key:
                continue
            event['id'] = key
            self.ensure(key)
            if event['k'] in ('route', 'seen', 'join'):
                self.seen[key].append(event['t'])
            if event['k'] == 'route':
                chain = [key, *[self.resolve(n, event['t']) for n in event['relays']], self.coordinator]
                chain = [v for i, v in enumerate(chain) if i == 0 or v != chain[i - 1]]
                if len(chain) != len(set(chain)):
                    self.notifications.append({'t': event['t'], 'type': 'loop', 'id': key, 'text': 'Opakovaný uzel v zaznamenané cestě'})
                    continue
                event['chain'] = chain
                self.routes[key].append(event)
                old = previous_route.get(key)
                if old and old != chain:
                    is_leaf = self.devices[key]['role'] == 'EndDevice'
                    self.notifications.append({'t': event['t'], 'type': 'route', 'id': key, 'text': 'Jiná pozorovaná cesta', 'old': old, 'new': chain, 'parent_changed': bool(is_leaf and old[1] != chain[1])})
                previous_route[key] = chain
                for relay in chain[1:]:
                    self.ensure(relay)
                    self.seen[relay].append(event['t'])
            elif event['k'] == 'join':
                event['parent'] = self.resolve(event['parent_nwk'], event['t'])
                self.ensure(event['parent'])
                self.joins[key].append(event)
                old = previous_parent.get(key)
                changed = old is not None and old != event['parent'] and (self.devices[key]['role'] == 'EndDevice')
                self.notifications.append({'t': event['t'], 'type': 'parent' if changed else 'rejoin', 'id': key, 'text': 'Změna rodiče při připojení' if changed else 'Připojení / rejoin', 'parent': event['parent'], 'status': event['status']})
                previous_parent[key] = event['parent']
            elif event['k'] == 'leave':
                self.notifications.append({'t': event['t'], 'type': 'leave', 'id': key, 'text': 'Oznámené opuštění sítě'})
            elif event['k'] == 'availability':
                self.availability[key].append(event)
                self.notifications.append({'t': event['t'], 'type': event['state'], 'id': key, 'text': 'Z2M: ' + event['state']})
        for key in self.seen:
            self.seen[key] = sorted(set(self.seen[key]))
        self.times = {key: [e['t'] for e in values] for key, values in self.routes.items()}
        self.join_times = {key: [e['t'] for e in values] for key, values in self.joins.items()}
        self.avail_times = {key: [e['t'] for e in values] for key, values in self.availability.items()}
        self.add_silence_events()
        self.notifications.sort(key=lambda e: e['t'])
        self.first = min((e['t'] for e in self.events), default=int(time.time()))
        self.end = max([e['t'] for e in self.events] + [s['end'] for s in self.spans] + [self.first])

    def ensure(self, key):
        if key not in self.devices:
            self.devices[key] = {'id': key, 'name': 'NWK ' + key[4:] if key.startswith('nwk:') else key, 'role': 'Unknown', 'nwk': int(key[4:]) if key.startswith('nwk:') else None}

    def resolve(self, nwk, when):
        if nwk == 0:
            return self.coordinator
        observations = self.addresses.get(nwk, [])
        index = bisect.bisect_right(observations, (when, 'z')) - 1
        if index >= 0:
            return observations[index][1]
        unique = {item[1] for item in observations}
        if len(unique) == 1:
            return next(iter(unique))
        if not observations:
            return self.fallback.get(nwk, f'nwk:{nwk}')
        return f'nwk:{nwk}'

    def intervals(self, key=None):
        cache_key = key or '*'
        if cache_key in self.coverage_cache:
            return self.coverage_cache[cache_key]
        name = self.devices.get(key, {}).get('name')
        spans = [(s['start'], s['end']) for s in self.spans if s['scope'] == 'all' or (key and s['scope'] == 'selected' and (name in s.get('devices', [])))]
        merged = []
        for start, end in sorted(spans):
            if merged and start <= merged[-1][1] + 180:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        self.coverage_cache[cache_key] = merged
        return merged

    def covered(self, key, start, end):
        return any((a <= start + 180 and b >= end - 180 for a, b in self.intervals(key)))

    def quiet_limit(self, key):
        device = self.devices[key]
        if device.get('manufacturer') == '_TZE200_pay2byax':
            return 2 * 3600
        return 2 * 3600 if device['role'] == 'Router' else 24 * 3600

    def add_silence_events(self):
        for key, times in self.seen.items():
            limit = self.quiet_limit(key)
            for a, b in zip(times, times[1:] + [max([s['end'] for s in self.spans] + [times[-1]])]):
                if b - a > limit and self.covered(key, a, b):
                    self.notifications.append({'t': a + limit, 'id': key, 'type': 'silent', 'text': 'Ticho po ' + str(limit // 3600) + ' h'})
                    if b in times:
                        self.notifications.append({'t': b, 'id': key, 'type': 'recovery', 'text': 'Komunikace obnovena', 'gap': b - a})

    @staticmethod
    def latest(values, times, when):
        i = bisect.bisect_right(times, when) - 1
        return values[i] if i >= 0 else None

    def frame(self, when, window=86400, highlight_window=3600):
        node_states = {}
        edges = {}
        cut = when - window
        latest_paths = {}
        for key, device in self.devices.items():
            seen_times = self.seen.get(key, [])
            index = bisect.bisect_right(seen_times, when) - 1
            last_seen = seen_times[index] if index >= 0 else None
            route = self.latest(self.routes.get(key, []), self.times.get(key, []), when)
            join = self.latest(self.joins.get(key, []), self.join_times.get(key, []), when)
            avail = self.latest(self.availability.get(key, []), self.avail_times.get(key, []), when)
            status = 'unknown'
            if last_seen is not None:
                status = 'observed' if when - last_seen <= self.quiet_limit(key) else 'unobserved'
                if status == 'unobserved' and self.covered(key, max(last_seen, when - self.quiet_limit(key)), when):
                    status = 'silent'
            if avail and (last_seen is None or avail['t'] >= last_seen):
                status = avail['state']
            if device['role'] == 'Coordinator':
                status = 'coordinator'
            parent_is_join = bool(join and (not route or join['t'] > route['t']))
            node_states[key] = {'status': status, 'last_seen': last_seen, 'last_route': route['t'] if route else None, 'parent': join['parent'] if parent_is_join else route['chain'][1] if route else None, 'parent_time': join['t'] if parent_is_join else route['t'] if route else None, 'parent_confirmed': bool(join and (not route or join['t'] > route['t']))}
            if route:
                kind = 'primary' if route['t'] >= cut else 'historical'
                superseded = join and device['role'] == 'EndDevice' and (join['t'] > route['t']) and (join['parent'] != route['chain'][1])
                if superseded:
                    kind = 'historical'
                chain = route['chain']
                latest_paths[key] = chain
                for source, target in zip(chain, chain[1:]):
                    self.add_edge(edges, source, target, key, kind, route['t'])
            if join and device['role'] == 'EndDevice' and (not route or join['t'] > route['t']):
                self.add_edge(edges, key, join['parent'], key, 'parent', join['t'])
        for key, rows in self.routes.items():
            start = bisect.bisect_left(self.times[key], cut)
            end = bisect.bisect_right(self.times[key], when)
            for route in rows[start:end]:
                for source, target in zip(route['chain'], route['chain'][1:]):
                    self.add_edge(edges, source, target, key, 'historical', route['t'])
        notifications = [e for e in self.notifications if cut < e['t'] <= when]
        grouped = {}
        for event in notifications:
            grouping = (event['type'], event.get('id'))
            count = grouped.get(grouping, {}).get('count', 0) + 1
            grouped[grouping] = {**event, 'count': count}
        visible = sorted(grouped.values(), key=lambda e: e['t'], reverse=True)[:200]
        for event in visible:
            key = event.get('id')
            if key in node_states and event['t'] >= when - highlight_window and (event['type'] != 'route' or event.get('parent_changed')):
                node_states[key].setdefault('events', []).append(event['type'])
        intervals = self.intervals()
        covered_seconds = sum((max(0, min(b, when) - max(a, cut)) for a, b in intervals))
        return {'t': when, 'nodes': node_states, 'edges': list(edges.values()), 'events': visible, 'coverage': round(min(1, covered_seconds / window), 4), 'paths': latest_paths, 'counts': {state: sum((n['status'] == state for n in node_states.values())) for state in ('observed', 'silent', 'offline', 'unknown', 'unobserved')}}

    @staticmethod
    def add_edge(edges, source, target, origin, kind, ts):
        if source == target:
            return
        key = source + '>' + target
        edge = edges.setdefault(key, {'id': key, 'a': source, 'b': target, 'kind': kind, 'sources': [], 'last': ts})
        rank = {'historical': 0, 'parent': 1, 'primary': 2}
        if rank[kind] > rank[edge['kind']]:
            edge['kind'] = kind
            edge['sources'] = []
        if kind == edge['kind'] and origin not in edge['sources']:
            edge['sources'].append(origin)
        edge['last'] = max(edge['last'], ts)

def overlap(a, b, padding=20):
    return abs(a['x'] - b['x']) < (a['w'] + b['w']) / 2 + padding and abs(a['y'] - b['y']) < (a['h'] + b['h']) / 2 + padding

def initial_layout(history, root, reset=False):
    path = root / 'layout.json'
    layout = json.loads(path.read_text()) if path.exists() and (not reset) else {'version': 1, 'nodes': {}}
    nodes = layout['nodes']
    fresh = not nodes
    for key, device in history.devices.items():
        if key in nodes:
            continue
        width = min(240, max(120, len(device['name']) * 6.7 + 24))
        nodes[key] = {'x': 0, 'y': 0, 'w': width, 'h': 42, 'new': True}
        if device['role'] == 'Coordinator':
            nodes[key].update(w=100, h=100)
    if fresh and shutil.which('sfdp'):
        lines = ['digraph G { graph [overlap=prism, sep="+32", start=42, K=1.2, pack=true]; node [shape=box, fixedsize=true];']
        for key, node in nodes.items():
            lines.append(f"""{json.dumps(key)} [label="", width={node['w'] / 72}, height={node['h'] / 72}];""")
        linked = set()
        for rows in history.routes.values():
            for row in rows:
                for edge in zip(row['chain'], row['chain'][1:]):
                    if edge not in linked:
                        lines.append(f'{json.dumps(edge[0])} -> {json.dumps(edge[1])};')
                        linked.add(edge)
        for rows in history.joins.values():
            for row in rows:
                edge = (row['id'], row['parent'])
                if edge not in linked:
                    lines.append(f'{json.dumps(edge[0])} -> {json.dumps(edge[1])};')
                    linked.add(edge)
        lines.append('}')
        try:
            result = subprocess.run(['sfdp', '-Tjson'], input='\n'.join(lines), capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            if 'triangulation' not in (error.stderr or ''):
                raise
            # Alpine builds can omit Graphviz's optional triangulation library.
            # fdp supports scaling without it; positions are cached afterwards.
            portable = '\n'.join(lines).replace('overlap=prism', 'overlap=scale')
            result = subprocess.run(['fdp', '-Tjson'], input=portable, capture_output=True, text=True, check=True)
        graph = json.loads(result.stdout)
        for entry in graph.get('objects', []):
            if entry.get('name') in nodes and entry.get('pos'):
                x, y = map(float, entry['pos'].split(','))
                nodes[entry['name']].update(x=x * 1.45, y=-y * 1.45, new=False)
    placed = [v for v in nodes.values() if not v.get('new')]
    for key in sorted(nodes):
        node = nodes[key]
        if not node.pop('new', False):
            continue
        candidates = []
        for row in history.routes.get(key, [])[-1:]:
            candidates += [nodes[x] for x in row['chain'][1:] if x in nodes and nodes[x] in placed]
        center = candidates[0] if candidates else {'x': 0, 'y': 0}
        for index in range(1, 20000):
            angle = index * 2.399963
            radius = math.sqrt(index) * 55
            node.update(x=center['x'] + radius * math.cos(angle), y=center['y'] + radius * math.sin(angle))
            if not any((overlap(node, other, 35) for other in placed)):
                break
        placed.append(node)
    dump(path, layout)
    return layout

def intersects(p, q, rect, pad=0):
    left, right = (rect['x'] - rect['w'] / 2 - pad, rect['x'] + rect['w'] / 2 + pad)
    top, bottom = (rect['y'] - rect['h'] / 2 - pad, rect['y'] + rect['h'] / 2 + pad)
    t0, t1 = (0.0, 1.0)
    dx, dy = (q[0] - p[0], q[1] - p[1])
    for v, d, lo, hi in ((p[0], dx, left, right), (p[1], dy, top, bottom)):
        if not d:
            if not lo < v < hi:
                return False
        else:
            a, b = sorted(((lo - v) / d, (hi - v) / d))
            t0, t1 = (max(t0, a), min(t1, b))
            if t0 >= t1:
                return False
    return t0 < t1

class Router:
    STEP = 12
    DIRS = [(1, 0), (-1, 0), (0, 1), (0, -1), (1, 1), (1, -1), (-1, 1), (-1, -1)]

    def __init__(self, layout):
        self.nodes = layout['nodes']
        self.left = math.floor(min((n['x'] - n['w'] / 2 for n in self.nodes.values())) / 12) * 12 - 120
        self.top = math.floor(min((n['y'] - n['h'] / 2 for n in self.nodes.values())) / 12) * 12 - 120
        self.width = math.ceil((max((n['x'] + n['w'] / 2 for n in self.nodes.values())) - self.left + 120) / 12)
        self.height = math.ceil((max((n['y'] + n['h'] / 2 for n in self.nodes.values())) - self.top + 120) / 12)
        self.blocked = set()
        for node in self.nodes.values():
            a = self.grid((node['x'] - node['w'] / 2 - 14, node['y'] - node['h'] / 2 - 14))
            b = self.grid((node['x'] + node['w'] / 2 + 14, node['y'] + node['h'] / 2 + 14))
            for x in range(a[0], b[0] + 1):
                for y in range(a[1], b[1] + 1):
                    self.blocked.add((x, y))

    def grid(self, p):
        return (round((p[0] - self.left) / 12), round((p[1] - self.top) / 12))

    def point(self, p):
        return (self.left + p[0] * 12, self.top + p[1] * 12)

    def port(self, node, other, side=None):
        dx, dy = (other['x'] - node['x'], other['y'] - node['y'])
        if side is None:
            side = ('R' if dx >= 0 else 'L') if abs(dx) / node['w'] > abs(dy) / node['h'] else 'B' if dy >= 0 else 'T'
        if side in ('R', 'L'):
            sign = 1 if side == 'R' else -1
            anchor = (node['x'] + sign * (node['w'] / 2 + 3), node['y'])
            outside = (anchor[0] + sign * 36, anchor[1])
        else:
            sign = 1 if side == 'B' else -1
            anchor = (node['x'], node['y'] + sign * (node['h'] / 2 + 3))
            outside = (anchor[0], anchor[1] + sign * 36)
        return (anchor, self.grid(outside))

    def nearest_free(self, p):
        for radius in range(0, 15):
            candidates = [(p[0] + x, p[1] + y) for x in range(-radius, radius + 1) for y in range(-radius, radius + 1) if max(abs(x), abs(y)) == radius]
            candidates.sort(key=lambda q: (abs(q[0] - p[0]) + abs(q[1] - p[1]), q))
            for q in candidates:
                if q not in self.blocked:
                    return q
        raise RuntimeError('No free port')

    def path(self, a, b):
        try:
            return self._path(a, b)
        except RuntimeError:
            candidates = []
            for side_a in ('R', 'L', 'B', 'T'):
                for side_b in ('R', 'L', 'B', 'T'):
                    p, _ = self.port(self.nodes[a], self.nodes[b], side_a)
                    q, _ = self.port(self.nodes[b], self.nodes[a], side_b)
                    candidates.append((math.dist(p, q), side_a, side_b))
            for _, side_a, side_b in sorted(candidates):
                try:
                    return self._path(a, b, side_a, side_b)
                except RuntimeError:
                    pass
        raise RuntimeError('No obstacle-free route from any port')

    def _path(self, a, b, side_a=None, side_b=None):
        first, start = self.port(self.nodes[a], self.nodes[b], side_a)
        last, goal = self.port(self.nodes[b], self.nodes[a], side_b)
        start, goal = (self.nearest_free(start), self.nearest_free(goal))
        queue = [(0, start)]
        cost = {start: 0}
        previous = {}
        while queue:
            _, current = heapq.heappop(queue)
            if current == goal:
                points = [current]
                while current in previous:
                    current = previous[current]
                    points.append(current)
                points = [first, *[self.point(v) for v in reversed(points)], last]
                compact = [points[0]]
                for i in range(1, len(points) - 1):
                    p, q, r = (compact[-1], points[i], points[i + 1])
                    if abs((q[0] - p[0]) * (r[1] - q[1]) - (q[1] - p[1]) * (r[0] - q[0])) > 0.01:
                        compact.append(q)
                compact.append(points[-1])
                for key, node in self.nodes.items():
                    if key not in (a, b) and any((intersects(p, q, node, 5) for p, q in zip(compact, compact[1:]))):
                        raise RuntimeError('Port connection intersects ' + key)
                return [[round(x, 2), round(y, 2)] for x, y in compact]
            for dx, dy in self.DIRS:
                nxt = (current[0] + dx, current[1] + dy)
                if not (0 <= nxt[0] <= self.width and 0 <= nxt[1] <= self.height) or nxt in self.blocked:
                    continue
                if dx and dy and ((current[0] + dx, current[1]) in self.blocked or (current[0], current[1] + dy) in self.blocked):
                    continue
                extra = math.sqrt(2) if dx and dy else 1
                if current in previous:
                    prev = previous[current]
                    if (current[0] - prev[0], current[1] - prev[1]) != (dx, dy):
                        extra += 0.45
                new_cost = cost[current] + extra
                if new_cost < cost.get(nxt, float('inf')):
                    cost[nxt] = new_cost
                    previous[nxt] = current
                    heuristic = math.hypot(goal[0] - nxt[0], goal[1] - nxt[1])
                    heapq.heappush(queue, (new_cost + heuristic, nxt))
        raise RuntimeError('No obstacle-free route')

def svg_path(points):
    return 'M ' + ' L '.join((f'{x:.2f},{y:.2f}' for x, y in points))

def static_svg(frame, world, label):
    left, top, width, height = world['bounds']
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{left} {top - 100} {width} {height + 100}">', '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0 0L10 5L0 10Z" fill="context-stroke"/></marker></defs>', f'<rect x="{left}" y="{top - 100}" width="{width}" height="{height + 100}" fill="#f6f8fc"/>', f'<text x="{left + 24}" y="{top - 52}" font-family="sans-serif" font-size="24" fill="#14263c">{html.escape(label)}</text>', f'<text x="{left + 24}" y="{top - 22}" font-family="sans-serif" font-size="13" fill="#526477">Sytě: poslední pozorované cesty · šedě: jiné historické cesty · přerušovaně fialově: rodič při připojení</text>']
    for edge in sorted(frame['edges'], key=lambda e: e['kind'] != 'historical'):
        geometry = world['edges'].get(edge['id'])
        if not geometry:
            continue
        kind = edge['kind']
        color = {'primary': '#087d9c', 'historical': '#c4cdd9', 'parent': '#8055c9'}[kind]
        width = 1.2 if kind == 'historical' else 2 + math.log2(len(edge['sources']) + 1) * 0.5
        dash = ' stroke-dasharray="6 5"' if kind == 'parent' else ''
        out.append(f'<path d="{svg_path(geometry)}" fill="none" stroke="{color}" stroke-width="{width}"{dash} marker-end="url(#arrow)"/>')
    for key, node in world['nodes'].items():
        status = frame['nodes'].get(key, {}).get('status', 'unknown')
        color = '#d85d46' if status == 'offline' else '#d48b24' if status == 'silent' else '#697c94'
        flags = frame['nodes'].get(key, {}).get('events', [])
        if status in ('observed', 'online'):
            if any((f in flags for f in ('parent', 'rejoin', 'route'))):
                color = '#9561bf'
            if any((f in flags for f in ('recovery', 'online'))):
                color = '#249b80'
        fill = '#d2eddf' if node['role'] == 'Coordinator' else '#fff0c2' if node['role'] == 'Router' else '#dceeff'
        if status in ('unknown', 'unobserved'):
            fill = '#edf0f5'
        x, y, w, h = (node[v] for v in ('x', 'y', 'w', 'h'))
        title = html.escape(node['name'] + ' · ' + status)
        out.append(f'<g><title>{title}</title>')
        if node['role'] in ('EndDevice', 'Coordinator'):
            out.append(f'<ellipse cx="{x}" cy="{y}" rx="{w / 2}" ry="{h / 2}" fill="{fill}" stroke="{color}" stroke-width="2"/>')
        else:
            out.append(f'<rect x="{x - w / 2}" y="{y - h / 2}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="{color}" stroke-width="2"/>')
        out.append(f'''<text x="{x}" y="{y + 4}" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#23364b">{html.escape(node['name'])}</text></g>''')
    return '\n'.join(out + ['</svg>'])

def render(db, root, output, reset_layout=False):
    old_manifest = {}
    old_path = output / 'manifest.js'
    if old_path.exists():
        try:
            old_manifest = json.loads(old_path.read_text().removeprefix('window.ZigbeeManifest=').strip().removesuffix(';'))
        except ValueError:
            pass
    row = db.execute("SELECT value FROM settings WHERE key='dirty_since'").fetchone()
    dirty_since = int(row[0]) if row else int(time.time()) + 1
    since = min(dirty_since, old_manifest.get('data_until', dirty_since)) - 86400 if old_manifest.get('frames') else None
    history = History(db, since)
    if not history.events:
        raise RuntimeError('No input events')
    output.mkdir(parents=True, exist_ok=True)
    layout = initial_layout(history, root, reset_layout)
    router = Router(layout)
    signature = hashlib.sha256(json.dumps(layout, sort_keys=True).encode()).hexdigest()
    reuse = {entry['id']: entry for entry in old_manifest.get('frames', [])} if old_manifest.get('schema') == SCHEMA else {}
    cache_path = root / 'edge-paths.json'
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    if cache.get('signature') != signature:
        old_edge_keys = list(cache.get('edges', {}))
        cache = {'signature': signature, 'edges': {}}
        for key in old_edge_keys:
            a, b = key.split('>')
            if a in layout['nodes'] and b in layout['nodes']:
                cache['edges'][key] = router.path(a, b)
    world = {'nodes': {key: {**history.devices.get(key, {'name': key, 'role': 'Unknown'}), **node} for key, node in layout['nodes'].items()}, 'edges': cache['edges'], 'bounds': [router.left, router.top, router.width * 12, router.height * 12], 'schema': SCHEMA}
    start = dt.datetime.fromtimestamp(history.first, ZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    times = set(range(int(start.timestamp()), history.end + 1, 3600))
    daily = set()
    day = start.date()
    last_day = dt.datetime.fromtimestamp(history.end, ZONE).date()
    while day <= last_day:
        tomorrow = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), ZONE)
        stamp = min(int(tomorrow.timestamp()) - 1, history.end)
        times.add(stamp)
        daily.add(stamp)
        day += dt.timedelta(days=1)
    important = {e['t'] for e in history.notifications if e['type'] in ('parent', 'rejoin', 'recovery', 'silent', 'offline', 'online', 'system', 'manual')}
    times.update(important)
    times.add(history.end)
    for entry in old_manifest.get('frames', []):
        local = dt.datetime.fromtimestamp(entry['t'], ZONE)
        if 'event' in entry['tags'] or local.minute == local.second == 0 or local.time().replace(tzinfo=None) == dt.time(23, 59, 59):
            times.add(entry['t'])
            if local.time().replace(tzinfo=None) == dt.time(23, 59, 59):
                daily.add(entry['t'])
    manifest = []
    geometry_failures = {}
    first = min([history.first] + [entry['t'] for entry in old_manifest.get('frames', [])])
    for when in sorted((t for t in times if first <= t <= history.end)):
        local = dt.datetime.fromtimestamp(when, ZONE)
        identity = str(when)
        path = 'frames/' + local.strftime('%Y-%m-%d/') + identity + '.js'
        tags = []
        if when in daily:
            tags.append('day')
        if local.minute == local.second == 0:
            tags.append('hour')
        if when in important:
            tags.append('event')
        if identity in reuse and 'event' in reuse[identity]['tags']:
            tags.append('event')
        if when == history.end:
            tags.extend(['latest', 'hour', 'day'])
        if identity in reuse and when < dirty_since and (output / path).exists():
            manifest.append({**reuse[identity], 'tags': sorted(set(tags))})
            continue
        frame = history.frame(when, highlight_window=86400 if when in daily else 3600)
        for edge in frame['edges']:
            if edge['id'] not in world['edges'] and edge['id'] not in geometry_failures:
                try:
                    world['edges'][edge['id']] = router.path(edge['a'], edge['b'])
                except RuntimeError as exc:
                    geometry_failures[edge['id']] = str(exc)
        target = output / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('window.ZigbeeFrames[' + json.dumps(identity) + ']=' + json.dumps(frame, ensure_ascii=False, separators=(',', ':')) + ';\n')
        manifest.append({'id': identity, 't': when, 'label': local.strftime('%d. %m. %Y · %H:%M %Z'), 'date': local.strftime('%Y-%m-%d'), 'file': path, 'tags': sorted(set(tags)), 'counts': frame['counts'], 'coverage': frame['coverage']})
        if len(manifest) % 100 == 0:
            print(json.dumps({'render_progress': len(manifest), 'paths': len(world['edges']), 'unrouted': len(geometry_failures)}), flush=True)
        if when in daily:
            (output / 'snapshots').mkdir(exist_ok=True)
            (output / 'snapshots' / (local.strftime('%Y-%m-%d') + '.svg')).write_text(static_svg(frame, world, 'Zigbee · ' + local.strftime('%d. %m. %Y %H:%M')))
    dump(cache_path, cache)
    world['routing_failures'] = geometry_failures
    (output / 'world.js').write_text('window.ZigbeeWorld=' + json.dumps(world, ensure_ascii=False, separators=(',', ':')) + ';\n')
    row = db.execute("SELECT value FROM settings WHERE key='last_poll'").fetchone()
    result = {'schema': SCHEMA, 'generated': int(time.time()), 'data_until': history.end, 'last_poll': int(row[0]) if row else None, 'frames': manifest, 'devices': len(history.devices), 'route_records': sum(map(len, history.routes.values())), 'unrouted_edges': len(geometry_failures), 'layout_signature': signature}
    (output / 'manifest.js').write_text('window.ZigbeeManifest=' + json.dumps(result, ensure_ascii=False, separators=(',', ':')) + ';\n')
    for file in (HERE / 'web').iterdir():
        if file.is_file():
            if file.suffix == '.html':
                (output / file.name).write_text(file.read_text().replace('__BUILD__', str(int(time.time()))))
            else:
                shutil.copy2(file, output / file.name)
    if manifest:
        shutil.copy2(output / 'snapshots' / (manifest[-1]['date'] + '.svg'), output / 'latest.svg')
    dump(output / 'status.json', {key: value for key, value in result.items() if key != 'frames'})
    with db:
        db.execute("INSERT OR REPLACE INTO settings VALUES('last_render',?)", (str(int(time.time())),))
        db.execute("DELETE FROM settings WHERE key='dirty_since'")
    print(json.dumps({'operation': 'render', 'frames': len(manifest), 'nodes': len(world['nodes']), 'edges': len(world['edges']), 'unrouted_edges': len(geometry_failures), 'output': str(output)}))

def maintain(db, days=30):
    previous = db.execute("SELECT value FROM settings WHERE key='last_maintenance'").fetchone()
    if previous and int(previous[0]) // 86400 == int(time.time()) // 86400:
        return
    cutoff = int(time.time()) - days * 86400
    with db:
        db.execute('CREATE TEMP TABLE IF NOT EXISTS preserved_keys(key TEXT PRIMARY KEY)')
        db.execute('DELETE FROM preserved_keys')
        for entity, kind in db.execute('SELECT entity,k FROM identities').fetchall():
            row = db.execute('SELECT key FROM events WHERE entity=? AND k=? AND t<? ORDER BY t DESC,key DESC LIMIT 1', (entity, kind, cutoff)).fetchone()
            if row:
                db.execute('INSERT OR IGNORE INTO preserved_keys VALUES(?)', row)
        deleted = db.execute('DELETE FROM events WHERE t<? AND key NOT IN(SELECT key FROM preserved_keys)', (cutoff,)).rowcount
        db.execute('DELETE FROM coverage WHERE end<?', (cutoff,))
        db.execute("INSERT OR REPLACE INTO settings VALUES('last_maintenance',?)", (str(int(time.time())),))
    print(json.dumps({'detail_retention_days': days, 'pruned_events': deleted, 'archived_frames_preserved': True}))
