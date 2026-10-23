"""Passive Zigbee2MQTT map engine. No radio commands or credentials."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from zoneinfo import ZoneInfo
STAMP = re.compile('^\\[([\\d-]+ [\\d:]+)\\]')
ROUTE = re.compile('RouteRecordHandler: source=(\\d+) sourceEui=(0x[\\da-f]+).*?lastHopLqi=(\\d+) lastHopRssi=(-?\\d+).*?relayList=([\\d,]*)', re.I)
JOIN = re.compile('TrustCenterJoinHandler: newNodeId=(\\d+) newNodeEui64=(0x[\\da-f]+) status=(\\S+).*?parentOfNewNodeId=(\\d+)', re.I)
RX = re.compile('IncomingMessageHandler:.*?"senderShortId":(\\d+)')
NAMED_RX = re.compile("Received Zigbee message from '([^']+)'")
AVAIL = re.compile("topic '(.+)/availability', payload '(.*?)'$")

def timestamp(line, previous=None, zone=None):
    match = STAMP.match(line)
    if not match:
        return None
    wall = dt.datetime.strptime(match[1], '%Y-%m-%d %H:%M:%S')
    tz = ZoneInfo(zone or os.environ.get('ATLAS_TIMEZONE','Europe/Prague'))
    candidates = [wall.replace(tzinfo=tz, fold=i).timestamp() for i in (0, 1)]
    if previous is not None and candidates[0] < previous - 1800 <= candidates[1]:
        return int(candidates[1])
    return int(candidates[0])

def parse_line(line, previous=None, zone=None):
    ts = timestamp(line, previous, zone)
    if ts is None:
        return (None, None)
    match = ROUTE.search(line)
    if match:
        nwk, ieee, lqi, rssi, relays = match.groups()
        return (ts, {'t': ts, 'k': 'route', 'id': ieee.lower(), 'nwk': int(nwk), 'relays': [int(x) for x in relays.split(',') if x], 'lqi': int(lqi), 'rssi': int(rssi)})
    match = JOIN.search(line)
    if match:
        nwk, ieee, status, parent = match.groups()
        return (ts, {'t': ts, 'k': 'leave' if status == 'DEVICE_LEFT' else 'join', 'id': ieee.lower(), 'nwk': int(nwk), 'parent_nwk': int(parent), 'status': status})
    match = RX.search(line)
    if match:
        return (ts, {'t': ts, 'k': 'seen', 'nwk': int(match[1])})
    match = NAMED_RX.search(line)
    if match:
        return (ts, {'t': ts, 'k': 'seen', 'name': match[1]})
    match = AVAIL.search(line)
    if match:
        prefix=os.environ.get('ATLAS_MQTT_BASE_TOPIC','zigbee2mqtt')+'/'
        if not match[1].startswith(prefix):return (ts,None)
        name=match[1][len(prefix):]
        try:
            state = json.loads(match[2])
        except ValueError:
            state = match[2]
        if isinstance(state, dict):
            state = state.get('state')
        if state in ('online', 'offline'):
            return (ts, {'t': ts, 'k': 'availability', 'name': name, 'state': state})
    if 'Starting Zigbee2MQTT' in line:
        return (ts, {'t': ts, 'k': 'system', 'message': 'Start Zigbee2MQTT'})
    return (ts, None)

def metadata(config_root):
    names = {}
    current = None
    in_devices = False
    text = (config_root / 'configuration.yaml').read_text()
    for line in text.splitlines():
        if line.startswith('devices:'):
            in_devices = True
            continue
        if line and (not line[0].isspace()) and (not line.startswith('#')):
            in_devices = False
            current = None
        if not in_devices:
            continue
        match = re.match('^  [\'\\"]?(0x[\\da-fA-F]+)[\'\\"]?:\\s*$', line)
        if match:
            current = match[1].lower()
        match = re.match('^    friendly_name:\\s*(.*?)\\s*$', line)
        if match and current:
            names[current] = match[1].strip('\'"')
    devices = []
    for line in (config_root / 'database.db').read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not row.get('ieeeAddr'):
            continue
        ieee = row['ieeeAddr'].lower()
        devices.append({'id': ieee, 'name': names.get(ieee, 'Koordinátor' if row.get('type') == 'Coordinator' else ieee), 'nwk': row.get('nwkAddr'), 'role': row.get('type', 'Unknown'), 'model': row.get('modelId', ''), 'manufacturer': row.get('manufName', '')})
    return devices

def read_paths(paths, checkpoint=None, scope='all', monitored=None):
    checkpoint = checkpoint or {}
    offsets = checkpoint.get('offsets', {})
    updated = {}
    events = {}
    seen = {}
    spans = []
    for path in sorted(paths, key=lambda p: (p.stat().st_mtime, str(p))):
        try:
            stat = path.stat()
            key = f'{stat.st_dev}:{stat.st_ino}'
            old = offsets.get(key, {})
            offset = old.get('offset', 0)
            if offset > stat.st_size:
                offset = 0
            previous = old.get('previous') if offset else int(stat.st_mtime) if scope == 'all' else None
            first = last = None
            intervals = []
            with path.open('rb') as handle:
                handle.seek(offset)
                while True:
                    position = handle.tell()
                    raw = handle.readline()
                    if not raw or not raw.endswith(b'\n'):
                        handle.seek(position)
                        break
                    line = raw.decode('utf-8', 'replace')
                    ts, event = parse_line(line, previous)
                    if ts is not None:
                        previous = ts
                    if ts is not None and event and (event['k'] in ('route', 'seen', 'join')):
                        if last is not None and ts - last > 180:
                            intervals.append((first, last))
                            first = None
                        first = ts if first is None else first
                        last = ts
                    if not event:
                        continue
                    if event['k'] == 'seen':
                        identity = event.get('name', event.get('nwk'))
                        bucket = (event['t'] // 60, identity)
                        seen.setdefault((*bucket, 'first'), {**event, 'point': 'first'})
                        seen[*bucket, 'last'] = {**event, 'point': 'last'}
                    else:
                        events[json.dumps(event, sort_keys=True)] = event
                updated[key] = {'offset': handle.tell(), 'previous': previous}
            if first is not None:
                intervals.append((first, last))
            for start, end in intervals:
                spans.append({'start': start, 'end': end, 'scope': scope, 'devices': monitored or [], 'source': str(path)})
        except FileNotFoundError:
            continue
    seen_events = [e for key, e in seen.items() if key[-1] == 'first' or e['t'] != seen[*key[:-1], 'first']['t']]
    events = list(events.values()) + seen_events
    events.sort(key=lambda e: (e['t'], e['k']))
    return {'events': events, 'coverage': spans, 'checkpoint': {'offsets': updated}, 'read_at': int(time.time())}
