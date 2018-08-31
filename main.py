#!/usr/bin/env python3
# -*- coding: ascii -*-

import os, re
import collections
import threading
import json
import argparse
import logging
import sqlite3

import basebot
import instabot

import autolinker

INSTANT_ROOM_TEMPLATE = os.environ.get('INSTANT_ROOM_TEMPLATE',
                                       'wss://instant.leet.nu/room/{}/ws')

NICKNAME = 'bridge'
SURROGATE_DELAY = 2
MAX_LOG_REQUEST = 100

HELP_TEMPLATE = ('I relay messages between a Euphoria room (&%(euphoria)s) '
    'and an Instant room (&%(instant)s).')

# UNIX timestampf for 2014-12-00 00:00:00 UTC. Note that the original
# definition has an off-by-one error.
EUPHORIA_ID_EPOCH = 1417305600

# Instant's URL regex.
INSTANT_URL_RE = re.compile(r'((?!javascript:)[a-zA-Z]+:(//)?)?'
    r'([a-zA-Z0-9._~-]+@)?([a-zA-Z0-9.-]+)(:[0-9]+)?(/[^>]*)?')

# Extended version of the URL regex.
INSTANT_URL_SEARCH = re.compile('<!?(' + INSTANT_URL_RE.pattern + ')>')

# Approximation of URL-s that Euphoria would auto-embed.
IMAGE_URL = re.compile(r'^(https?://)?((i\.)?imgur\.com|i\.ytimg\.com|'
    r'imgs\.xkcd\.com)\b')

# Python's standard library is sometimes rather short-sighted, in particular
# when it comes to inverses for some type conversions...
def base_encode(number, base=10, pad=0):
    if isinstance(base, int):
        alphabet = '0123456789abcdefghijklmnopqrstuvwxyz'[:base]
    else:
        alphabet = base
        base = len(alphabet)
    ret = []
    while 1:
        number, digit = divmod(number, base)
        ret.append(alphabet[digit])
        if number == 0: break
    if pad > len(ret):
        ret.extend(('0',) * (len(ret) - pad))
    return ''.join(reversed(ret))

def ping_matches(ping, nick):
    if not ping.startswith('@'): return False
    return basebot.normalize_nick(ping[1:]) == basebot.normalize_nick(nick)

class EuphoriaBot(basebot.HeimEndpoint):
    def submit_post(self, parent, text, sequence=None, callback=None):
        packet = {'type': 'send',
                  'data': {'parent': parent, 'content': text}}
        if sequence is not None:
            packet['id'] = sequence
            if callback is not None:
                self.callbacks[sequence] = callback
        self.send_raw(packet)

# We shoehorn instabot Bot-s into the interface expected by basebot in order
# to leverage the latter's more powerful tools.
class InstantBot(instabot.Bot):
    def __init__(self, roomname, nickname=Ellipsis, **kwds):
        url = INSTANT_ROOM_TEMPLATE.format(roomname)
        instabot.Bot.__init__(self, url, nickname, keepalive=True, **kwds)
        self.roomname = roomname
        self.manager = kwds.get('manager')
        self.logger = kwds.get('logger', logging.getLogger())
        self.lock = threading.RLock()
        self.callbacks = {}
        self._sent_nick = Ellipsis

    def connect(self):
        self.logger.info('Connecting to %s...', self.url)
        return instabot.Bot.connect(self)

    def on_open(self):
        instabot.Bot.on_open(self)
        self.logger.info('Connected')

    def on_connection_error(self, exc):
        self.logger.warning('Exception while connecting: %r', exc)

    def handle_response(self, content, rawmsg):
        instabot.Bot.handle_response(self, content, rawmsg)
        cb = self.callbacks.pop(content.get('seq'), None)
        if cb: cb(content)

    def on_close(self, final):
        instabot.Bot.on_close(self, final)
        self.logger.info('Closing')
        # Instabot does not expose the "ok" parameter.
        if self.manager: self.manager.handle_close(self, True, final)

    def send_nick(self, peer=None):
        if self.nickname != self._sent_nick:
            if self.nickname is not None:
                self.logger.info('Setting nickname: %r', self.nickname)
            self._sent_nick = self.nickname
        return instabot.Bot.send_nick(self, peer)

    def set_nickname(self, nick=Ellipsis):
        if nick is not Ellipsis: self.nickname = nick
        self.send_nick()

    def submit_post(self, parent, text, sequence=None, callback=None):
        packet = {'type': 'broadcast',
                  'data': {'type': 'post', 'parent': parent,
                           'nick': self.nickname, 'text': text}}
        if sequence is not None:
            packet['seq'] = sequence
            if callback is not None:
                self.callbacks[sequence] = callback
        self.send_raw(json.dumps(packet, separators=(',', ':')))

class EuphoriaBridgeBot(EuphoriaBot):
    def handle_any(self, packet):
        EuphoriaBot.handle_any(self, packet)
        add_users, remove_users, users_new = None, None, False
        # Switch on various packet types.
        if packet.type == 'who-reply':
            add_users = packet.data
        elif packet.type == 'hello-event':
            self.manager.nexus.ignore_users(({
                'platform': 'euphoria',
                'euphoria_id': packet.data['session'].session_id
            },))
        elif packet.type == 'snapshot-event':
            self.manager.nexus.gather_ids('euphoria', [msg.id
                for msg in packet.data['log']])
            add_users = packet.data['listing']
        elif packet.type == 'network-event':
            if packet.data['type'] == 'partition':
                self.manager.nexus.remove_group(('euphoria',
                    packet.data['server_id'], packet.data['server_era']))
        elif packet.type == 'nick-event':
            self.manager.nexus.add_users(({
                'platform': 'euphoria',
                'euphoria_id': packet.data['session_id'],
                'nick': packet.data['to']
            },))
        elif packet.type == 'join-event':
            add_users = (packet.data,)
            users_new = True
        elif packet.type == 'part-event':
            remove_users = (packet.data,)
        elif packet.type == 'send-event':
            self.manager.nexus.handle_message({
                'platform': 'euphoria',
                'euphoria_id': packet.data.sender.session_id,
                'msgid': packet.data.id,
                'parent': packet.data.parent,
                'nick': packet.data.sender.name,
                'text': packet.data.content
            })
        # Apply user additions/deletions.
        if add_users:
            self.manager.nexus.add_users([{'platform': 'euphoria',
                    'euphoria_id': entry.session_id, 'nick': entry.name,
                    'group': ('euphoria', entry.server_id, entry.server_era)}
                for entry in add_users], users_new)
        if remove_users:
            self.manager.nexus.remove_users([{'euphoria_id': entry.session_id}
                                             for entry in remove_users])

    def query_logs(self, before, after, maxlen, callback):
        def process(response):
            callback(response.data['log'])
        if before is not None:
            # Euphoria returns results *not* including before, while Instant
            # returns results *including* before.
            before = base_encode(int(before, 36) - 1, 36, 13)
        if after is not None:
            raise RuntimeError('Cannot request Euphoria logs with a lower '
                'bound')
        self.send_packet('log', n=maxlen, before=before,
                         _callback=process)

class InstantBridgeBot(InstantBot):
    def on_open(self):
        InstantBot.on_open(self)
        self.send_broadcast({'type': 'who'})

    def handle_identity(self, content, rawmsg):
        InstantBot.handle_identity(self, content, rawmsg)
        self.manager.nexus.ignore_users(({
            'instant_id': self.identity['id']
        },))

    def handle_joined(self, content, rawmsg):
        InstantBot.handle_joined(self, content, rawmsg)
        self.manager.nexus.add_users(({
            'platform': 'instant',
            'instant_id': content['data']['id']
        },), True)

    def handle_left(self, content, rawmsg):
        InstantBot.handle_left(self, content, rawmsg)
        self.manager.nexus.remove_users(({
            'instant_id': content['data']['id']
        },))

    def on_client_message(self, data, content, rawmsg):
        def send_log(messages):
            self.send_unicast(content['from'], {'type': 'log',
                'key': data.get('key'), 'data': messages})
        InstantBot.on_client_message(self, data, content, rawmsg)
        tp = data.get('type')
        if tp == 'nick':
            self.manager.nexus.add_users(({
                'platform': 'instant',
                'instant_id': content['from'],
                'nick': data.get('nick')
            },))
        elif tp == 'post':
            self.manager.nexus.handle_message({
                'platform': 'instant',
                'instant_id': content['from'],
                'msgid': content['id'],
                'parent': data.get('parent'),
                'nick': data.get('nick'),
                'text': data.get('text')
            })
        elif tp == 'log-query':
            bounds = self.manager.nexus.message_bounds('instant')
            if bounds[2]:
                self.send_unicast(content['from'], {'type': 'log-info',
                    'from': bounds[0], 'to': bounds[1], 'length': bounds[2]})
        elif tp == 'log-request':
            self.manager.nexus.request_messages('instant', data.get('to'),
                data.get('from'), data.get('length'), send_log)

class EuphoriaSendBot(EuphoriaBot):
    def __init__(self, roomname=None, **config):
        config.setdefault('roomname', roomname)
        EuphoriaBot.__init__(self, **config)
        self.ready = False
        self.on_ready = config.get('on_ready')

    def on_hello_event(self, packet):
        EuphoriaBot.on_hello_event(self, packet)
        self.manager.nexus.ignore_users(({
            'platform': 'euphoria',
            'euphoria_id': packet.data['session'].session_id,
        },))

    def handle_any(self, packet):
        EuphoriaBot.handle_any(self, packet)
        if packet.type == 'send-reply':
            if (isinstance(packet.id, str) and
                    packet.id.startswith('instant:')):
                self.manager.nexus.add_mapping({
                    'euphoria': packet.data.id,
                    'instant': packet.id.partition(':')[2]
                })

    def handle_login(self):
        EuphoriaBot.handle_login(self)
        if not self.ready:
            self.ready = True
            if self.on_ready: self.on_ready()

class InstantSendBot(InstantBot):
    def __init__(self, roomname, nickname=None, **kwds):
        InstantBot.__init__(self, roomname, nickname, **kwds)
        self.ready = False
        self.on_ready = kwds.get('on_ready')

    def handle_identity(self, content, rawmsg):
        InstantBot.handle_identity(self, content, rawmsg)
        self.manager.nexus.ignore_users(({
            'platform': 'instant',
            'instant_id': content['data']['id']
        },))
        if not self.ready:
            self.ready = True
            if self.on_ready: self.on_ready()

    def handle_response(self, content, rawmsg):
        InstantBot.handle_response(self, content, rawmsg)
        sequence = content.get('seq')
        if isinstance(sequence, str) and sequence.startswith('euphoria:'):
            self.manager.nexus.add_mapping({
                'euphoria': sequence.partition(':')[2],
                'instant': content['data']['id']
            })

def euphoria_id_to_timestamp(msgid):
    # The return value is a UNIX timestamp in milliseconds.
    return (int(msgid, 36) >> 22) + EUPHORIA_ID_EPOCH * 1000

def timestamp_to_instant_id(ts, sequence):
    # ts is a timestamp as returned by euphoria_id_to_timestamp(), sequence
    # should be in the [0, 1024) range, and the return value is a proper
    # string representation.
    return '%016X' % ((ts << 10) + sequence)

class MessageStore:
    def __init__(self, dbname=None):
        if dbname is None: dbname = ':memory:'
        self.dbname = dbname
        self.conn = None
        self.curs = None
        self.watchers = {}
        self.lock = threading.RLock()

    def init(self):
        self.conn = sqlite3.connect(self.dbname, check_same_thread=False)
        sync = os.environ.get('BRIDGE_DB_SYNC', '')
        if re.match('^[A-Za-z09-9]+$', sync):
            self.conn.execute('PRAGMA synchronous = ' + sync)
        self.curs = self.conn.cursor()
        # Either ID can be null to signal that it is not available yet.
        self.curs.execute('CREATE TABLE IF NOT EXISTS id_map ( '
                'euphoria TEXT UNIQUE, '
                'instant TEXT UNIQUE'
            ')')

    def gc(self):
        with self.lock:
            self.curs.execute('DELETE FROM id_map WHERE euphoria IS NULL OR '
                'instant IS NULL')
            self.conn.commit()
            return self.curs.rowcount

    def close(self):
        with self.lock:
            try:
                self.curs.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass

    def get_bounds(self):
        with self.lock:
            self.curs.execute('SELECT MIN(euphoria), MAX(euphoria), '
                    'COUNT(euphoria), MIN(instant), MAX(instant), '
                    'COUNT(instant) '
                'FROM id_map')
            emin, emax, ecnt, imin, imax, icnt = self.curs.fetchone()
            return {'euphoria': (emin, emax, ecnt),
                    'instant': (imin, imax, icnt)}

    def translate_ids(self, platform, ids, create=True):
        ret = dict.fromkeys(ids, Ellipsis)
        with self.lock:
            if platform == 'euphoria':
                for i in ids:
                    if i is None:
                        ret[i] = None
                        continue
                    self.curs.execute('SELECT euphoria, instant '
                        'FROM id_map WHERE euphoria = ?', (i,))
                    res = self.curs.fetchone()
                    if res is not None: ret[res[0]] = res[1]
            else:
                for i in ids:
                    if i is None:
                        ret[i] = None
                        continue
                    self.curs.execute('SELECT instant, euphoria '
                        'FROM id_map WHERE instant = ?', (i,))
                    res = self.curs.fetchone()
                    if res is not None: ret[res[0]] = res[1]
            if create:
                for k in ret:
                    if ret[k] is Ellipsis:
                        ret[k] = self.generate_id(platform, k, _commit=False)
                self.conn.commit()
            return ret

    def translate_id(self, platform, ident, create=True):
        res = self.translate_ids(platform, (ident,), create)
        return res[ident]

    def generate_id(self, platform, original, _commit=True):
        # platform is the platform of original.
        if platform == 'instant':
            raise RuntimeError('Cannot generate Euphoria ID-s')
        ts = euphoria_id_to_timestamp(original)
        with self.lock:
            # We iterate down from the highest sequence ID to avoid collisions
            # with the Instant backend.
            for seq in range(1023, -1, -1):
                candidate = timestamp_to_instant_id(ts, seq)
                try:
                    self.curs.execute('INSERT '
                        'INTO id_map(euphoria, instant) '
                        'VALUES (?, ?)', (original, candidate))
                except sqlite3.IntegrityError:
                    continue
                else:
                    break
            else:
                raise RuntimeError('Cannot generate translation for Euphoria '
                    'ID %r' % original)
            self._run_watchers(original, candidate)
            if _commit: self.conn.commit()
            return candidate

    def update_ids(self, platform, mapping):
        # platform is the platform of the keys of the mapping.
        if platform == 'euphoria':
            items = [(e, i) for e, i in mapping.items() if e is not None]
        else:
            items = [(e, i) for i, e in mapping.items() if i is not None]
        with self.lock:
            for euphoria, instant in items:
                self.curs.execute('INSERT OR REPLACE '
                    'INTO id_map(euphoria, instant) '
                    'VALUES (?, ?)', (euphoria, instant))
                self._run_watchers(euphoria, instant)
            self.conn.commit()

    def watch_ids(self, platform, idents, callback, create=True):
        def check(orig, translated):
            if orig is not None:
                ret[orig] = translated
                pending_keys.discard(orig)
            if not pending_keys:
                callback(ret)
        with self.lock:
            ret = self.translate_ids(platform, idents, create)
            pending_keys = set()
            for k, v in ret.items():
                if k is not None and v is None:
                    pending_keys.add(k)
                    self.watchers.setdefault((platform, k), []).append(
                        lambda translated, orig=k: check(orig, translated))
            check(None, None)

    def watch_id(self, platform, ident, callback):
        if ident is None: return callback(None)
        key = (platform, ident)
        with self.lock:
            if platform == 'euphoria':
                self.curs.execute('SELECT instant FROM id_map '
                    'WHERE euphoria = ?', (ident,))
            else:
                self.curs.execute('SELECT euphoria FROM id_map '
                    'WHERE instant = ?', (ident,))
            res = self.curs.fetchone()
            if res is not None and res[0] is not None: return callback(res[0])
            self.watchers.setdefault(key, []).append(callback)

    def _run_watchers(self, euphoria, instant):
        if euphoria is None or instant is None: return
        for w in self.watchers.pop(('euphoria', euphoria), ()):
            w(instant)
        for w in self.watchers.pop(('instant', instant), ()):
            w(euphoria)

class Nexus:
    def __init__(self, dbname=None):
        self.euphoria_users = {}
        self.instant_users = {}
        self.bots = {}
        self.euphoria_bot = None
        self.instant_bot = None
        self.messages = MessageStore(dbname)
        self.scheduler = instabot.EventScheduler()
        self.parent = None
        self.lock = threading.RLock()
        self.seq_lock = threading.RLock()
        self.bot_lock = threading.RLock()
        self.logger = logging.getLogger('nexus')
        self._last_sequence = 0

    def close(self):
        self.messages.close()

    def _sequence(self):
        with self.seq_lock:
            self._last_sequence += 1
            return 'nexus:%s' % self._last_sequence

    def _bot_ident(self, entry):
        if entry['platform'] == 'euphoria':
            return 'e/' + entry['euphoria_id']
        else:
            return 'i/' + entry['instant_id']

    def get_bot(self, entry, on_ready):
        identity = self._bot_ident(entry)
        with self.bot_lock:
            if identity not in self.bots:
                self.bots[identity] = self.make_bot(entry, on_ready)
            return self.bots[identity]

    def remove_bot(self, entry):
        identity = self._bot_ident(entry)
        with self.bot_lock:
            self.bots.pop(identity, None)

    def make_bot(self, entry, on_ready):
        return None

    def _get_user(self, query, create=False):
        euphoria_id = query.get('euphoria_id')
        instant_id = query.get('instant_id')
        ret = None
        if euphoria_id:
            if euphoria_id in self.euphoria_users:
                ret = self.euphoria_users[euphoria_id]
            elif create:
                ret = {'euphoria_id': euphoria_id}
                self.euphoria_users[euphoria_id] = ret
        if instant_id:
            if instant_id in self.instant_users:
                ret = self.instant_users[instant_id]
            elif create:
                ret = ret or {}
                ret['instant_id'] = instant_id
                self.instant_users[instant_id] = ret
        if create:
            ret.setdefault('ignore', False)
            ret.setdefault('delay', None)
            ret.setdefault('nick', None)
            ret.setdefault('actions', collections.deque())
            ret.setdefault('platform', None)
        return ret

    def add_users(self, users, new=False, _run=True):
        pending = []
        with self.lock:
            delay = self.scheduler.time() + SURROGATE_DELAY if new else None
            for u in users:
                entry = self._get_user(u, True)
                if u.get('platform'):
                    entry['platform'] = u['platform']
                if u.get('group'):
                    entry['group'] = u['group']
                if u.get('nick'):
                    entry['nick'] = u['nick']
                    entry['actions'].append(u)
                if delay is not None and (entry['delay'] is None or
                                          entry['delay'] < delay):
                    entry['delay'] = delay
                pending.append(entry)
            if _run:
                do_update = lambda: self._perform_actions(pending)
                if new:
                    self.scheduler.add_abs(delay, do_update)
                else:
                    self.scheduler.add_now(do_update)

    def remove_users(self, users):
        with self.lock:
            pending = []
            for u in users:
                ui, ue = None, None
                if 'euphoria_id' in u:
                    ue = self.euphoria_users.pop(u['euphoria_id'], None)
                    if ue:
                        ue['actions'].append({'remove': True})
                        pending.append(ue)
                if 'instant_id' in u:
                    ui = self.instant_users.pop(u['instant_id'], None)
                    if ui:
                        ui['actions'].append({'remove': True})
                        pending.append(ui)
            self.scheduler.add_now(lambda: self._perform_actions(pending))

    def remove_group(self, group):
        with self.lock:
            toremove = []
            toremove.extend(u for u in self.euphoria_users
                            if u.get('group') == group)
            toremove.extend(u for u in self.instant_users
                            if u.get('group') == group)
            self.remove_users(toremove)

    def ignore_users(self, users):
        with self.lock:
            self.add_users(users, _run=False)
            for u in users:
                entry = self._get_user(u)
                entry['ignore'] = True

    def handle_message(self, data):
        with self.lock:
            self.add_users(({k: v for k, v in data.items()
                             if k in ('platform', 'euphoria_id', 'instant_id',
                                      'nick', 'delay')},), _run=False)
            entry = self._get_user(data)
            data['text'] = self.translate_message_text(entry['platform'],
                                                       data['text'])
            entry['actions'].append(data)
            self.scheduler.add_now(lambda: self._perform_actions((entry,)))
            if not entry['ignore'] and data['text'].startswith('!'):
                tokens = basebot.parse_command(data['text'])
                reply = lambda text: self.send_bridge_message(
                    entry['platform'], data['msgid'], text)
                self.handle_command(tokens, reply)

    def handle_command(self, tokens, reply):
        normnick = basebot.normalize_nick
        if tokens[0] == '!help' and (len(tokens) == 1 or
                ping_matches(tokens[1], NICKNAME)):
            reply(HELP_TEMPLATE % {'euphoria': self.euphoria_bot.roomname,
                                   'instant': self.instant_bot.roomname})

    def translate_message_text(self, platform, text):
        def text_before(idx):
            return '' if idx == 0 else parsed[idx - 1][1]
        def text_after(idx):
            return '' if idx == len(parsed) - 1 else parsed[idx + 1][1]
        if platform == 'euphoria':
            # For Euphoria -> Instant, run the auto-linker and enclose
            # appropriate matches in Instant's sigils.
            parsed = list(autolinker.autolink(text))
            for idx, (tp, text) in enumerate(parsed):
                if tp == 'link':
                    if (re.match(r'<!?$', text_before(idx)) and
                            re.match(r'^>', text_after(idx))):
                        continue
                    m = INSTANT_URL_RE.search(text)
                    if not m or m.start() != 0 or m.end() != len(text):
                        continue
                    prefix = '<!' if IMAGE_URL.match(text) else '<'
                    parsed[idx] = (tp, text, prefix, '>')
            res = []
            for item in parsed:
                if len(item) > 2:
                    res.extend((item[2], item[1], item[3]))
                else:
                    res.append(item[1])
            return ''.join(res)
        else:
            # For Instant -> Euphoria, remove the sigils.
            res, idx, end = [], 0, len(text)
            while idx < end:
                m = INSTANT_URL_SEARCH.search(text, idx)
                if not m: break
                res.append(text[idx:m.start()])
                idx = m.end()
                # Only change things that will (hopefully) be recognized as
                # hyperlinks.
                if autolinker.is_link(m.group(1)):
                    res.append(m.group(1))
                else:
                    res.append(m.group())
            if idx != end: res.append(text[idx:])
            return ''.join(res)

    def add_mapping(self, data):
        self.messages.update_ids('euphoria',
                                 {data['euphoria']: data['instant']})

    def gather_ids(self, platform, ids):
        try:
            self.messages.translate_ids(platform, ids)
        except RuntimeError as exc:
            self.logger.warning('Could not gather up message ID-s: %r', exc)

    def message_bounds(self, platform):
        return self.messages.get_bounds()[platform]

    def request_messages(self, platform, before, after, maxlen, callback):
        def before_translated(result):
            # The "before" ID has been translated; schedule the actual
            # execution of the log query.
            self.scheduler.add_now(lambda: run_query(result))
        def run_query(translated):
            # Actually execute the log query.
            # See process_result() for the apparently lacking  "after"
            # parameter.
            self.euphoria_bot.query_logs(translated, None, maxlen,
                                         process_logs)
        def process_logs(logs):
            # Translate the ID-s of the log messages.
            ids = set(msg.id for msg in logs)
            ids.update(msg.parent for msg in logs)
            self.messages.watch_ids('euphoria', ids,
                lambda mapping: process_result(logs, mapping))
        def process_result(logs, mapping):
            # Translate the logs and pass them to the callback.
            result = []
            # HACK: Euphoria does not support querying "downwards" from a
            #       message; we partially emulate that by ignoring the "after"
            #       parameter up to here and cutting out the messages we
            #       accidentally pick up as a result.
            #       This suffices for the Instant frontend's needs in the
            #       hopefully common case that less than MAX_LOG_REQUEST
            #       messages were missed after a reconnect.
            for msg in logs:
                if after is not None and mapping[msg.id] < after: continue
                result.append({
                    'id': mapping[msg.id],
                    'parent': mapping[msg.parent],
                    'nick': msg.sender.name,
                    'text': self.translate_message_text('euphoria',
                                                        msg.content),
                    'timestamp': msg.time * 1000
                })
            callback(result)
        if platform != 'instant':
            raise RuntimeError('Cannot query messages from Instant for '
                'Euphoria')
        if maxlen is None or maxlen > MAX_LOG_REQUEST:
            maxlen = MAX_LOG_REQUEST
        self.messages.watch_id(platform, before, before_translated)

    def send_bridge_message(self, platform, parent, text):
        # The message is sent on both platforms; platform defines which
        # platform parent is on.
        def parent_resolved(other):
            if platform == 'euphoria':
                euphoria_parent, instant_parent = parent, other
            else:
                euphoria_parent, instant_parent = other, parent
            self.scheduler.add_now(
                lambda: do_send(euphoria_parent, instant_parent))
        def do_send(euphoria_parent, instant_parent):
            self.euphoria_bot.submit_post(euphoria_parent, text,
                self._sequence(),
                lambda p: self.scheduler.add_now(lambda: euphoria_cb(p)))
            self.instant_bot.submit_post(instant_parent, text,
                self._sequence(),
                lambda p: self.scheduler.add_now(lambda: instant_cb(p)))
        def euphoria_cb(packet):
            ids['euphoria'] = packet.data.id
            if ids['instant'] is not Ellipsis:
                self.add_mapping(ids)
        def instant_cb(packet):
            ids['instant'] = packet['data']['id']
            if ids['euphoria'] is not Ellipsis:
                self.add_mapping(ids)
        ids = {'euphoria': Ellipsis, 'instant': Ellipsis}
        self.messages.watch_id(platform, parent, parent_resolved)

    def _perform_actions(self, entries):
        def make_runner(e):
            # Has to be a closure because e is a loop variable.
            return lambda: self._perform_actions((e,))
        now = self.scheduler.time()
        for e in entries:
            if e['ignore']:
                e['actions'].clear()
                continue
            elif not e['actions']:
                continue
            elif e['delay'] is not None and e['delay'] > now:
                continue
            runner = make_runner(e)
            bot = self.get_bot(e, runner)
            if not bot.ready: continue
            while 1:
                try:
                    action = e['actions'].popleft()
                except IndexError:
                    break
                if 'nick' in action and action['nick'] != bot.nickname:
                    bot.set_nickname(action['nick'])
                if 'text' in action:
                    with self.messages.lock:
                        try:
                            tr_parent = self.messages.translate_id(
                                e['platform'], action['parent'])
                        except RuntimeError as exc:
                            self.logger.warning('Could not translate message '
                                'ID %s:%s: %r', e['platform'],
                                action['parent'], exc)
                            continue
                        self.messages.update_ids(e['platform'],
                            {action['msgid']: None})
                        if (action['parent'] is not None and
                                tr_parent is None):
                            self.messages.watch_id(e['platform'],
                                action['parent'], lambda result: runner)
                            break
                    bot.submit_post(tr_parent, action['text'],
                        e['platform'] + ':' + action['msgid'])
                if action.get('remove'):
                    bot.close()
                    self.remove_bot(e)

    def start(self):
        self.logger.info('Starting...')
        self.messages.init()
        res = self.messages.gc()
        if res == 1:
            self.logger.warning('Discarded %s incomplete mapping', res)
        elif res > 1:
            self.logger.warning('Discarded %s incomplete mappings', res)
        basebot.spawn_thread(self.scheduler.main)

    def shutdown(self):
        self.scheduler.shutdown()

    def join(self):
        self.scheduler.join()

def logger_name(platform, room, nick, botname):
    if room in (None, Ellipsis): room = '???'
    if nick in (None, Ellipsis): nick = '???' if botname is None else botname
    return '%s@%s/%s' % (nick, platform, room)

class EuphoriaBotManager(basebot.BotManager):
    def __init__(self, **config):
        basebot.BotManager.__init__(self, **config)
        self.nexus = config['nexus']

    def make_bot(self, roomname=Ellipsis, passcode=Ellipsis,
                 nickname=Ellipsis, logger=Ellipsis, **config):
        if logger is Ellipsis:
            botname = config.get('botname', self.botname)
            logger = logging.getLogger(logger_name('euphoria', roomname,
                                                   nickname, botname))
        return basebot.BotManager.make_bot(self, roomname, passcode, nickname,
                                           logger, **config)

class InstantBotManager(basebot.BotManager):
    def __init__(self, **config):
        basebot.BotManager.__init__(self, **config)
        self.nexus = config['nexus']

    def make_bot(self, roomname=Ellipsis, passcode=Ellipsis,
                 nickname=Ellipsis, logger=Ellipsis, **config):
        if passcode is not Ellipsis:
            raise TypeError('Instant bots do not have passcodes')
        if logger is Ellipsis:
            botname = config.get('botname', self.botname)
            logger = logging.getLogger(logger_name('instant', roomname,
                                                   nickname, botname))
        return basebot.BotManager.make_bot(self, roomname, Ellipsis,
                                           nickname, logger, **config)

def main():
    def make_bot(entry, on_ready):
        # Nexus gives us the data of the original connection, so we swap the
        # platform here.
        bot_counter[0] += 1
        if entry['platform'] == 'euphoria':
            bot = inst_mgr.make_bot(botname='surrogate#%s' % bot_counter[0],
                                    roomname=arguments.instant_room,
                                    on_ready=on_ready)
            inst_mgr.add_bot(bot)
        else:
            bot = euph_mgr.make_bot(botname='surrogate#%s' % bot_counter[0],
                                    roomname=arguments.euphoria_room,
                                    on_ready=on_ready)
            euph_mgr.add_bot(bot)
        bot.start()
        return bot
    p = argparse.ArgumentParser()
    p.add_argument('--loglevel', default='INFO',
                   help='Logging level to use')
    p.add_argument('--db', metavar='PATH',
                   help='Database path (default in-memory)')
    p.add_argument('--euphoria-room', metavar='ROOMNAME', default='test',
                   help='Euphoria room to bridge (default &test)')
    p.add_argument('--instant-room', metavar='ROOMNAME', default='test',
                   help='Instant room to bridge (default &test)')
    arguments = p.parse_args()
    loglevel = arguments.loglevel
    logging.basicConfig(format='[%(asctime)s %(name)s %(levelname)s] '
        '%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=loglevel)
    nexus = Nexus(arguments.db)
    nexus.make_bot = make_bot
    bot_counter = [0]
    euph_mgr = EuphoriaBotManager(botcls=EuphoriaSendBot,
        botname='EuphoriaBridge', nexus=nexus)
    inst_mgr = InstantBotManager(botcls=InstantSendBot,
        botname='InstantBridge', nexus=nexus)
    nexus.euphoria_bot = euph_mgr.make_bot(botcls=EuphoriaBridgeBot,
        botname='bridge', roomname=arguments.euphoria_room, nickname=NICKNAME)
    nexus.instant_bot = inst_mgr.make_bot(botcls=InstantBridgeBot,
        botname='bridge', roomname=arguments.instant_room, nickname=NICKNAME)
    euph_mgr.add_bot(nexus.euphoria_bot)
    inst_mgr.add_bot(nexus.instant_bot)
    euph_mgr.add_child(inst_mgr)
    euph_mgr.add_child(nexus)
    try:
        euph_mgr.main()
    except (KeyboardInterrupt, SystemExit) as exc:
        euph_mgr.shutdown()
        euph_mgr.join()
        if isinstance(exc, SystemExit): raise
    finally:
        nexus.close()

if __name__ == '__main__': main()
