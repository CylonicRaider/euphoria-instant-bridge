#!/usr/bin/env python3
# -*- coding: ascii -*-

import os
import collections
import threading
import argparse
import logging

import basebot
import instabot

INSTANT_ROOM_TEMPLATE = os.environ.get('INSTANT_ROOM_TEMPLATE',
                                       'wss://instant.leet.nu/room/{}/ws')

NICKNAME = 'bridge'

# We shoehorn instabot Bot-s into the interface expected by basebot in order
# to leverage the latter's more powerful tools.
class InstantBotWrapper(instabot.Bot):
    def __init__(self, url, nickname=Ellipsis, **kwds):
        instabot.Bot.__init__(self, url, nickname, **kwds)
        self.manager = kwds.get('manager')
        self.logger = kwds.get('logger', logging.getLogger())
        self.lock = threading.RLock()
        self._sent_nick = Ellipsis

    def connect(self):
        self.logger.info('Connecting to %s...', self.url)
        return instabot.Bot.connect(self)

    def on_open(self):
        instabot.Bot.on_open(self)
        self.logger.info('Connected')

    def on_close(self):
        instabot.Bot.on_close(self)
        self.logger.info('Closing')
        # FIXME: Instabot does not expose the "ok" and "final" parameters.
        if self.manager: self.manager.handle_close(self, True, True)

    def send_nick(self, peer=None):
        if self.nickname != self._sent_nick:
            self.logger.info('Setting nickname: %r', self.nickname)
            self._sent_nick = self.nickname
        return instabot.Bot.send_nick(self, peer)

    def set_nickname(self, nick=Ellipsis):
        if nick is not Ellipsis: self.nickname = nick
        self.send_nick()

class EuphoriaBridgeBot(basebot.Bot):
    def __init__(self, *args, **kwds):
        basebot.Bot.__init__(self, *args, **kwds)
        self.log_users = True

    def handle_any(self, packet):
        basebot.Bot.handle_any(self, packet)
        add_users, remove_users = None, None
        # Switch on various packet types.
        if packet.type == 'who-reply':
            add_users = packet.data
        elif packet.type == 'hello-event':
            self.manager.nexus.ignore_users(({
                'platform': 'euphoria',
                'euphoria_id': packet.data['session'].session_id
            },))
        elif packet.type == 'snapshot-event':
            add_users = packet.data['listing']
        elif packet.type == 'network-event':
            # TODO: support this
            pass
        elif packet.type == 'nick-event':
            self.manager.nexus.add_users(({
                'platform': 'euphoria',
                'euphoria_id': packet.data['session_id'],
                'nick': packet.data['to']
            },))
        elif packet.type == 'join-event':
            add_users = (packet.data,)
        elif packet.type == 'part-event':
            remove_users = (packet.data,)
        elif packet.type == 'send-event':
            self.manager.nexus.handle_message({
                'platform': 'euphoria',
                'euphoria_id': packet.data.sender.session_id,
                'nick': packet.data.sender.name,
                'text': packet.data.content
            })
        # Apply user additions/deletions.
        if add_users:
            self.manager.nexus.add_users(
                [{'platform': 'euphoria', 'euphoria_id': entry.session_id,
                  'nick': entry.name} for entry in add_users])
        if remove_users:
            self.manager.nexus.remove_users([{'euphoria_id': entry.session_id}
                                             for entry in remove_users])

class InstantBridgeBot(InstantBotWrapper):
    def on_open(self):
        InstantBotWrapper.on_open(self)
        self.send_broadcast({'type': 'who'})

    def handle_identity(self, content, rawmsg):
        InstantBotWrapper.handle_identity(self, content, rawmsg)
        self.manager.nexus.ignore_users(({
            'instant_id': self.identity['id']
        },))

    def handle_joined(self, content, rawmsg):
        InstantBotWrapper.handle_joined(self, content, rawmsg)
        self.manager.nexus.add_users(({
            'platform': 'instant',
            'instant_id': content['data']['id']
        },))

    def handle_left(self, content, rawmsg):
        InstantBotWrapper.handle_left(self, content, rawmsg)
        self.manager.nexus.remove_users(({
            'instant_id': content['data']['id']
        },))

    def on_client_message(self, data, content, rawmsg):
        InstantBotWrapper.on_client_message(self, data, content, rawmsg)
        if data.get('type') == 'nick':
            self.manager.nexus.add_users(({
                'platform': 'instant',
                'instant_id': content['from'],
                'nick': data.get('nick')
            },))
        elif data.get('type') == 'post':
            self.manager.nexus.handle_message({
                'platform': 'instant',
                'instant_id': content['from'],
                'nick': data.get('nick'),
                'text': data.get('text')
            })

class EuphoriaSendBot(basebot.HeimEndpoint):
    def __init__(self, roomname=None, **config):
        config.setdefault('roomname', roomname)
        basebot.HeimEndpoint.__init__(self, **config)
        self.ready = False
        self.on_ready = config.get('on_ready')

    def handle_login(self):
        basebot.HeimEndpoint.handle_login(self)
        if not self.ready:
            self.ready = True
            if self.on_ready: self.on_ready()

class InstantSendBot(InstantBotWrapper):
    def __init__(self, url, nickname=None, **kwds):
        InstantBotWrapper.__init__(self, url, nickname, **kwds)
        self.ready = False
        self.on_ready = kwds.get('on_ready')

    def handle_identity(self, content, rawmsg):
        InstantBotWrapper.handle_identity(self, content, rawmsg)
        if not self.ready:
            self.ready = True
            if self.on_ready: self.on_ready()

class Nexus:
    def __init__(self):
        self.euphoria_users = {}
        self.instant_users = {}
        self.bots = {}
        self.scheduler = instabot.EventScheduler()
        self.lock = threading.RLock()
        self.bot_lock = threading.RLock()
        self.logger = logging.getLogger('nexus')

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
            ret.setdefault('nick', None)
            ret.setdefault('actions', collections.deque())
            ret.setdefault('platform', None)
        return ret

    def add_users(self, users, _run=True):
        pending = []
        with self.lock:
            for u in users:
                entry = self._get_user(u, True)
                if u.get('platform'):
                    entry['platform'] = u['platform']
                if u.get('nick'):
                    entry['nick'] = u['nick']
                    entry['actions'].append(u)
                pending.append(entry)
            if _run:
                self.scheduler.add_now(lambda: self._perform_actions(pending))

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

    def ignore_users(self, users):
        with self.lock:
            self.add_users(users, False)
            for u in users:
                entry = self._get_user(u)
                entry['ignore'] = True

    def handle_message(self, data):
        with self.lock:
            self.add_users((data,), False)
            entry = self._get_user(data)
            entry['actions'].append(data)
            self.scheduler.add_now(lambda: self._perform_actions((entry,)))

    def _perform_actions(self, entries):
        def make_runner(e):
            # Has to be a closure because e is a loop variable.
            return lambda: self._perform_actions((e,))
        for e in entries:
            if e['ignore']:
                continue
            bot = self.get_bot(e, make_runner(e))
            if not bot.ready: continue
            while 1:
                try:
                    action = e['actions'].popleft()
                except IndexError:
                    break
                if 'nick' in action and action['nick'] != bot.nickname:
                    bot.set_nickname(action['nick'])
                if 'text' in action:
                    pass # NYI
                if action.get('remove'):
                    bot.close()
                    self.remove_bot(e)

    def start(self):
        # TODO: Make Nexus a proper BotManager child.
        basebot.spawn_thread(self.scheduler.main)

def logger_name(platform, room, nick):
    if room in (None, Ellipsis): room = '???'
    if nick in (None, Ellipsis): nick = '???'
    return '%s@%s/%s' % (nick, platform, room)

class EuphoriaBotManager(basebot.BotManager):
    def __init__(self, **config):
        basebot.BotManager.__init__(self, **config)
        self.nexus = config['nexus']

    def make_bot(self, roomname=Ellipsis, passcode=Ellipsis,
                 nickname=Ellipsis, logger=Ellipsis, **config):
        if logger is Ellipsis:
            logger = logging.getLogger(logger_name('euphoria', roomname,
                                                   nickname))
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
        if roomname is not Ellipsis:
            config['url'] = INSTANT_ROOM_TEMPLATE.format(roomname)
        if logger is Ellipsis:
            logger = logging.getLogger(logger_name('instant', roomname,
                                                   nickname))
        return basebot.BotManager.make_bot(self, Ellipsis, Ellipsis,
                                           nickname, logger, **config)

def main():
    def make_bot(entry, on_ready):
        if entry['platform'] == 'euphoria':
            bot = euph_mgr.make_bot(roomname=arguments.euphoria_room,
                                    on_ready=on_ready)
            euph_mgr.add_bot(bot)
        else:
            bot = inst_mgr.make_bot(roomname=arguments.instant_room,
                                    on_ready=on_ready)
            inst_mgr.add_bot(bot)
        bot.start()
        return bot
    p = argparse.ArgumentParser()
    p.add_argument('--loglevel', default='INFO',
                   help='Logging level to use')
    p.add_argument('--euphoria-room', metavar='ROOMNAME', default='test',
                   help='Euphoria room to bridge (default &test)')
    p.add_argument('--instant-room', metavar='ROOMNAME', default='test',
                   help='Instant room to bridge (default &test)')
    arguments = p.parse_args()
    loglevel = arguments.loglevel
    logging.basicConfig(format='[%(asctime)s %(name)s %(levelname)s] '
        '%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=loglevel)
    nexus = Nexus()
    nexus.make_bot = make_bot
    euph_mgr = EuphoriaBotManager(botcls=EuphoriaSendBot,
        botname='EuphoriaBridge', nexus=nexus)
    inst_mgr = InstantBotManager(botcls=InstantSendBot,
        botname='InstantBridge', nexus=nexus)
    euph_mgr.add_bot(euph_mgr.make_bot(botcls=EuphoriaBridgeBot,
        roomname=arguments.euphoria_room, nickname=NICKNAME))
    inst_mgr.add_bot(inst_mgr.make_bot(botcls=InstantBridgeBot,
        roomname=arguments.instant_room, nickname=NICKNAME))
    euph_mgr.add_child(inst_mgr)
    try:
        nexus.start()
        euph_mgr.main()
    except (KeyboardInterrupt, SystemExit) as exc:
        euph_mgr.shutdown()
        euph_mgr.join()
        if isinstance(exc, SystemExit): raise

if __name__ == '__main__': main()
