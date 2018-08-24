#!/usr/bin/env python3
# -*- coding: ascii -*-

import os
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
                'euphoria_id': packet.data['session'].session_id
            },))
        elif packet.type == 'snapshot-event':
            add_users = packet.data['listing']
        elif packet.type == 'network-event':
            # TODO: support this
            pass
        elif packet.type == 'nick-event':
            self.manager.nexus.add_users(({
                'euphoria_id': packet.data['session_id'],
                'nick': packet.data['to']
            },))
        elif packet.type == 'join-event':
            add_users = (packet.data,)
        elif packet.type == 'part-event':
            remove_users = (packet.data,)
        elif packet.type == 'send-event':
            self.manager.nexus.handle_message({
                'euphoria_id': packet.data.sender.session_id,
                'nick': packet.data.sender.name,
                'text': packet.data.content
            })
        # Apply user additions/deletions.
        if add_users:
            self.manager.nexus.add_users(
                [{'euphoria_id': entry.session_id, 'nick': entry.name}
                 for entry in add_users])
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
            'instant_id': content['data']['id'],
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
                'instant_id': content['from'],
                'nick': data.get('nick')
            },))
        elif data.get('type') == 'post':
            self.manager.nexus.handle_message({
                'instant_id': content['from'],
                'nick': data.get('nick'),
                'text': data.get('text')
            })

class EuphoriaSendBot(basebot.HeimEndpoint):
    pass

class InstantSendBot(InstantBotWrapper):
    pass

class Nexus:
    def __init__(self):
        self.scheduler = instabot.EventScheduler()
        self.lock = threading.RLock()
        self.logger = logging.getLogger('nexus')

    def __enter__(self):
        return self.lock.__enter__()

    def __exit__(self, *args):
        return self.lock.__exit__(*args)

    def add_users(self, users):
        self.logger.info('Adding users: %r', users)

    def remove_users(self, users):
        self.logger.info('Removing users: %r', users)

    def ignore_users(self, users):
        self.logger.info('Ignoring users: %r', users)

    def handle_message(self, data):
        self.logger.info('Processing message: %r', data)

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
        euph_mgr.main()
    except (KeyboardInterrupt, SystemExit) as exc:
        euph_mgr.shutdown()
        euph_mgr.join()
        if isinstance(exc, SystemExit): raise

if __name__ == '__main__': main()
