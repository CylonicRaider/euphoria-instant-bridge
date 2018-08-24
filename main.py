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

class EuphoriaSendBot(basebot.HeimEndpoint):
    pass

class InstantSendBot(instabot.Bot):
    def __init__(self, url, nickname=Ellipsis, **kwds):
        instabot.Bot.__init__(self, url, nickname, **kwds)
        self.manager = kwds.get('manager')
        self.lock = threading.RLock()

    def on_close(self):
        instabot.Bot.on_close(self)
        # FIXME: Instabot does not expose the "ok" and "final" parameters.
        if self.manager: self.manager.handle_close(self, True, True)

class InstantBotManager(basebot.BotManager):
    def make_bot(self, roomname=Ellipsis, passcode=Ellipsis,
                 nickname=Ellipsis, logger=Ellipsis, **config):
        if passcode is not Ellipsis:
            raise TypeError('Instant bots do not have passcodes')
        if logger in (None, Ellipsis):
            logger = None
        else:
            raise TypeError('Instant sending bots do not have loggers')
        if roomname is not Ellipsis:
            config['url'] = INSTANT_ROOM_TEMPLATE.format(roomname)
            roomname = Ellipsis
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
    euph_mgr = basebot.BotManager(botcls=EuphoriaSendBot,
                                  botname='EuphoriaBridge')
    inst_mgr = InstantBotManager(botcls=InstantSendBot,
                                 botname='InstantBridge')
    euph_mgr.add_bot(euph_mgr.make_bot(roomname=arguments.euphoria_room,
        nickname=NICKNAME))
    inst_mgr.add_bot(inst_mgr.make_bot(roomname=arguments.instant_room,
        nickname=NICKNAME))
    euph_mgr.add_child(inst_mgr)
    try:
        euph_mgr.main()
    except (KeyboardInterrupt, SystemExit) as exc:
        euph_mgr.shutdown()
        euph_mgr.join()
        if isinstance(exc, SystemExit): raise

if __name__ == '__main__': main()
