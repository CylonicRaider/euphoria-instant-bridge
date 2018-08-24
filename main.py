#!/usr/bin/env python3
# -*- coding: ascii -*-

import os
import argparse
import logging

import basebot
import instabot

INSTANT_ROOM_TEMPLATE = os.environ.get('INSTANT_ROOM_TEMPLATE',
                                       'wss://instant.leet.nu/room/{}/ws')

class EuphoriaSendBot(basebot.HeimEndpoint):
    pass

class InstantSendBot(instabot.Bot):
    pass

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
    mgr = basebot.BotManager(botcls=EuphoriaSendBot,
                             botname='EuphoriaBridge')
    mgr.add_child(InstantBotManager(botcls=InstantSendBot,
                                    botname='InstantBridge'))
    try:
        mgr.main()
    except (KeyboardInterrupt, SystemExit) as exc:
        mgr.shutdown()
        mgr.join()
        if isinstance(exc, SystemExit): raise

if __name__ == '__main__': main()
