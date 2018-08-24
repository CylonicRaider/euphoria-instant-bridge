#!/usr/bin/env python3
# -*- coding: ascii -*-

import os
import argparse

import basebot
import instabot

INSTANT_ROOM_TEMPLATE = os.environ.get('INSTANT_ROOM_TEMPLATE',
                                       'wss://instant.leet.nu/room/{}/ws')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--euphoria-room', required=True, metavar='ROOMNAME',
                   help='Euphoria room to bridge')
    p.add_argument('--instant-room', required=True, metavar='ROOMNAME',
                   help='Instant room to bridge')
    arguments = p.parse_args()

if __name__ == '__main__': main()
