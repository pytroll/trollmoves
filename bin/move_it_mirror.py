#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import _strptime
import logging
import os
import time
from datetime import datetime
from threading import Lock, Timer
from urlparse import urlparse

import pyinotify

from posttroll.message import Message
from posttroll.publisher import Publisher, get_own_ip
from trollmoves.client import Listener, request_push
from trollmoves.server import (Deleter, EventHandler, RequestManager,
                               reload_config, terminate)

LOGGER = logging.getLogger("move_it_mirror")

chains = {}

PUB = None

running = True

file_registery = {}
cache_lock = Lock()


def main():
    while running:
        time.sleep(1)
        PUB.heartbeat(30)


def foo(*args, **kwargs):
    pass


class Listeners(object):

    def __init__(self, callback, client_topic, providers, **attrs):
        self.listeners = []
        for provider in providers.split():
            self.listeners.append(Listener('tcp://' + provider, [client_topic],
                                           callback, **attrs))

    def start(self):
        for listener in self.listeners:
            listener.start()

    def stop(self):
        for listener in self.listeners:
            listener.stop()


def create_listener_notifier(attrs, publisher):

    request_address = attrs.get("request_address",
                                get_own_ip()) + ":" + attrs["request_port"]

    delay = float(attrs.get('delay', 0))
    if delay > 0:
        def send(msg):
            Timer(delay, publisher.send, [msg]).start()
    else:
        def send(msg):
            publisher.send(msg)

    def publish_callback(msg, *args, **kwargs):
        # save to file_cache
        with cache_lock:
            if msg.data['uid'] in file_registery:
                file_registery[msg.data['uid']].append(msg)
                return

            file_registery[msg.data['uid']] = [msg]
        # transform message
        new_msg = Message(msg.subject, msg.type, msg.data.copy())
        new_msg.data['request_address'] = request_address

        # send onwards
        LOGGER.debug('Sending %s', str(new_msg))
        send(str(new_msg))

    listeners = Listeners(publish_callback, **attrs)

    return listeners, foo


class MirrorRequestManager(RequestManager):

    def __init__(self, *args, **kwargs):
        RequestManager.__init__(self, *args, **kwargs)
        self._deleter = MirrorDeleter()

    def push(self, message):
        new_uri = None
        for source_message in file_registery.get(message.data['uid'], []):
            request_push(source_message, publisher=None, **self._attrs)
            destination = urlparse(self._attrs['destination']).path
            new_uri = os.path.join(destination, message.data['uid'])
            if os.path.exists(new_uri):
                break
        if new_uri is None:
            raise KeyError('No source message found for %s',
                           str(message.data['uid']))
        message.data['uri'] = new_uri
        return RequestManager.push(self, message)


class MirrorDeleter(Deleter):

    @staticmethod
    def delete(filename):
        Deleter.delete(filename)
        with cache_lock:
            file_registery.pop(os.path.basename(filename), None)


if __name__ == '__main__':
    import argparse
    import signal

    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l",
                        "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-p",
                        "--port",
                        help="The port to publish on. 9010 is the default",
                        default=9010)
    cmd_args = parser.parse_args()

    log_format = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
    LOGGER = logging.getLogger('')
    LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight", backupCount=7)
    else:
        fh = logging.StreamHandler()

    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)

    LOGGER.addHandler(fh)

    LOGGER = logging.getLogger('move_it_server')
    LOGGER.setLevel(logging.DEBUG)

    pyinotify.log.handlers = [fh]

    LOGGER.info("Starting up.")

    LOGGER.info("Starting publisher on port %s.", str(cmd_args.port))

    PUB = Publisher("tcp://*:" + str(cmd_args.port), "move_it_server")

    mask = (pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)
    watchman = pyinotify.WatchManager()

    def local_reload_config(filename):
        return reload_config(filename, chains, create_listener_notifier,
                             MirrorRequestManager, PUB)

    notifier = pyinotify.ThreadedNotifier(
        watchman,
        EventHandler(local_reload_config,
                     cmd_filename=cmd_args.config_file))
    watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def chains_stop(*args):
        global running
        running = False
        notifier.stop()
        terminate(chains, PUB)

    signal.signal(signal.SIGTERM, chains_stop)

    def reload_cfg_file(*args):
        reload_config(cmd_args.config_file, chains, create_listener_notifier,
                      MirrorRequestManager, PUB)

    signal.signal(signal.SIGHUP, reload_cfg_file)

    notifier.start()

    try:
        reload_config(cmd_args.config_file, chains, create_listener_notifier,
                      MirrorRequestManager, PUB)
        main()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    except:
        LOGGER.exception('Interrupting on error')
    finally:
        if running:
            chains_stop()
