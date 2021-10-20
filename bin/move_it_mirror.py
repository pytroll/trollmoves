#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Move it Mirror."""

import logging.handlers
from threading import Lock, Timer
from urllib.parse import urlunparse
import argparse

from posttroll.message import Message
from posttroll.publisher import Publisher, get_own_ip

from trollmoves.move_it_base import MoveItBase, create_publisher
from trollmoves.client import Listener
from trollmoves.server import reload_config
from trollmoves.mirror import MirrorRequestManager, file_registry

LOGGER = logging.getLogger("move_it_mirror")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


class MoveItMirror(MoveItBase):
    """Mirror for move_it."""

    def __init__(self, cmd_args):
        """Set up the mirror."""
        publisher = create_publisher(cmd_args.port, "move_it_mirror")
        super(MoveItMirror, self).__init__(cmd_args, "mirror", publisher=publisher)
        self.cache_lock = Lock()

    def reload_cfg_file(self, filename):
        """Reload the config file."""
        reload_config(filename, self.chains, self.create_listener_notifier,
                      MirrorRequestManager, publisher=self.publisher)

    def signal_reload_cfg_file(self, *args):
        """Reload the config file when we get a signal."""
        del args
        self.reload_cfg_file(self.cmd_args.config_file)

    def create_listener_notifier(self, attrs, publisher):
        """Create a listener notifier."""
        request_address = attrs.get("request_address",
                                    get_own_ip()) + ":" + attrs["request_port"]

        delay = float(attrs.get('delay', 0))
        if delay > 0:
            def send(msg):
                """Delay the sending."""
                Timer(delay, publisher.send, [msg]).start()
        else:
            def send(msg):
                """Use the regular publisher to send."""
                publisher.send(msg)

        def publish_callback(msg, *args, **kwargs):
            """Forward an updated message."""
            del args
            del kwargs
            # save to file_cache
            with self.cache_lock:
                if msg.data['uid'] in file_registry:
                    file_registry[msg.data['uid']].append(msg)
                    return

            file_registry[msg.data['uid']] = [msg]
            # transform message
            new_msg = Message(msg.subject, msg.type, msg.data.copy())
            new_msg.data['request_address'] = request_address

            # send onwards
            LOGGER.debug('Sending %s', str(new_msg))
            send(str(new_msg))

        if "client_topic" not in attrs:
            attrs["client_topic"] = None
        listeners = Listeners(publish_callback, **attrs)

        return listeners, foo


def foo(*args, **kwargs):
    """Do not do anything."""
    pass


class Listeners(object):
    """Class for multiple listeners."""

    def __init__(self, callback, client_topic, providers, **attrs):
        """Set up the listeners."""
        self.listeners = []
        if client_topic is None:
            client_topic = []
        else:
            client_topic = [client_topic]

        for provider in providers.split():
            topic = client_topic
            if '/' in provider:
                parts = provider.split('/', 1)
                provider = parts[0]
                topic = ['/' + parts[1]]
                LOGGER.info("Using provider-specific topic %s for %s",
                            topic, provider)
            self.listeners.append(Listener(
                urlunparse(('tcp', provider, '', '', '', '')),
                topic,
                callback, **attrs))

    def start(self):
        """Start the listeners."""
        for listener in self.listeners:
            listener.start()

    def stop(self):
        """Stop the listeners."""
        for listener in self.listeners:
            listener.stop()


def parse_args():
    """Parse the command line arguments."""
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
    parser.add_argument("-v", "--verbose", default=False, action="store_true",
                        help="Toggle verbose logging")

    return parser.parse_args()


def main():
    """Start the mirroring."""
    cmd_args = parse_args()
    mirror = MoveItMirror(cmd_args)

    try:
        mirror.reload_cfg_file(cmd_args.config_file)
        mirror.run()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if mirror.running:
            mirror.chains_stop()


if __name__ == '__main__':
    main()
