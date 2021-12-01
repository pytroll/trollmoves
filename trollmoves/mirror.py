#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 Trollmoves developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
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

"""All you need for mirroring."""

import os
import logging

from urllib.parse import urlparse, urlunparse
from threading import Lock, Timer

from posttroll.message import Message
from posttroll.publisher import get_own_ip

from trollmoves.client import Listener
from trollmoves.client import request_push
from trollmoves.server import RequestManager, Deleter
from trollmoves.move_it_base import MoveItBase, create_publisher
from trollmoves.server import reload_config


LOGGER = logging.getLogger(__name__)
file_registry = {}


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

        return listeners, noop


def noop(*args, **kwargs):
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
            topic = _get_topic(client_topic, provider)
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


def _get_topic(client_topic, provider):
    topic = client_topic
    if '/' in provider:
        parts = provider.split('/', 1)
        provider = parts[0]
        topic = ['/' + parts[1]]
        LOGGER.info("Using provider-specific topic %s for %s",
                    topic, provider)
    return topic


class MirrorRequestManager(RequestManager):
    """Handle requests as a mirror."""

    def __init__(self, port, attrs):
        """Set up this mirror request manager."""
        RequestManager.__init__(self, port, attrs)
        self._deleter = MirrorDeleter(attrs)

    def push(self, message):
        """Push the file."""
        new_uri = None
        for source_message in file_registry.get(message.data['uid'], []):
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
    """Deleter for mirroring."""

    def __init__(self, attrs=None):
        """Set up the deleter."""
        super().__init__(attrs)

    @staticmethod
    def delete(filename):
        """Delete the file."""
        Deleter.delete(filename)
        # Pop is atomic, so we don't need a lock.
        file_registry.pop(os.path.basename(filename), None)
