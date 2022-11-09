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
import argparse
import os
import logging

from urllib.parse import urlparse, urlunparse
from threading import Lock, Timer

from posttroll.message import Message
from posttroll.publisher import get_own_ip

from trollmoves.client import Listener
from trollmoves.client import request_push
from trollmoves.logging import add_logging_options_to_parser
from trollmoves.server import RequestManager, Deleter, AbstractMoveItServer
from trollmoves.move_it_base import create_publisher


LOGGER = logging.getLogger(__name__)
file_registry = {}
cache_lock = Lock()


class MirrorListener(Listener):
    """Listener for Trollmoves Mirror.

    Subclass the Client Listener to replace how the messages are processed.
    """

    def _process_message(self, msg):
        if _file_already_published(msg):
            return
        file_registry[msg.data['uid']] = [msg]
        request_address = self.ckwargs.get("request_address", get_own_ip()) + ":" + self.ckwargs["request_port"]
        delay = float(self.ckwargs.get("delay", 0))
        publisher = self.ckwargs["publisher"]
        mirror_message = _get_mirror_message(msg, request_address)
        if delay:
            Timer(delay, publisher.send, [mirror_message]).start()
        else:
            publish_mirror_message(mirror_message, publisher.send)


def _file_already_published(msg):
    with cache_lock:
        if msg.data['uid'] in file_registry:
            file_registry[msg.data['uid']].append(msg)
            return True
    return False


def _get_mirror_message(msg, request_address):
    mirror_message = Message(msg.subject, msg.type, msg.data.copy())
    mirror_message.data['request_address'] = request_address
    return mirror_message


def publish_mirror_message(mirror_message, publisher_send):
    """Forward an updated message."""
    LOGGER.debug('Sending %s', str(mirror_message))
    publisher_send(str(mirror_message))


class MoveItMirror(AbstractMoveItServer):
    """Mirror for move_it."""

    def __init__(self, cmd_args):
        """Set up the mirror."""
        self.name = "move_it_mirror"
        publisher = create_publisher(cmd_args.port, self.name)
        super().__init__(cmd_args, publisher=publisher)
        self.request_manager = MirrorRequestManager

    def reload_cfg_file(self, filename):
        """Reload the config file."""
        self.reload_config(filename, self.create_listener_notifier, disable_backlog=True)

    def signal_reload_cfg_file(self, *args):
        """Reload the config file when we get a signal."""
        del args
        self.reload_cfg_file(self.cmd_args.config_file)

    def create_listener_notifier(self, attrs, publisher):
        """Create a listener notifier."""
        if "publisher" not in attrs:
            attrs["publisher"] = publisher
        listeners = Listeners(attrs.pop("client_topic"), attrs.pop("providers"), **attrs)

        return listeners, noop


def noop(*args, **kwargs):
    """Do not do anything."""
    pass


class Listeners(object):
    """Class for multiple listeners."""

    def __init__(self, client_topic, providers, **attrs):
        """Set up the listeners."""
        self.listeners = []
        if client_topic is None:
            client_topic = []
        else:
            client_topic = [client_topic]

        for provider in providers.split():
            topic = _get_topic(client_topic, provider)
            self.listeners.append(MirrorListener(
                urlunparse(('tcp', provider, '', '', '', '')),
                topic,
                **attrs))

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
            request_push(source_message, **self._attrs)
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


def parse_args(args=None):
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-p",
                        "--port",
                        help="The port to publish on. 9010 is the default",
                        default=9010)
    add_logging_options_to_parser(parser, legacy=True)

    return parser.parse_args(args)
