#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2020
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

"""Dispatcher dispatches(!) files to some destination.

It listens to posttroll messages to find out what to dispatch.


Format of the configuration file
================================

.. highlight:: yaml

Example config::

    target1:
      host: ftp://ftp.target1.com
      connection_parameters:
        connection_uptime: 60
      filepattern: '{platform_name}_{start_time}.{format}'
      directory: /input_data/{sensor}
      # Optional direct subscriptions
      # subscribe_addresses:
      #   - tcp://127.0.0.1:40000
      # Nameserver to connect to. Optional. Defaults to localhost
      # nameserver: 127.0.0.1
      # Subscribe to specific services. Optional. Default: connect to all services
      # subscribe_services:
      #   - service_name_1
      #   - service_name_2
      # Message topics for published messages. Required if command-line option
      #   "-p"/"--publish-port" is used.  The topic can be composed using
      #   metadata from incoming message
      # publish_topic: "/new/topic/{platform_name}"
      aliases:
        product:
          natural_color: dnc
          overview: ovw
      dispatch_configs:
        - topics:
            - /level2/viirs
            - /level2/avhrr
          conditions:
            # key matches metadata items or provides default
            - product: [green_snow, true_color]
              sensor: viirs
            - product: [green_snow, overview]
              sensor: avhrr
              # special section "except" for negating
              except:
                platform_name: NOAA-15
        - topics:
            - /level3/cloudtype
          directory: /input/cloud_products
          conditions:
            - area: omerc_bb
              daylight: '<30'
              coverage: '>50'


The configuration file is divided in sections for each host receiving data.

Each host section have to contain the following information:

    - `host`: The host to tranfer to. If it's empty (`""`), the files will be
      dispatched to localhost.
    - `filepattern`: The file pattern to use. The fields that can be used are
      the ones that are available in the message metadata.
      See the trollsift documentation for details on the field formats.
    - `directory`: The directory to dispatch the data to on the receiving host.
      Can also make use of the fields from the message metadata.
    - `dispatch_configs`: the dispatch_configs section describing what files to dispatch. See below.
    - `connection_parameters` (optional): Some extra connection parameters to
      pass to the moving function. See the `trollmoves.movers` module documentation.
    - `aliases` (optional): A dictionary of metadata items to change for the
      final filename. These are not taken into account for checking the conditions.
    - `nameserver` (optional): Address of a nameserver to connect to.  Default: 'localhost'.
    - `addresses` (optional): List of TCP connections to listen for messages.
    - `services` (optional): List of service names to subscribe to.  Default: connect to all services.

Note that the `host`, `filepattern`, and `directory` items can be overridden in
the dispatch_configs section.

The dispatch_configs section contains a list of dispatch items. Each dispatch item have
to contain the following information:

  - `topics`: the posttroll topics to listen to.
  - `conditions` (optional): when specified, this will be use to filter out the
    files to dispatch. More info below.
  - `host`, `filepattern`, `directory`: same as in the the base host section.

The conditions are to be formatted in the following manner: they are to contain
a list of conditions set to match. The conditions set will be OR'ed. Each
conditions set contains conditions that will be AND'ed. For example::

           conditions:
            # key matches metadata items or provides default
            - product: [green_snow, true_color]
              sensor: viirs
            - product: [green_snow, overview]
              sensor: avhrr

will match every product `green_snow` or `true_color` from the sensor `viirs` or
`green_snow` or `overview` from sensor `avhrr`. The items (`product`, `sensor`,
etc) have correspond to fields in the message metadata.


Notice the multiple choices are possible using a list (eg
`[green_snow, true_color]`).
For exceptions, you can use the `except` keyword as in the following example::

            - product: [green_snow, overview]
              sensor: avhrr
              # special section "except" for negating
              except:
                platform_name: NOAA-15

This will match all `green_snow` and `overview` products from sensor `avhrr`,
except the ones from platform `NOAA-15`.

Also, the conditions can be matched again numerical fields from the message
metadata::

          conditions:
            - area: omerc_bb
              daylight: '<30'
              coverage: '>50'

The comparison operators that can be used are the ones that can be used in
python: `==`, `!=`, `<`, `>`, `<=`, `>=`.
"""

import logging
import os
import signal
from queue import Empty
from threading import Thread

import yaml

import inotify.adapters
from inotify.constants import IN_MODIFY, IN_CLOSE_WRITE, IN_CREATE, IN_MOVED_TO
from six.moves.urllib.parse import urlsplit, urlunsplit, urlparse
import socket
from posttroll.listener import ListenerContainer
from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollmoves.movers import move_it
from trollmoves.utils import (clean_url, is_file_local)
from trollsift import compose

logger = logging.getLogger(__name__)

INOTIFY_MASK = IN_MODIFY | IN_CLOSE_WRITE | IN_CREATE | IN_MOVED_TO


class Notifier(Thread):
    """Class to handle file notifications."""

    def __init__(self, filename, event_types, callback):
        """Initialize the notifier."""
        self.filename = filename
        self.loop = True
        self.i = inotify.adapters.Inotify()
        self.i.add_watch(filename, mask=INOTIFY_MASK)
        self.event_types = set(event_types)
        self.callback = callback
        super().__init__()

    def run(self):
        """Run the notifier."""
        for event in self.i.event_gen():
            if event is None:
                if not self.loop:
                    logger.info('Terminating watch on %s', self.filename)
                    return
                else:
                    continue
            (_, type_names, path, filename) = event
            if self.event_types.intersection(set(type_names)):
                self.callback()

    def close(self):
        """Close the notifier."""
        self.loop = False


class YAMLConfig():
    """Class to hold and watch for configuration changes."""

    def __init__(self, filename):
        """Initialize the config handler."""
        self.err = None
        self.filename = filename
        self.config = None
        self.read_config()
        self.notifier = Notifier(filename,
                                 ['IN_CLOSE_WRITE', 'IN_MOVED_TO', 'IN_CREATE'],
                                 self.read_config)
        self.notifier.start()
        signal.signal(signal.SIGUSR1, self.signal_reread)

    def signal_reread(self, *args, **kwargs):
        """Read the config when a signal is received."""
        self.read_config()

    def read_config(self):
        """Trigger a reread of the config file."""
        logger.info('Reading config from %s', self.filename)
        with open(self.filename, 'r') as fd:
            self.config = yaml.safe_load(fd.read())

    def close(self):
        """Close the config handler."""
        try:
            self.notifier.close()
            self.notifier.join()
        except AttributeError as err:
            self.err = err

    def __del__(self, *args, **kwargs):
        """Delete the config handler."""
        self.close()


class DispatchConfig(YAMLConfig):
    """Class to handle dispatch configs."""

    def __init__(self, filename, callback):
        """Initialize dispatch configuration class."""
        self.callback = callback
        super().__init__(filename)

    def read_config(self):
        """Read configuration file."""
        super().read_config()
        self.callback(self.config)


class Dispatcher(Thread):
    """Class that dispatches files."""

    def __init__(self, config_file, publish_port=None,
                 publish_nameservers=None):
        """Initialize dispatcher class."""
        super().__init__()
        self.config = None
        self.topics = None
        self.listener = None
        self.publisher = None
        self.host = socket.gethostname()

        if publish_port is not None:
            self.publisher = NoisyPublisher(
                "dispatcher", port=publish_port,
                nameservers=publish_nameservers)
            self.publisher.start()
        self.loop = True
        self.config_handler = DispatchConfig(config_file, self.update_config)
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown dispatcher."""
        self.close()

    def update_config(self, new_config):
        """Update configuration and reload listeners."""
        old_config = self.config
        topics = set()
        try:
            for _client, client_config in new_config.items():
                topics |= set(sum([item['topics'] for item in client_config['dispatch_configs']], []))
            if self.topics != topics:
                if self.listener is not None:
                    # FIXME: make sure to get the last messages though
                    self.listener.stop()
                self.config = new_config
                addresses = client_config.get('subscribe_addresses', None)
                nameserver = client_config.get('nameserver', 'localhost')
                services = client_config.get('subscribe_services', '')
                self.listener = ListenerContainer(topics=topics,
                                                  addresses=addresses,
                                                  nameserver=nameserver,
                                                  services=services)
                self.topics = topics

        except KeyError as err:
            logger.warning('Invalid config for %s, keeping the old one running: %s', _client, str(err))
            self.config = old_config

    def run(self):
        """Run dispatcher."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
            except Empty:
                continue
            else:
                if msg.type != 'file':
                    continue
                destinations = self.get_destinations(msg)
                if destinations:
                    # Check if the url are on another host:
                    url = urlparse(msg.data['uri'])
                    if not is_file_local(url):
                        # This warning may appear even if the file path
                        # does exist on both servers (a central file server
                        # reachable from both). But in those cases one
                        # should probably not use an url scheme but just a
                        # file path:
                        raise NotImplementedError(("uri is pointing to a file path on another server! "
                                                   "Host=<%s> uri netloc=<%s>", self.host, url.netloc))
                    success = dispatch(url.path, destinations)
                    if self.publisher:
                        self._publish(msg, destinations, success)

    def _publish(self, msg, destinations, success):
        """Publish a message.

        The URI is replaced with the URI on the target server.

        """
        for url, params, client in destinations:
            if not success[client]:
                continue
            del params
            info = msg.data.copy()
            info["uri"] = urlsplit(url).path
            topic = self.config[client].get("publish_topic")
            if topic is None:
                logger.error("Publish topic not configured for '%s'",
                             client)
                continue
            topic = compose(topic, info)
            msg = Message(topic, 'file', info)
            logger.debug('Publishing %s', str(msg))
            self.publisher.send(str(msg))

    def get_destinations(self, msg):
        """Get the destinations for this message."""
        destinations = []
        for client, config in self.config.items():
            for disp_config in config['dispatch_configs']:
                for topic in disp_config['topics']:
                    if msg.subject.startswith(topic):
                        break
                else:
                    continue
                if check_conditions(msg, disp_config):
                    destinations.append(
                        self.create_dest_url(msg, client, disp_config))
        return destinations

    def create_dest_url(self, msg, client, disp_config):
        """Create the destination URL and the connection parameters."""
        defaults = self.config[client].copy()
        if 'filepattern' not in defaults:
            source_filename = os.path.basename(urlsplit(msg.data['uri']).path)
            defaults['filepattern'] = source_filename
        info_dict = dict()
        for key in ['host', 'directory', 'filepattern']:
            try:
                info_dict[key] = disp_config[key]
            except KeyError:
                info_dict[key] = defaults[key]
        connection_parameters = disp_config.get(
            'connection_parameters',
            defaults.get('connection_parameters'))
        host = info_dict['host']
        path = os.path.join(info_dict['directory'], info_dict['filepattern'])
        mda = msg.data.copy()

        for key, aliases in defaults.get('aliases', {}).items():
            if isinstance(aliases, dict):
                aliases = [aliases]

            for alias in aliases:
                new_key = alias.pop("_alias_name", key)
                if key in msg.data:
                    mda[new_key] = alias.get(msg.data[key], msg.data[key])

        path = compose(path, mda)
        parts = urlsplit(host)
        host_path = urlunsplit((parts.scheme, parts.netloc, path,
                                parts.query, parts.fragment))
        return host_path, connection_parameters, client

    def close(self):
        """Shutdown the dispatcher."""
        logger.info('Terminating dispatcher.')
        self.loop = False
        try:
            self.listener.stop()
        except Exception:
            logger.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")
        try:
            self.config_handler.close()
        except Exception:
            logger.exception("Couldn't stop config handler.")


def check_conditions(msg, item):
    """Check if a message matches the config item's conditions.

    The conditions sets are OR'ed, so this returns True if one condition set
    matches. Within a condition set, all the conditions are AND'ed.
    Example::

        - product: [green_snow, true_color]
          sensor: viirs
        - product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15
            product: green_snow

    This would allow 'green_snow' and 'true_color' from VIIRS to pass the check,
    and 'green_snow' and 'overview' from AVHRR, with the exception of
    'green_snow' from NOAA-15. 'true_color' from MODIS will not be dispatched.

    """
    # Fixme: except !
    if 'conditions' not in item:
        return True
    for condition_set in item['conditions']:
        if _check_condition_set(msg, condition_set):
            return True
    return False


def _check_condition_set(msg, condition_set, negate=False):
    """Check a condition set against the message data.

    The conditions are AND'ed, they must all match for the condition set to be
    valid. Setting `negate` to True makes this function return the negated
    result of the check.
    If a key from the condition set is missing in the message metadata,
    this function returns False, even if `negate` is True.
    For example::

          product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15

    """
    for key, value in condition_set.items():
        try:
            if key == 'except':
                if not _check_condition_set(msg, value, negate=True):
                    return negate
            elif not _check_condition(msg, key, value):
                return negate
        except KeyError as err:
            logger.warning('Missing metadata info to check condition: %s', err)
            return False
    return not negate


def _check_condition(msg, key, value):
    """Check one condition.

    If the value is a list, check that the message element is within the list.
    Example::

        product: [green_snow, overview]

    """
    if isinstance(value, list):
        if msg.data[key] not in value:
            return False
    else:
        if isinstance(value, str) and value[0] in ['<', '>', '=', '!']:
            return eval(str(float(msg.data[key])) + value)
        elif msg.data[key] != value:
            return False
    return True


def dispatch(source, destinations):
    """Dispatch source file to destinations."""
    any_error = False
    # check that file actually exists
    if not os.path.exists(source):
        message = "Source file for dispatching does not exist:{}".format(str(source))
        logger.error(message)
        any_error = True
    success = {}
    # rename and send file with right protocol
    for url, params, client in destinations:
        # Multiple destinations for one client isn't implemented
        if client in success:
            raise NotImplementedError("Only one destination allowed per client")
        try:
            logger.debug("Dispatching %s to %s", source, str(clean_url(url)))
            move_it(source, url, params)
            success[client] = True
        except Exception as err:
            message = "Could not dispatch to {}: {}".format(str(clean_url(url)),
                                                            str(err))
            logger.error(message)
            any_error = True
            success[client] = False
    if not any_error:
        logger.info("Dispatched all files.")

    return success
