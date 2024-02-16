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

    # Optional direct subscriptions
    # subscribe_addresses:
    #   - tcp://127.0.0.1:40000
    # Nameserver to connect to. Optional. Defaults to localhost
    # nameserver: 127.0.0.1
    # Subscribe to specific services. Optional. Default: connect to all services
    # subscribe_services:
    #   - service_name_1
    #   - service_name_2
    target1:
      host: ftp://ftp.target1.com
      connection_parameters:
        connection_uptime: 60
      filepattern: '{platform_name}_{start_time}.{format}'
      directory: /input_data/{sensor}
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


Some general configuration can be provided at the main level:

    - `nameserver` (optional): Address of a nameserver to connect to.  Default: 'localhost'.
    - `addresses` (optional): List of TCP connections to listen for messages.
    - `services` (optional): List of service names to subscribe to.  Default: connect to all services.

The rest of the configuration file is divided in sections for each host receiving data.

Each host section have to contain the following information:

    - `host`: The host to transfer to. If it's empty (`""`), the files will be
      dispatched to localhost using regular copy operations.
    - `filepattern`: The file pattern to use. The fields that can be used are
      the ones that are available in the message metadata. Moreover, the file creation time can be accessed through
      the `file_creation_time` item, and is a datetime object.
      See the trollsift documentation for details on the field formats.
    - `directory`: The directory to dispatch the data to on the receiving host.
      Can also make use of the fields from the message metadata.
    - `dispatch_configs`: the dispatch_configs section describing what files to dispatch. See below.
    - `connection_parameters` (optional): Some extra connection parameters to
      pass to the moving function. See the `trollmoves.movers` module documentation.
    - `aliases` (optional): A dictionary of metadata items to change for the
      final filename. These are not taken into account for checking the conditions.

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
import contextlib
import logging
import os
import signal
import socket
from datetime import datetime
from queue import Empty
from urllib.parse import urlsplit, urlunsplit, urlparse

import yaml
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher, create_publisher_from_dict_config
from trollsift import compose

from trollmoves.movers import move_it
from trollmoves.utils import (clean_url, is_file_local)

logger = logging.getLogger(__name__)


def read_config(filename):
    """Read the configuration from file."""
    logger.info('Reading config from %s', filename)
    with open(filename, 'r') as fd:
        return yaml.safe_load(fd.read())


def _create_publisher(publish_port, publish_nameservers):
    if publish_port is not None:
        publisher = NoisyPublisher("dispatcher", port=publish_port,
                                   nameservers=publish_nameservers)
        publisher.start()
        return publisher


class Dispatcher:
    """Class that dispatches files."""

    # Idea for future refactoring: the publish arguments should really be provided in the configuration file, see
    # https://github.com/pytroll/trollmoves/issues/159

    def __init__(self, config_file, publish_port=None, publish_nameservers=None, messages=None):
        """Initialize dispatcher class.

        Arguments:
            messages: an iterable of messages to use in the dispatcher. This short-circuits the posttroll reception.
                      Useful for testing.
        """
        self.config = read_config(config_file)

        self.messages = messages

        if publish_port is not None:
            self.publisher = PublisherReporter(self.config, publish_port, publish_nameservers)

        else:
            self.publisher = None

        self.host = socket.gethostname()

        signal.signal(signal.SIGTERM, self.close)

    def close(self, *args, **kwargs):
        """Shut down the dispatcher."""
        logger.info('Terminating dispatcher.')
        with contextlib.suppress(AttributeError):
            self.messages.stop()
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")

    def run(self):
        """Run dispatcher."""
        if self.messages is None:
            self.messages = PosttrollMessageIterator(self.config)

        for msg in self.messages:
            if msg.type == 'file':
                self.dispatch_from_message(msg)
            elif msg.type == 'dataset':
                # Loop through files in the dataset and publish a message for each one of them
                file_messages = self._get_file_messages_from_dataset_message(msg)
                for fmsg in file_messages:
                    self.dispatch_from_message(fmsg)
            else:
                continue

    def dispatch_from_message(self, msg):
        """Dispatch from message."""
        destinations = self.get_destinations(msg)
        if destinations:
            # Check if the url are on another host:
            url = urlparse(msg.data['uri'])
            _check_file_locality(url, self.host)
            success = dispatch(url.path, destinations)
            if self.publisher:
                self.publisher.publish(msg, destinations, success)

    def get_destinations(self, msg):
        """Get the destinations for this message."""
        destinations = []
        for client, config in self.config.items():
            for dispatch_config in config['dispatch_configs']:
                destination = self._get_destination(dispatch_config, msg, client)
                if destination is None:
                    continue
                destinations.append(destination)
        return destinations

    def _get_destination(self, dispatch_config, msg, client):
        destination = None
        if _has_correct_topic(dispatch_config, msg):
            if check_conditions(msg, dispatch_config):
                destination = self.create_dest_url(msg, client, dispatch_config)
        return destination

    def create_dest_url(self, msg, client, conf):
        """Create the destination URL and the connection parameters."""
        config = self.config[client].copy()
        _verify_filepattern(config, msg)
        config.update(conf)
        connection_parameters = config.get('connection_parameters')

        host = config['host']

        metadata = _get_metadata_with_aliases(msg, config)

        path = compose(
            os.path.join(config['directory'],
                         config['filepattern']),
            metadata)
        parts = urlsplit(host)
        host_path = urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))
        return host_path, connection_parameters, client

    def _get_file_messages_from_dataset_message(self, msg):
        """From a dataset type message create individual messages for each file in the dataset."""
        basic_msg_data = msg.data.copy()
        dataset_part = basic_msg_data.pop('dataset')
        file_msgs = []
        for item in dataset_part:
            content = {}
            content.update(basic_msg_data)
            content['uid'] = item['uid']
            content['uri'] = item['uri']
            file_msgs.append(Message(msg.subject, "file", data=content))

        return file_msgs


class PublisherReporter:
    """This class uses a posttroll publisher to report the results of the dispatching.

    This is the first of possibly many reporting classes, eg for monitoring or generating reports daily.
    """

    # Idea for future refactoring: This could be made flexible enough to be used in other parts of trollmoves, eg in
    # move_it_client to report moved files. The main problem is the to pass the right configuration/topic for the
    # messages to be correct. See https://github.com/pytroll/trollmoves/issues/160

    def __init__(self, config, publish_port, publish_nameservers):
        """Set up the reporter."""
        self.config = config

        pub_settings = {
            "name": "dispatcher",
            "port": publish_port,
            "nameservers": publish_nameservers
        }

        self._pub_starter = create_publisher_from_dict_config(pub_settings)
        self.publisher = self._pub_starter.start()

    def publish(self, msg, destinations, success):
        """Publish a message.

        The URI is replaced with the URI on the target server.
        """
        for url, _, client in destinations:
            if not success[client]:
                continue
            try:
                msg = self._get_new_message(msg, url, client)
            except ValueError as err:
                logger.error(str(err))
                continue
            logger.debug('Publishing %s', str(msg))
            self.publisher.send(str(msg))

    def _get_new_message(self, msg, url, client):
        info = self._get_message_info(msg, url)
        topic = self._get_topic(client, info)
        if topic is None:
            return None
        return Message(topic, 'file', info)

    def _get_message_info(self, msg, url):
        info = msg.data.copy()
        info["uri"] = urlsplit(url).path
        return info

    def _get_topic(self, client, info):
        try:
            topic = self.config[client]["publish_topic"]
        except KeyError:
            raise ValueError(f"Publish topic not configured for '{client}'")
        return compose(topic, info)

    def stop(self):
        """Stop the reporter."""
        self._pub_starter.stop()


class PosttrollMessageIterator:
    """Posttroll message iterator."""

    def __init__(self, config):
        """Set up the iterator."""
        self.config = config
        self.running = True

    def __iter__(self):
        """Iterate over messages from a listener container."""
        with posttroll_listener(self.config) as listener:
            while self.running:
                try:
                    yield listener.output_queue.get(timeout=0.05)
                except Empty:
                    continue

    def stop(self):
        """Stop the iterator."""
        self.running = False


@contextlib.contextmanager
def posttroll_listener(new_config):
    """Create a posttroll listener."""
    subscriber_config = new_config.pop("posttroll_subscriber", {})
    addresses = subscriber_config.pop('subscribe_addresses', None)
    nameserver = subscriber_config.pop('nameserver', 'localhost')
    services = subscriber_config.pop('subscribe_services', '')

    topics = set()
    for _client, client_config in new_config.items():
        topics |= set(sum([item['topics'] for item in client_config['dispatch_configs']], []))

    listener = ListenerContainer(topics=topics,
                                 addresses=addresses,
                                 nameserver=nameserver,
                                 services=services)
    yield listener
    listener.stop()


def _check_file_locality(url, host):
    if not is_file_local(url):
        # This warning may appear even if the file path does exist on both servers (a central file server
        # reachable from both). But in those cases one should probably not use an url scheme but just a
        # file path:
        raise NotImplementedError(("uri is pointing to a file path on another server! "
                                   "Host=<%s> uri netloc=<%s>", host, url.netloc))


def _has_correct_topic(dispatch_config, msg):
    for topic in dispatch_config['topics']:
        if msg.subject.startswith(topic):
            return True
    return False


def _verify_filepattern(defaults, msg):
    if 'filepattern' not in defaults:
        source_filename = os.path.basename(urlsplit(msg.data['uri']).path)
        defaults['filepattern'] = source_filename


def _get_metadata_with_aliases(msg, defaults):
    metadata = msg.data.copy()
    metadata["file_creation_time"] = get_uri_creation_time(msg)
    for key, aliases in defaults.get('aliases', {}).items():
        if isinstance(aliases, dict):
            aliases = [aliases]

        for alias in aliases:
            new_key = alias.pop("_alias_name", key)
            if key in msg.data:
                metadata[new_key] = alias.get(msg.data[key], msg.data[key])
    return metadata


def get_uri_creation_time(msg):
    """Get the creation time of the file pointed to by the uri."""
    return datetime.fromtimestamp(os.path.getctime(msg.data.get("uri")))


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
