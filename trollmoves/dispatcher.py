#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019
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

Example config:

client1:
  host: ftp://ftp.client1.com
  connection_parameters:
    credential_file: .netrc
  filepattern: '{platform_name}_{start_time}.{format}'
  directory: /input_data/{sensor}
  dispatch:
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
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'
"""

# TODO:
# - include monitoring

import argparse
import logging
import signal
import os
from queue import Empty
from threading import Thread

import yaml
from six.moves.urllib.parse import urljoin

import inotify.adapters
from posttroll.listener import ListenerContainer
from trollsift import compose

from trollmoves.hooks import DummyHook
from trollmoves.utils import clean_url
from trollmoves.movers import move_it

logger = logging.getLogger(__name__)
LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"


class Notifier(Thread):
    """Class to handle file notifications."""

    def __init__(self, filename, event_types, callback):
        """Initialize the notifier."""
        self.filename = filename
        self.loop = True
        self.i = inotify.adapters.Inotify()
        self.i.add_watch(filename)
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
        self.notifier.close()
        self.notifier.join()

    def __del__(self, *args, **kwargs):
        """Delete the config handler."""
        self.close()


class DispatchConfig(YAMLConfig):
    """Class to handle dispatch configs."""

    def __init__(self, filename, callback):
        self.callback = callback
        super().__init__(filename)

    def read_config(self):
        super().read_config()
        self.callback(self.config)


class Dispatcher(Thread):
    """Class that dispatches files."""

    def __init__(self, config_file):
        super().__init__()
        self.config = None
        self.topics = None
        self.listener = None
        self.loop = True
        self.config_handler = DispatchConfig(config_file, self.update_config)
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def signal_shutdown(self, *args, **kwargs):
        self.close()

    def update_config(self, new_config):
        old_config = self.config
        topics = set()
        try:
            for _client, client_config in new_config.items():
                topics |= set(sum([item['topics'] for item in client_config['dispatch']], []))
            if self.topics != topics:
                if self.listener is not None:
                    # FIXME: make sure to get the last messages though
                    self.listener.close()
                self.config = new_config
                self.listener = ListenerContainer(topics)

        except KeyError as err:
            logger.warning('Invalid config for %s, keeping the old one running: %s', _client, str(err))
            self.config = old_config

    def run(self):
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
                    dispatch(msg.data['uri'], destinations)

    def get_destinations(self, msg):
        """Get the destinations for this message."""
        destinations = []
        for client, config in self.config.items():
            for item in config['dispatch']:
                # remove 'pytroll:/'
                for topic in item['topics']:
                    msg.subject[9:].startswith(topic)
                    break
                else:
                    continue
                if check_conditions(msg, item):
                    destinations.append(self.create_dest_url(msg, client, item))
        return destinations

    def create_dest_url(self, msg, client, item):
        """Create the destination URL and the connection parameters."""
        defaults = self.config[client]
        info_dict = dict()
        for key in ['host', 'directory', 'filepattern']:
            try:
                info_dict[key] = item[key]
            except KeyError:
                info_dict[key] = defaults[key]
        connection_parameters = item.get('connection_parameters',
                                         defaults.get('connection_parameters'))
        host = info_dict['host']
        path = os.path.join(info_dict['directory'], info_dict['filepattern'])
        path = compose(path, msg.data)
        return urljoin(host, path), connection_parameters

    def close(self):
        """Shutdown the dispatcher."""
        logger.info('Terminating dispatcher.')
        self.loop = False
        self.listener.close()
        self.config_handler.close()


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
            product: greensnow

    This would allow 'green_snow' and 'true_color' from VIIRS to pass the check,
    and 'green_snow' and 'overview' from AVHRR, with the exception of 'overview'
    from NOAA-15. 'true_color' from MODIS will not be dispatched.

    """
    # Fixme: except !
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


def dispatch(source, destinations, hook=None):
    """Dispatch source file to destinations.

    *hook* is a monitoring hook.
    """
    if hook is None:
        hook = DummyHook()
    any_error = False
    # check that file actually exists
    if not os.path.exists(source):
        message = "Source file for dispatching does not exist:{}".format(str(source))
        logger.error(message)
        hook.error(message)
        any_error = True
    # rename and send file with right protocol
    for url, params in destinations:
        try:
            move_it(source, url, params)
        except Exception as err:
            message = "Could not dispatch to {}: {}".format(str(clean_url(url)),
                                                            str(err))
            hook.error(message)
            any_error = True
    if not any_error:
        hook.ok("Dispatched all files.")
        logger.info("Dispatched all files.")


def setup_logging(cmd_args):
    global logger
    logger = logging.getLogger('move_it')
    logger.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh_ = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh_ = logging.StreamHandler()

    formatter = logging.Formatter(LOG_FORMAT)
    fh_.setFormatter(formatter)

    logger.addHandler(fh_)


def main():
    """Start and run the dispatcher."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    cmd_args = parser.parse_args()

    setup_logging(cmd_args)

    logger.info("Starting up.")

    try:
        dispatcher = Dispatcher(cmd_args.config_file)
        dispatcher.start()
        dispatcher.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    finally:
        dispatcher.close()


if __name__ == '__main__':
    main()
