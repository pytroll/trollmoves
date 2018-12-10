#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2013, 2014, 2015, 2016

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

"""
Moving and unpacking files
==========================

This program is comprised of two parts: this script and the configuration file.

The usage of this script is quite straightforward, just call it with the name
of the configuration file as argument.

The configuration file is comprised of sections describing the chain of
moving/unpacking.

Installation
------------

This scripts needs pyinotify, argparse, posttroll and trollsift which are available on pypi, or via pip/easy_install
(on redhat systems, install the packages python-inotify.noarch and python-argparse.noarch).  Other than this,
the script doesn't need any installation, and can be run as is. If you wish though, you can install it to your
standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  providers=tellicast_server:9090
  destination=/the/directory/you/want/stuff/in /another/directory/you/want/stuff/in
  login=username:greatPassword
  topic=/1b/hrit/zds
  publish_port=0

* 'provider' is the address of the server receiving the data you want.

* 'destinations' is the list of places to put the unpacked data in.

* 'topic' gives the topic to listen to on the provider side.

* 'publish_port' defines on which port to publish incomming files. 0 means random port.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout)
with the -l or --log option::

  move_it_client --log /path/to/mylogfile.log myconfig.ini

"""

# TODO: implement ping and server selection
import logging
import logging.handlers
import os
import time
import argparse
import signal

import pyinotify

from posttroll.publisher import NoisyPublisher
from trollmoves.client import StatCollector, reload_config, terminate
from trollmoves.server import EventHandler

LOGGER = logging.getLogger("move_it_client")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


class MoveItClient(object):

    def __init__(self, cmd_args):
        self.cmd_args = cmd_args
        self.running = False
        self.notifier = None
        self.watchman = None
        self.chains = {}
        setup_logging(cmd_args)
        LOGGER.info("Starting up.")
        self._np = NoisyPublisher("move_it_client")
        self.pub = self._np.start()
        self.setup_watchers(cmd_args)

    def reload_cfg_file(self, filename, *args, **kwargs):
        reload_config(filename, self.chains, *args, pub_instance=self.pub,
                      **kwargs)

    def signal_reload_cfg_file(self, *args):
        del args
        reload_config(self.cmd_args.config_file, self.chains,
                      pub_instance=self.pub)

    def chains_stop(self, *args):
        del args
        self.running = False
        self.notifier.stop()
        self._np.stop()
        terminate(self.chains)

    def setup_watchers(self, cmd_args):
        mask = (pyinotify.IN_CLOSE_WRITE |
                pyinotify.IN_MOVED_TO |
                pyinotify.IN_CREATE)
        self.watchman = pyinotify.WatchManager()
        event_handler = EventHandler(self.reload_cfg_file,
                                     cmd_filename=cmd_args.config_file)
        self.notifier = pyinotify.ThreadedNotifier(self.watchman, event_handler)
        self.watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def run(self):
        signal.signal(signal.SIGTERM, self.chains_stop)
        signal.signal(signal.SIGHUP, self.signal_reload_cfg_file)
        self.notifier.start()
        self.running = True
        while self.running:
            time.sleep(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-s", "--stats",
                        help="Save stats to this file")
    parser.add_argument("-v", "--verbose", default=False, action="store_true",
                        help="Toggle verbose logging")
    return parser.parse_args()


def setup_logging(cmd_args):
    """Setup logging."""
    global LOGGER
    LOGGER = logging.getLogger('')
    if cmd_args.verbose:
        LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh_ = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh_ = logging.StreamHandler()

    formatter = logging.Formatter(LOG_FORMAT)
    fh_.setFormatter(formatter)

    LOGGER.addHandler(fh_)
    LOGGER = logging.getLogger('move_it_client')

    pyinotify.log.handlers = [fh_]


def main():
    """Main()"""
    cmd_args = parse_args()
    client = MoveItClient(cmd_args)

    try:
        if cmd_args.stats:
            stat = StatCollector(cmd_args.stats)
            client.reload_cfg_file(cmd_args.config_file, callback=stat.collect)
        else:
            client.reload_cfg_file(cmd_args.config_file)
        client.run()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if client.running:
            client.chains_stop()


if __name__ == '__main__':
    main()

