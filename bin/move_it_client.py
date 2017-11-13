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
  destinations=/the/directory/you/want/stuff/in /another/directory/you/want/stuff/in
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

I Like To Move It Move It
I Like To Move It Move It
I Like To Move It Move It
Ya Like To (MOVE IT!)

"""

# TODO: implement ping and server selection
import _strptime
import logging
import logging.handlers
import os
import time
from datetime import datetime

import pyinotify

from posttroll.publisher import NoisyPublisher
from trollmoves.client import StatCollector, reload_config, terminate
from trollmoves.server import EventHandler

NP = None
PUB = None

running = True

LOGGER = logging.getLogger("move_it_client")


chains = {}


def main():
    while running:
        time.sleep(1)

if __name__ == '__main__':
    import argparse
    import signal

    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-s", "--stats",
                        help="Save stats to this file")
    cmd_args = parser.parse_args()

    log_format = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
    LOGGER = logging.getLogger('')
    LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh = logging.StreamHandler()

    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)

    LOGGER.addHandler(fh)
    LOGGER = logging.getLogger('move_it_client')

    pyinotify.log.handlers = [fh]

    LOGGER.info("Starting up.")

    NP = NoisyPublisher("move_it_client")
    PUB = NP.start()

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)
    watchman = pyinotify.WatchManager()

    def reload_cfg_file(filename, *args, **kwargs):
        reload_config(filename, chains, *args, pub_instance=PUB, **kwargs)

    notifier = pyinotify.ThreadedNotifier(watchman, EventHandler(reload_cfg_file, cmd_filename=cmd_args.config_file))
    watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def chains_stop(*args):
        global running
        running = False
        notifier.stop()
        NP.stop()
        terminate(chains)

    signal.signal(signal.SIGTERM, chains_stop)

    def signal_reload_cfg_file(*args):
        reload_config(cmd_args.config_file, chains, pub_instance=PUB)

    signal.signal(signal.SIGHUP, signal_reload_cfg_file)

    notifier.start()

    try:
        if cmd_args.stats:
            stat = StatCollector(cmd_args.stats)
            reload_cfg_file(cmd_args.config_file, callback=stat.collect)
        else:
            reload_cfg_file(cmd_args.config_file)
        main()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    except:
        LOGGER.exception('wow')
    finally:
        if running:
            chains_stop()
