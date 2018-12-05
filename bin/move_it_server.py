#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012, 2013, 2014, 2015, 2016
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

This scripts needs pyinotify and argparse which are available on pypi, or via
pip/easy_install (on redhat systems, install the packages python-inotify.noarch
and python-argparse.noarch).  Other than this, the script doesn't need any
installation, and can be run as is. If you wish though, you can install it to
your standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  origin=/tmp/H-000-{series:_<6s}-{platform_name:_<12s}-{channel:_<9s}-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-{compression:1s}_
  publisher_port=9011
  compression=xrit
  prog=/home/a001673/usr/src/PublicDecompWT/Image/Linux_32bits/xRITDecompress
  topic=/1b/hrit/zds
  info=sensor=seviri;sublon=0
  request_port=9092
  working_directory=/tmp/unpacked

* 'origin' is the directory and pattern of files to watch for. For a description
  of the pattern format, see the trollsift documentation:
  http://trollsift.readthedocs.org/en/latest/index.html

* 'working_directory' is telling where to unpack the files before they are put
  in their final destination. This can come in handy in case the file has to be
  transfered by ftp and cannot be unpacked in the origin directory. The default
  for this parameter is the '/tmp' directory.

* Available compressions are 'xrit' and 'bzip'.

* The prog parameter is used for the 'xrit' unpacking function to know which
  external program to call for unpack xRIT files.

  .. note:: The 'xrit' unpacking function is dependent on a program that can
    unpack xRIT files. Such a program is available from the `Eumetsat.int
    <http://www.eumetsat.int/Home/Main/DataAccess/SupportSoftwareTools/index.htm?l=en>`_
    website.

* 'topic', 'publish_port', and 'info' define the messaging behaviour using posttroll. 'info' being a ';' separated
  list of 'key=value' items that has to be added to the message info.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout) w
ith the -l or --log option::

  move_it_server --log /path/to/mylogfile.log myconfig.ini

"""

import logging
import logging.handlers
import os
import time
import argparse
import signal

import pyinotify

from posttroll.publisher import Publisher
from trollmoves.server import EventHandler, reload_config, terminate

LOGGER = logging.getLogger("move_it_server")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


class MoveItServer(object):

    def __init__(self, cmd_args):
        self.cmd_args = cmd_args
        self.running = False
        self.notifier = None
        self.watchman = None
        self.chains = {}
        setup_logging(cmd_args)
        LOGGER.info("Starting up.")
        LOGGER.info("Starting publisher on port %s.", str(cmd_args.port))
        self.pub = Publisher("tcp://*:" + str(cmd_args.port), "move_it_server")
        self.setup_watchers(cmd_args)

    def run(self):
        signal.signal(signal.SIGTERM, self.chains_stop)
        signal.signal(signal.SIGHUP, self.signal_reload_cfg_file)
        self.notifier.start()
        self.running = True
        while self.running:
            time.sleep(1)
            self.pub.heartbeat(30)

    def reload_cfg_file(self, filename):
        return reload_config(filename, self.chains, publisher=self.pub)

    def setup_watchers(self, cmd_args):
        mask = (pyinotify.IN_CLOSE_WRITE |
                pyinotify.IN_MOVED_TO |
                pyinotify.IN_CREATE)
        self.watchman = pyinotify.WatchManager()

        event_handler = EventHandler(self.reload_cfg_file,
                                     cmd_filename=self.cmd_args.config_file)
        self.notifier = pyinotify.ThreadedNotifier(self.watchman, event_handler)
        self.watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def chains_stop(self, *args):
        del args
        self.running = False
        self.notifier.stop()
        terminate(self.chains, self.pub)

    def signal_reload_cfg_file(self, *args):
        del args
        self.reload_cfg_file(self.cmd_args.config_file)


def setup_logging(cmd_args):
    """Setup logging"""
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
    LOGGER = logging.getLogger('move_it_server')
    pyinotify.log.handlers = [fh_]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-p", "--port",
                        help="The port to publish on. 9010 is the default",
                        default=9010)
    parser.add_argument("-v", "--verbose", default=False, action='store_true')

    return parser.parse_args()


def main():
    """Main()"""
    cmd_args = parse_args()
    server = MoveItServer(cmd_args)

    try:
        server.reload_cfg_file(cmd_args.config_file)
        server.run()
    except KeyboardInterrupt:
        server.logger.debug("Stopping Trollmoves server")
    finally:
        if server.running:
            server.chains_stop()


if __name__ == '__main__':
    main()
