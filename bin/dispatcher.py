#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019, 2022
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
"""Dispatcher.

The configuration file is watched with inotify. If needed, the reload of the
configuration file can be triggered with a `kill -10 <dispatcher pid>`.
"""

import argparse
import logging
import os
import sys

from trollmoves.logger import LoggerSetup
from trollmoves.dispatcher import Dispatcher

LOG = logging.getLogger('dispatcher')


def parse_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-c", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")
    parser.add_argument(
        "-p", "--publish-port", type=int, dest="pub_port", nargs='?',
        const=0, default=None,
        help="Publish messages for dispatched files on this port. "
        "Default: no publishing.")
    parser.add_argument("-n", "--publish-nameserver", nargs='*',
                        dest="pub_nameservers",
                        help="Nameserver for publisher to connect to")
    return parser.parse_args()


def main():
    """Start and run the dispatcher."""
    cmd_args = parse_args()
    logger = LoggerSetup(cmd_args)
    logger.setup_logging()
    LOG = logger.get_logger()
    logger.info("Starting up.")

    try:
        dispatcher = Dispatcher(cmd_args.config_file,
                                publish_port=cmd_args.pub_port,
                                publish_nameservers=cmd_args.pub_nameservers)
    except Exception as err:
        logger.error('Dispatcher crashed: %s', str(err))
        sys.exit(1)
    try:
        dispatcher.start()
        dispatcher.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    finally:
        dispatcher.close()


if __name__ == '__main__':
    main()
