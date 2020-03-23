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
"""Dispatcher.

The configuration file is watched with inotify. If needed, the reload of the
configuration file can be triggered with a `kill -10 <dispatcher pid>`.
"""

import argparse
import logging
import logging.config
import logging.handlers
import os
import sys

import yaml

from trollmoves.dispatcher import Dispatcher

LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"
logger = logging.getLogger('dispatcher')

log_levels = {
    0: logging.WARN,
    1: logging.INFO,
    2: logging.DEBUG,
}


def setup_logging(cmd_args):
    """Set up logging."""
    if cmd_args.log_config is not None:
        with open(cmd_args.log_config) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)
            return

    root = logging.getLogger('')
    root.setLevel(log_levels[cmd_args.verbosity])

    if cmd_args.log:
        fh_ = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh_ = logging.StreamHandler()

    formatter = logging.Formatter(LOG_FORMAT)
    fh_.setFormatter(formatter)

    root.addHandler(fh_)


def main():
    """Start and run the dispatcher."""
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
    cmd_args = parser.parse_args()
    setup_logging(cmd_args)
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
