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


import argparse
import logging
import os

from trollmoves.dispatcher import Dispatcher

LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"
logger = logging.getLogger(__name__)


def setup_logging(cmd_args):
    """Set up logging."""
    global logger
    logger = logging.getLogger('dispatcher')
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
