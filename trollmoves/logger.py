#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Pytroll Developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""The log handling.
"""

import os
import logging
import logging.config
import logging.handlers
import yaml
import pyinotify

LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"

log_levels = {
    0: logging.WARN,
    1: logging.INFO,
    2: logging.DEBUG,
}


class LoggerSetup():
    """Setup logging."""

    def __init__(self, cmd_args, logger=None):
        """Init the logging setup class."""
        self._cmd_args = cmd_args
        self._file_handler = None
        self._logger = logger

    def setup_logging(self, chain_type=None):
        """Set up logging."""
        if self._cmd_args.log_config is not None:
            with open(self._cmd_args.log_config) as fd_:
                log_dict = yaml.safe_load(fd_.read())
                logging.config.dictConfig(log_dict)
            self._logger = logging.getLogger('')
        else:
            self._setup_default_logging(chain_type)

    def _setup_default_logging(self, chain_type):
        """Setup default logging without using a log-config file."""
        self._logger = logging.getLogger('')
        self._logger.setLevel(log_levels[self._cmd_args.verbosity])

        if self._cmd_args.log:
            self._file_handler = logging.handlers.TimedRotatingFileHandler(
                os.path.join(self._cmd_args.log),
                "midnight",
                backupCount=7)
        else:
            self._file_handler = logging.StreamHandler()

        formatter = logging.Formatter(LOG_FORMAT)
        self._file_handler.setFormatter(formatter)

        self._logger.addHandler(self._file_handler)
        self._set_loggername(chain_type)

    def _set_loggername(self, chain_type):
        if not chain_type:
            return

        logger_name = "move_it_server"
        if chain_type == "client":
            logger_name = "move_it_client"
        elif chain_type == "mirror":
            logger_name = "move_it_mirror"
        self._logger = logging.getLogger(logger_name)

    def get_logger(self):
        """Get the logger to use for logging."""
        return self._logger

    def init_pyinotify_logging(self):
        """Initialize the pyinotify handler."""
        pyinotify.log.handlers = [self._file_handler]
