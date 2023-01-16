# Copyright (c) 2022
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
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
"""Logging utilities."""
import argparse
import logging
import logging.config
import os
import pathlib
import warnings
from contextlib import suppress

import yaml


def add_logging_options_to_parser(parser, legacy=False):
    """Add logging options to parser."""
    parser.add_argument("-c", "--log-config",
                        help="Log config file to use instead of the standard logging.",
                        type=pathlib.Path)
    if legacy:
        parser.add_argument("-l", "--log",
                            help="The file to log to. stdout otherwise.",
                            type=pathlib.Path,
                            action=DeprecationWarningAction)
        parser.add_argument("-v", "--verbose", default=False, action=DeprecationWarningAction, const=True, nargs=0,
                            help="Toggle verbose logging")


class DeprecationWarningAction(argparse.Action):
    """Default action with deprecation warning."""

    def __call__(self, parser, namespace, values, option_string=None):
        """Call the action."""
        warnings.warn(f"{option_string} is pending deprecation, please use --log-config instead.", RuntimeWarning)
        setattr(namespace, self.dest, values)


def setup_logging(name, cmd_args=None):
    """Set up the logging."""
    with suppress(AttributeError):
        with cmd_args.log_config.open() as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)
            return logging.getLogger(name)
    with suppress(AttributeError, TypeError):
        setup_legacy_logger(cmd_args)
        return logging.getLogger(name)

    setup_default_logger()
    return logging.getLogger(name)


def setup_legacy_logger(cmd_args):
    """Set up the legacy logger."""
    log_file = cmd_args.log
    log_level = logging.DEBUG
    log_dict = {'version': 1,
                'handlers': {'time_handler': {'class': 'logging.handlers.TimedRotatingFileHandler',
                                              'filename': os.fspath(log_file),
                                              'when': 'midnight',
                                              'backupCount': 7,
                                              'level': log_level}},
                'loggers': {'simple_example': {'level': log_level,
                                               'handlers': ['time_handler'],
                                               'propagate': False}},
                'root': {'level': log_level, 'handlers': ['time_handler']}}
    logging.config.dictConfig(log_dict)


def setup_default_logger():
    """Set up the default logger."""
    root = logging.getLogger('')
    handler = logging.StreamHandler()
    root.addHandler(handler)
