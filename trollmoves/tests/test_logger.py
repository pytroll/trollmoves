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

"""Test the logging setup
"""

import pytest
from trollmoves.logger import LoggerSetup
from dataclasses import dataclass
import logging

TEST_LOGGER = logging.getLogger("mytest")


TEST_LOG_YAML_CONTENT = """
version: 1
disable_existing_loggers: false
formatters:
  pytroll:
    format: '[%(asctime)s %(levelname)-8s %(name)s] %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    formatter: pytroll
    stream: ext://sys.stdout
  timed_log_rotation:
    class: logging.handlers.TimedRotatingFileHandler
    level: DEBUG
    formatter: pytroll
    filename: name_of_the_process
    when: h
  monitor:
    (): pytroll_monitor.op5_logger.AsyncOP5Handler
    auth: [username, passwd]
    service: check_name_of_the_process
    server: https://monitor-xxx.somewhere.yy/api/command/PROCESS_SERVICE_CHECK_RESULT
    host: myhost
loggers:
  posttroll:
    level: ERROR
    propagate: false
    handlers: [console, monitor, timed_log_rotation]
root:
  level: DEBUG
  handlers: [console, monitor, timed_log_rotation]
"""


@dataclass
class FakeArgparseOutput:
    config_file: str
    log: str
    log_config: str
    verbosity: bool


@pytest.fixture
def fake_cmd_args_no_logfile():
    """Return a fake argparse Namespace."""
    args_namespace = FakeArgparseOutput('./example_move_it_client.cfg', None,
                                        None,
                                        False)
    return args_namespace


@pytest.fixture
def fake_cmd_args(fake_yamlconfig_file):
    """Return a fake argparse Namespace."""
    args_namespace = FakeArgparseOutput('./example_move_it_client.cfg', None,
                                        fake_yamlconfig_file,
                                        False)
    return args_namespace


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_file_log_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_LOG_YAML_CONTENT)

    yield file_path


def test_setup_logging_init(fake_cmd_args):
    """Test initializing the LoggerSetup class."""
    mylogger = LoggerSetup(fake_cmd_args)

    assert mylogger._cmd_args == fake_cmd_args
    assert mylogger._file_handler is None
    assert mylogger._logger is None

    mylogger = LoggerSetup(fake_cmd_args, TEST_LOGGER)
    assert mylogger._logger == TEST_LOGGER


def test_setup_logging_from_log_config(fake_cmd_args):
    """Test getting and setting logging config from file."""
    mylogger = LoggerSetup(fake_cmd_args)
    mylogger.setup_logging('client')

    this_logger = mylogger.get_logger()
    assert this_logger.name == 'root'
    ahandler = this_logger.handlers[2]
    assert ahandler.when == 'H'
    assert ahandler.interval == 3600
    assert ahandler.name == 'timed_log_rotation'
    assert ahandler.level == 10

    # To be continued!? FIXME!


def test_setup_logging_default(fake_cmd_args_no_logfile):
    """Test setting the logging config from without a log-config file."""
    mylogger = LoggerSetup(fake_cmd_args_no_logfile)
    mylogger.setup_logging('client')

    this_logger = mylogger.get_logger()
    assert this_logger.name == 'move_it_client'

    # To be continued!? FIXME!
