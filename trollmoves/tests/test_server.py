#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 Trollmoves developers
#
# Author(s):
#
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

"""Test Trollmoves server."""

from unittest.mock import MagicMock, patch
import unittest
from tempfile import TemporaryDirectory, NamedTemporaryFile
import os
from collections import deque
import time
import datetime as dt
import pytest

from trollsift import globify
from trollmoves.server import MoveItServer, parse_args


@patch("trollmoves.server.process_notify")
def test_create_watchdog_notifier(process_notify):
    """Test creating a watchdog notifier."""
    from trollmoves.server import create_watchdog_notifier

    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"
    publisher = "publisher"
    with TemporaryDirectory() as tmpdir:
        pattern_path = os.path.join(tmpdir, fname_pattern)
        file_path = os.path.join(tmpdir, fname)
        attrs = {"origin": pattern_path}
        observer, fun = create_watchdog_notifier(attrs, publisher)
        observer.start()

        with open(os.path.join(file_path), "w") as fid:
            fid.write('')

        # Wait for a while for the watchdog to register the event
        time.sleep(2.0)

        observer.stop()
        observer.join()

    fun.assert_called_with(file_path, publisher, globify(pattern_path), attrs)


@patch("trollmoves.server.WatchdogHandler")
@patch("trollmoves.server.PollingObserver")
@patch("trollmoves.server.process_notify")
def test_create_watchdog_notifier_timeout_default(process_notify, PollingObserver, WatchdogHandler):
    """Test creating a watchdog notifier with default settings."""
    from trollmoves.server import create_watchdog_notifier

    attrs = {"origin": "/tmp"}
    publisher = ""
    # No timeout, the default should be used
    observer, fun = create_watchdog_notifier(attrs, publisher)
    PollingObserver.assert_called_with(timeout=1.0)


@patch("trollmoves.server.WatchdogHandler")
@patch("trollmoves.server.PollingObserver")
@patch("trollmoves.server.process_notify")
def test_create_watchdog_notifier_timeout_float_timeout(process_notify, PollingObserver, WatchdogHandler):
    """Test creating a watchdog notifier with default settings."""
    from trollmoves.server import create_watchdog_notifier

    attrs = {"origin": "/tmp", "watchdog_timeout": 2.0}
    publisher = ""
    observer, fun = create_watchdog_notifier(attrs, publisher)
    PollingObserver.assert_called_with(timeout=2.0)


@patch("trollmoves.server.WatchdogHandler")
@patch("trollmoves.server.PollingObserver")
@patch("trollmoves.server.process_notify")
def test_create_watchdog_notifier_timeout_string_timeout(process_notify, PollingObserver, WatchdogHandler):
    """Test creating a watchdog notifier with default settings."""
    from trollmoves.server import create_watchdog_notifier

    attrs = {"origin": "/tmp", "watchdog_timeout": "3.0"}
    publisher = ""
    observer, fun = create_watchdog_notifier(attrs, publisher)
    PollingObserver.assert_called_with(timeout=3.0)


@patch("trollmoves.server.file_cache", new_callable=deque)
@patch("trollmoves.server.Message")
def test_process_notify_not_matching_file(Message, file_cache):
    """Test process_notify() with a file that doesn't match the configured pattern."""
    from trollmoves.server import process_notify

    publisher = MagicMock()
    not_matching_pattern = "bar"

    _ = _run_process_notify(process_notify, publisher, not_matching_pattern)

    publisher.assert_not_called()
    assert len(file_cache) == 0


def _run_process_notify(process_notify, publisher, pattern=None):
    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

    with TemporaryDirectory() as tmpdir:
        matching_pattern = os.path.join(tmpdir, fname_pattern)
        pathname = os.path.join(tmpdir, fname)
        kwargs = {"origin": matching_pattern,
                  "request_address": "localhost",
                  "request_port": "9001",
                  "topic": "/topic"}
        if pattern is None:
            pattern = globify(matching_pattern)

        with open(os.path.join(pathname), "w") as fid:
            fid.write('foo')

        process_notify(pathname, publisher, pattern, kwargs)

    return pathname, fname, kwargs


@patch("trollmoves.server.file_cache", new_callable=deque)
@patch("trollmoves.server.Message")
def test_process_notify_matching_file(Message, file_cache):
    """Test process_notify() with a file matching the configured pattern."""
    from trollmoves.server import process_notify

    publisher = MagicMock()

    pathname, fname, kwargs = _run_process_notify(process_notify, publisher)

    # Check that the message was formed correctly
    message_info = {'start_time': dt.datetime(2020, 4, 28, 10, 0),
                    'product': 'foo',
                    'uri': pathname,
                    'uid': fname,
                    'request_address': 'localhost:9001'}
    Message.assert_called_with(kwargs['topic'], 'file', message_info)
    publisher.send.assert_called_with(str(Message.return_value))
    assert "/topic/20200428_1000_foo.tif" in file_cache
    assert len(file_cache) == 1


class TestDeleter(unittest.TestCase):
    """Test the deleter."""

    def test_empty_init_arguments_does_not_crash_add(self):
        """Test that empty init arguments still work."""
        from trollmoves.server import Deleter
        Deleter(dict()).add('bla')


config_file = b"""
[eumetcast-hrit-0deg]
origin = /local_disk/tellicast/received/MSGHRIT/H-000-{nominal_time:%Y%m%d%H%M}-{compressed:_<2s}
request_port = 9094
publisher_port = 9010
info = sensor=seviri;variant=0DEG
topic = /1b/hrit-segment/0deg
delete = False
"""


class TestMoveItServer:
    """Test the move it server."""

    def test_reloads_config_crashes_when_config_file_does_not_exist(self):
        """Test that reloading a non existing config file crashes."""
        cmd_args = parse_args(["--port", "9999", "somefile99999.cfg"])
        server = MoveItServer(cmd_args)
        with pytest.raises(FileNotFoundError):
            server.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    def test_reloads_config_on_example_config(self, fake_publisher):
        """Test that config can be reloaded with basic example."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            server = MoveItServer(cmd_args)
            server.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.server.MoveItServer.reload_config")
    def test_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            server = MoveItServer(cmd_args)
            server.reload_cfg_file(cmd_args.config_file)
            mock_reload_config.assert_called_once()

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.server.MoveItServer.reload_config")
    def test_signal_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded through signal."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args([config_filename])
            client = MoveItServer(cmd_args)
            client.signal_reload_cfg_file()
            mock_reload_config.assert_called_once()


@patch("trollmoves.server.get_context")
@patch("trollmoves.server.Poller.poll")
@patch("trollmoves.server.RequestManager._set_station")
@patch("trollmoves.server.RequestManager._set_out_socket")
@patch("trollmoves.server.RequestManager._get_address_and_payload")
@patch("trollmoves.server.RequestManager._validate_file_pattern")
@patch("trollmoves.server.RequestManager._process_request")
def test_requestmanager_run_valid_pytroll_message(patch_process_request,
                                                  patch_validate_file_pattern,
                                                  patch_get_address_and_payload,
                                                  patch_set_out_socket,
                                                  patch_set_station,
                                                  patch_poller,
                                                  patch_get_context):
    """Test request manager run with valid address and payload."""
    from zmq import POLLIN
    from trollmoves.server import RequestManager
    from posttroll.message import _MAGICK
    payload = (_MAGICK +
               r'/test/1/2/3 info ras@hawaii 2008-04-11T22:13:22.123000 v1.01' +
               r' text/ascii "what' + r"'" + r's up doc"')
    address = b'tcp://192.168.10.8:37325'
    patch_get_address_and_payload.return_value = address, payload
    port = 9876
    patch_poller.return_value = {'POLLIN': POLLIN}
    req_man = RequestManager(port)
    req_man.out_socket = 'POLLIN'
    req_man._run_loop()
    patch_process_request.assert_called_once()


@patch("trollmoves.server.get_context")
@patch("trollmoves.server.Poller.poll")
@patch("trollmoves.server.RequestManager._set_station")
@patch("trollmoves.server.RequestManager._set_out_socket")
@patch("trollmoves.server.RequestManager._get_address_and_payload")
@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_run_MessageError_exception(patch_validate_file_pattern,
                                                   patch_get_address_and_payload,
                                                   patch_set_out_socket,
                                                   patch_set_station,
                                                   patch_poller,
                                                   patch_get_context,
                                                   caplog):
    """Test request manager run with invalid payload causing a MessageError exception."""
    from zmq import POLLIN
    from trollmoves.server import RequestManager
    import logging
    patch_get_address_and_payload.return_value = "address", "fake_payload"
    port = 9876
    patch_poller.return_value = {'POLLIN': POLLIN}
    req_man = RequestManager(port)
    req_man.out_socket = 'POLLIN'
    with caplog.at_level(logging.DEBUG):
        req_man._run_loop()
    assert "Failed to create message from payload: fake_payload with address address" in caplog.text


@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_is_delete_set(patch_validate_file_pattern):
    """Test delete default config."""
    from trollmoves.server import RequestManager
    port = 9876
    req_man = RequestManager(port, attrs={})
    assert req_man._is_delete_set() is False


@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_is_delete_set_True(patch_validate_file_pattern):
    """Test setting delete to True."""
    from trollmoves.server import RequestManager
    port = 9876
    req_man = RequestManager(port, attrs={'delete': True})
    assert req_man._is_delete_set() is True
