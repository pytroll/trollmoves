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

import datetime as dt
import os
import time
import unittest
from collections import deque
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest
from trollsift import globify

from trollmoves.server import MoveItServer, parse_args


def test_file_detected_with_inotify_is_published(tmp_path):
    """Test that a file detected with inotify is published."""
    from threading import Thread

    from posttroll.testing import patched_publisher

    test_file_path = tmp_path / "my_file.hdf"

    config_file = f"""
        [eumetcast-hrit-0deg]
        origin={str(test_file_path)}
        publisher_port=9010
        topic=/some/hdf/file
        delete=False
    """
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as fd:
        fd.write(config_file)

    cmd_args = parse_args([str(config_path)])

    with patched_publisher() as message_list:
        server = MoveItServer(cmd_args)
        server.reload_cfg_file(cmd_args.config_file)
        thr = Thread(target=server.run)
        thr.start()

        # Wait a bit so that the watcher is properly up and running
        time.sleep(.2)
        with open(test_file_path, "w") as fd:
            fd.write("hello!")

        time.sleep(.2)
        try:
            assert len(message_list) == 1
            assert str(test_file_path) in message_list[0]
        finally:
            server.chains_stop()
            thr.join()


def test_create_watchdog_notifier(tmp_path):
    """Test creating a polling notifier."""
    from trollmoves.server import create_watchdog_polling_notifier

    fname = "20200428_1000_foo.tif"
    file_path = tmp_path / fname

    fname_pattern = tmp_path / "{start_time:%Y%m%d_%H%M}_{product}.tif"
    pattern_path = tmp_path / fname_pattern

    function_to_run = MagicMock()
    observer = create_watchdog_polling_notifier(globify(str(pattern_path)), function_to_run, timeout=.1)
    observer.start()

    with open(os.path.join(file_path), "w") as fid:
        fid.write('')

    # Wait for a while for the watchdog to register the event
    time.sleep(.2)

    observer.stop()
    observer.join()

    function_to_run.assert_called_with(str(file_path))


@pytest.mark.parametrize("config,expected_timeout",
                         [({"origin": "/tmp"}, 1.0),
                          ({"origin": "/tmp", "watchdog_timeout": 2.0}, 2.0),
                          ({"origin": "/tmp", "watchdog_timeout": "3.0"}, 3.0),
                          ])
@patch("trollmoves.server.PollingObserver")
def test_create_watchdog_notifier_timeout_default(PollingObserver, config, expected_timeout):
    """Test creating a watchdog notifier with default settings."""
    from trollmoves.server import Chain
    chain = Chain("some_chain", config)
    function_to_run = MagicMock()
    chain.create_notifier(notifier_builder=None, use_polling=True, function_to_run_on_matching_files=function_to_run)
    PollingObserver.assert_called_with(timeout=expected_timeout)


def test_create_posttroll_notifier():
    """Test creating a posttroll notifier."""
    from trollmoves.server import Chain
    config = {"listen": "some_topic"}
    chain = Chain("some_chain", config)
    function_to_run = MagicMock()
    # assert no crash
    from posttroll.testing import patched_subscriber_recv
    with patched_subscriber_recv(["hello"]):
        chain.create_notifier(notifier_builder=None,
                              use_polling=True,
                              function_to_run_on_matching_files=function_to_run)
        chain.start()
        chain.stop()


def test_handler_does_not_dispatch_files_not_matching_pattern():
    """Test that the handle does not dispatch files that are not matching the pattern."""
    from trollmoves.server import WatchdogCreationHandler

    function_to_run = MagicMock()

    handler = WatchdogCreationHandler(function_to_run, pattern="bar")
    event = MagicMock()
    event.dest_path = "foo"
    event.is_directory = False
    assert handler.dispatch(event) is None


def _run_process_notify(process_notify, publisher):
    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

    with TemporaryDirectory() as tmpdir:
        matching_pattern = os.path.join(tmpdir, fname_pattern)
        pathname = os.path.join(tmpdir, fname)
        kwargs = {"origin": matching_pattern,
                  "request_address": "localhost",
                  "request_port": "9001",
                  "topic": "/topic"}

        with open(os.path.join(pathname), "w") as fid:
            fid.write('foo')

        process_notify(pathname, publisher, kwargs)

    return pathname, fname, kwargs


@patch("trollmoves.server.file_cache", new_callable=deque)
def test_process_notify_matching_file(file_cache):
    """Test process_notify() with a file matching the configured pattern."""
    from posttroll.message import Message

    from trollmoves.server import process_notification

    publisher = MagicMock()

    pathname, fname, kwargs = _run_process_notify(process_notification, publisher)

    # Check that the message was formed correctly
    message_info = {'start_time': dt.datetime(2020, 4, 28, 10, 0),
                    'product': 'foo',
                    'uri': pathname,
                    'uid': fname,
                    'request_address': 'localhost:9001'}

    message = Message(rawstr=publisher.send.mock_calls[0][1][0])
    assert message.subject == kwargs["topic"]
    assert message.type == "file"
    assert message.data == message_info

    assert "/topic/20200428_1000_foo.tif" in file_cache
    assert len(file_cache) == 1


class TestDeleter(unittest.TestCase):
    """Test the deleter."""

    def test_empty_init_arguments_does_not_crash_add(self):
        """Test that empty init arguments still work."""
        from trollmoves.server import Deleter
        Deleter(dict()).add('bla')


CONFIG_INI = b"""
[eumetcast-hrit-0deg]
origin = /local_disk/tellicast/received/MSGHRIT/H-000-{nominal_time:%Y%m%d%H%M}-{compressed:_<2s}
request_port = 9094
publisher_port = 9010
info = sensor=seviri;variant=0DEG
topic = /1b/hrit-segment/0deg
delete = False
# Everything below this should end up in connection_parameters dict
connection_uptime = 30
ssh_key_filename = id_rsa.pub
ssh_private_key_file = id_rsa
ssh_connection_timeout = 30
connection_parameters__secret = secret
connection_parameters__client_kwargs__endpoint_url = https://endpoint.url
connection_parameters__client_kwargs__verify = false
"""


def test_read_config_ini_with_dicts():
    """Test reading a config in ini format when dictionary values should be created."""
    from trollmoves.server import read_config

    with NamedTemporaryFile(suffix=".ini") as config_file:
        config_file.write(CONFIG_INI)
        config_file.flush()
        with pytest.warns(UserWarning, match="Consider using connection_parameters__"):
            config = read_config(config_file.name)
        eumetcast = config["eumetcast-hrit-0deg"]
        assert "origin" in eumetcast
        assert "request_port" in eumetcast
        assert "publisher_port" in eumetcast
        assert "info" in eumetcast
        assert "topic" in eumetcast
        assert "delete" in eumetcast
        expected_conn_params = {
            "secret": "secret",
            "client_kwargs": {
                "endpoint_url": "https://endpoint.url",
                "verify": False,
            },
            "connection_uptime": "30",
            "ssh_key_filename": "id_rsa.pub",
            "ssh_private_key_file": "id_rsa",
            "ssh_connection_timeout": "30",
        }
        assert eumetcast["connection_parameters"] == expected_conn_params


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
            temporary_config_file.write(CONFIG_INI)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            server = MoveItServer(cmd_args)
            server.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.server.MoveItServer.reload_config")
    def test_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(CONFIG_INI)
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
            temporary_config_file.write(CONFIG_INI)
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
    from posttroll.message import _MAGICK
    from zmq import POLLIN

    from trollmoves.server import RequestManager
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
    import logging

    from zmq import POLLIN

    from trollmoves.server import RequestManager
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


def test_unpack_with_delete(tmp_path):
    """Test unpacking with deletion."""
    import bz2
    zipped_file = tmp_path / "my_file.txt.bz2"
    with open(zipped_file, 'wb') as fd_:
        fd_.write(bz2.compress(b"hello world", 5))

    from trollmoves.server import unpack

    res = unpack(zipped_file, delete=True, working_directory=tmp_path, compression="bzip")
    assert not os.path.exists(zipped_file)
    assert res == os.path.splitext(zipped_file)[0]
