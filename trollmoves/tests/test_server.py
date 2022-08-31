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

from unittest.mock import MagicMock, patch, call
import unittest
from tempfile import TemporaryDirectory
import os
from collections import deque
import time
import datetime as dt

from trollsift import globify


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


@patch("trollmoves.server.Listener._run")
@patch("trollmoves.server.Subscribe")
def test_listener_subscribe_default_settings(Subscribe, _run):
    """Test the default usage of trollmoves.server.Listener."""
    from trollmoves.server import Listener

    attrs = {'listen': '/topic'}
    publisher = 'foo'
    expected = call(
        services='',
        topics=attrs['listen'],
        addr_listener=True,
        addresses=None,
        timeout=10,
        translate=False,
        nameserver=None,
    )
    listener = Listener(attrs, publisher)
    listener.run()
    assert expected in Subscribe.mock_calls
