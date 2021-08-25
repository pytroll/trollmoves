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
from tempfile import TemporaryDirectory
import os


@patch("trollmoves.server.process_notify")
def test_create_watchdog_notifier(process_notify):
    """Test creating a watchdog notifier."""
    import time
    from trollmoves.server import create_watchdog_notifier
    from trollsift import globify

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


@patch("trollmoves.server.Message")
def test_process_notify(Message):
    """Test process_notify()."""
    from trollmoves.server import process_notify
    from trollmoves.server import file_cache, file_cache_lock
    from trollsift import globify
    import datetime as dt

    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"
    not_matching_pattern = "bar"
    publisher = MagicMock()
    with TemporaryDirectory() as tmpdir:
        matching_pattern = os.path.join(tmpdir, fname_pattern)
        pathname = os.path.join(tmpdir, fname)
        kwargs = {"origin": matching_pattern,
                  "request_address": "localhost",
                  "request_port": "9001",
                  "topic": "/topic"}

        process_notify(pathname, publisher, not_matching_pattern, kwargs)
        publisher.assert_not_called()

        with open(os.path.join(pathname), "w") as fid:
            fid.write('foo')

        process_notify(pathname, publisher, globify(matching_pattern), kwargs)

        # Check that the message was formed correctly
        message_info = {'start_time': dt.datetime(2020, 4, 28, 10, 0),
                        'product': 'foo',
                        'uri': pathname,
                        'uid': fname,
                        'request_address': 'localhost:9001'}
        Message.assert_called_with(kwargs['topic'], 'file', message_info)
        # Check that the publisher send was called
        publisher.send.assert_called_with(str(Message.return_value))
        # Check that the file cache was updated
        with file_cache_lock:
            assert "/topic/20200428_1000_foo.tif" in file_cache
            assert len(file_cache) == 1


def test_create_publisher():
    """Test that publisher is created"""
    from trollmoves.move_it_base import create_publisher

    pub = create_publisher(40000, "test_move_it_server")
    assert pub.name == "test_move_it_server"
    assert pub.port_number == 40000
