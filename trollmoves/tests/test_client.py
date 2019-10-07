#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019
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
"""Test the trollmoves client."""

from unittest.mock import MagicMock, patch

from posttroll.message import Message

# The different messages that are handled.  For further tests `data`
# can be populated with more values.
MSG_PUSH = Message('/topic', 'push', data={'uid': 'file1'})
MSG_ACK = Message('/topic', 'ack', data={'uid': 'file1'})
MSG_FILE1 = Message('/topic', 'file', data={'uid': 'file1'})
MSG_FILE2 = Message('/topic', 'file', data={'uid': 'file2'})
MSG_BEAT = Message('/topic', 'beat', data={'uid': 'file1'})


@patch('trollmoves.heartbeat_monitor')
@patch('trollmoves.client.Subscriber')
def test_listener(Subscriber, heartbeat_monitor):
    """Test listener."""
    from trollmoves.client import Listener, ongoing_transfers, file_cache

    # Mock subscriber returning messages
    subscriber = MagicMock()
    Subscriber.return_value = subscriber

    # Mock heartbeat monitor
    beat_monitor = MagicMock()
    # Mock callback
    callback = MagicMock()

    # Create the listener that is configured with small processing
    # delay so it works as it would in client meant to be a hot spare
    listener = Listener('127.0.0.1:0', ['/topic'], callback, 'arg1', 'arg2',
                        processing_delay=0.02,
                        kwarg1='kwarg1', kwarg2='kwarg2')

    # Test __init__
    assert listener.topics == ['/topic']
    assert listener.callback is callback
    assert listener.subscriber is None
    assert listener.address == '127.0.0.1:0'
    assert listener.running is False
    assert listener.cargs == ('arg1', 'arg2')
    kwargs = {'processing_delay': 0.02, 'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'}
    for key, itm in listener.ckwargs.items():
        assert kwargs[key] == itm

    # "Receive" no message, and a 'push' message
    subscriber.return_value = [None, MSG_PUSH]
    # Raise something to stop listener
    callback.side_effect = [None, StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 0
    assert len(ongoing_transfers) == 1
    assert len(callback.mock_calls) == 1
    assert listener.subscriber is subscriber
    assert listener.running
    heartbeat_monitor.assert_called()

    # Reset
    ongoing_transfers = {}

    # "Receive" 'push' and 'ack' messages
    subscriber.return_value = [MSG_PUSH, MSG_ACK]
    # Raise something to stop listener
    callback.side_effect = [None, StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 1
    assert len(ongoing_transfers) == 0
    assert len(callback.mock_calls) == 3

    # Receive also a 'file' and 'beat' messages
    subscriber.return_value = [MSG_PUSH, MSG_ACK, MSG_BEAT, MSG_FILE1]
    callback.side_effect = [None, None, None, StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 1
    assert len(ongoing_transfers) == 0
    # Messages with type 'beat' don't increment callback call-count
    assert len(callback.mock_calls) == 7

    # Test listener.stop()
    listener.stop()
    assert listener.running is False
    subscriber.close.assert_called_once()
    assert listener.subscriber is None
