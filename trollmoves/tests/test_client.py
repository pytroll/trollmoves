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

import copy
from unittest.mock import MagicMock, patch, call
from tempfile import NamedTemporaryFile
import os

from posttroll.message import Message


MSG_FILE = Message('/topic', 'file', {'uid': 'file1.png',
                                      'uri': '/tmp/file1.png'})
MSG_FILE_TAR = Message('/topic', 'file', {'uid': 'file1.tar',
                                          'uri': '/tmp/file1.tar'})
MSG_FILE_BZ2 = Message('/topic', 'file', {'uid': 'file1.bz2',
                                          'uri': '/tmp/file1.bz2'})
MSG_FILE_XRIT = Message('/topic', 'file', {'uid': 'file1-C_',
                                           'uri': '/tmp/file1-C_'})
MSG_DATASET_TAR = Message('/topic', 'dataset',
                          {'dataset': [{'uid': 'file1.tgz',
                                        'uri': '/tmp/file1.tgz'},
                                       {'uid': 'file2.tar.gz',
                                        'uri': '/tmp/file2.tar.gz'}]})
MSG_COLLECTION_TAR = Message('/topic', 'collection',
                             {'collection':
                              [{'dataset': [{'uid': 'file1.tar.bz2',
                                             'uri': '/tmp/file1.tar.bz2'}]}]})
COMPRESSION_CONFIG = """
[DEFAULT]
providers = 127.0.0.1:40000
destination = ftp://127.0.0.1:/tmp
topic = /topic

[empty_decompression]

[xrit_decompression]
compression = xrit
"""


@patch('os.remove')
@patch('trollmoves.client.check_output')
def test_unpack_xrit(check_output, remove):
    """Test unpacking of Eumetsat SEVIRI XRIT/HRIT segments."""
    from trollmoves.client import unpack_xrit

    # No configured options
    kwargs = {}

    # File already decompressed
    fname_in = "/tmp/H-000-MSG4__-MSG4________-_________-PRO______-201909031245-__"

    res = unpack_xrit(fname_in, **kwargs)
    assert res == fname_in
    check_output.assert_not_called()
    remove.assert_not_called()

    # Compressed segment
    fname_in = "/tmp/H-000-MSG4__-MSG4________-IR_134___-000003___-201909031245-C_"

    try:
        res = unpack_xrit(fname_in, **kwargs)
        # Should raise OSError as xritdecompressor hasn't been defined
        assert False
    except OSError:
        assert True
    remove.assert_not_called()

    # Define xritdecompressor path
    kwargs = {'xritdecompressor': '/path/to/xRITDecompress'}
    res = unpack_xrit(fname_in, **kwargs)
    assert check_output.called_once_with(
        ['/path/to/xRITDecompress', fname_in], cwd=('/tmp'))
    remove.assert_not_called()

    # Define also delete
    kwargs = {'delete': True, 'xritdecompressor': '/path/to/xRITDecompress'}

    res = unpack_xrit(fname_in, **kwargs)
    assert check_output.called_once_with(
        ['/path/to/xRITDecompress', fname_in], cwd=('/tmp'))
    assert remove.called_once_with(fname_in)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message(unpackers):
    """Test unpacking and updating the message with new filenames."""
    from trollmoves.client import unpack_and_create_local_message as unp

    local_dir = '/local'
    kwargs = {'kwarg': 'value'}

    # No compression defined
    res = unp(copy.copy(MSG_FILE), local_dir, **kwargs)
    assert res.subject == MSG_FILE.subject
    assert res.data == MSG_FILE.data
    assert res.type == MSG_FILE.type
    # A new message is returned
    assert res is not MSG_FILE
    unpackers.__getitem__.assert_not_called()

    # One file with 'tar' compression
    kwargs['compression'] = 'tar'
    unpackers['tar'].return_value = 'new_file1.png'
    res = unp(copy.copy(MSG_FILE_TAR), local_dir, **kwargs)
    assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_TAR.subject
    assert res.type == MSG_FILE_TAR.type
    unpackers['tar'].assert_called_with('/local/file1.tar', **kwargs)

    # One file with 'bz2' compression
    kwargs['compression'] = 'bz2'
    unpackers['bz2'].return_value = 'new_file1.png'
    res = unp(copy.copy(MSG_FILE_BZ2), local_dir, **kwargs)
    assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_BZ2.subject
    assert res.type == MSG_FILE_BZ2.type
    unpackers['bz2'].assert_called_with('/local/file1.bz2', **kwargs)

    # One file with 'xrit' compression
    kwargs['compression'] = 'xrit'
    unpackers['xrit'].return_value = 'new_file1.png'
    res = unp(copy.copy(MSG_FILE_XRIT), local_dir, **kwargs)
    assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_XRIT.subject
    assert res.type == MSG_FILE_XRIT.type
    unpackers['xrit'].assert_called_with('/local/file1-C_', **kwargs)

    # Multiple files returned when decompression is applied. 'tar'
    # compression, 'file' message
    kwargs['compression'] = 'tar'
    unpackers['tar'].return_value = ('new_file1.png', 'new_file2.png')
    res = unp(copy.copy(MSG_FILE_TAR), local_dir, **kwargs)
    assert res.data['dataset'][0]['uid'] == 'new_file1.png'
    assert res.data['dataset'][0]['uri'] == '/local/new_file1.png'
    assert res.data['dataset'][1]['uid'] == 'new_file2.png'
    assert res.data['dataset'][1]['uri'] == '/local/new_file2.png'
    assert res.subject == MSG_FILE_TAR.subject
    assert res.type == "dataset"
    unpackers['tar'].assert_called_with('/local/file1.tar', **kwargs)

    # Dataset message, 'tar' compression
    kwargs['compression'] = 'tar'
    unpackers['tar'].return_value = None
    unpackers['tar'].side_effect = ['new_file1.png', 'new_file2.png']
    res = unp(copy.copy(MSG_DATASET_TAR), local_dir, **kwargs)
    assert res.data['dataset'][0]['uid'] == 'new_file1.png'
    assert res.data['dataset'][0]['uri'] == '/local/new_file1.png'
    assert res.data['dataset'][1]['uid'] == 'new_file2.png'
    assert res.data['dataset'][1]['uri'] == '/local/new_file2.png'
    assert res.subject == MSG_DATASET_TAR.subject
    assert res.type == MSG_DATASET_TAR.type
    assert call('/local/file1.tgz', **kwargs) in unpackers['tar'].mock_calls
    assert call('/local/file2.tar.gz', **kwargs) in unpackers['tar'].mock_calls

    # Collection message, 'tar' compression
    kwargs['compression'] = 'tar'
    unpackers['tar'].return_value = None
    unpackers['tar'].side_effect = ['new_file1.png']
    res = unp(copy.copy(MSG_COLLECTION_TAR), local_dir, **kwargs)
    assert res.data['collection'][0]['dataset'][0]['uid'] == 'new_file1.png'
    assert res.data['collection'][0]['dataset'][0]['uri'] == '/local/new_file1.png'
    assert res.subject == MSG_COLLECTION_TAR.subject
    assert res.type == MSG_COLLECTION_TAR.type
    assert call('/local/file1.tar.bz2', **kwargs) in unpackers['tar'].mock_calls

    # Test with config file
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
        fid.write(COMPRESSION_CONFIG)

    try:
        from trollmoves.client import read_config
        config = read_config(config_fname)

        # No compression
        kwargs = config['empty_decompression']
        res = unp(copy.copy(MSG_FILE), local_dir, **kwargs)
        assert res.subject == MSG_FILE.subject
        assert res.data == MSG_FILE.data
        assert res.type == MSG_FILE.type
        # A new message is returned
        assert res is not MSG_FILE

        # One file with 'xrit' compression
        kwargs = config['xrit_decompression']
        unpackers['xrit'].side_effect = None
        unpackers['xrit'].return_value = 'new_file1.png'
        res = unp(copy.copy(MSG_FILE_XRIT), local_dir, **kwargs)
        assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
        assert res.data['uid'] == 'new_file1.png'
        assert res.subject == MSG_FILE_XRIT.subject
        assert res.type == MSG_FILE_XRIT.type
    finally:
        os.remove(config_fname)


# The different messages that are handled.  For further tests `data`
# can be populated with more values.
MSG_PUSH = Message('/topic', 'push', data={'uid': 'file1'})
MSG_ACK = Message('/topic', 'ack', data={'uid': 'file1'})
MSG_FILE1 = Message('/topic', 'file', data={'uid': 'file1'})
UID_FILE1 = "826e8142e6baabe8af779f5f490cf5f5"
MSG_FILE2 = Message('/topic', 'file', data={'uid': 'file2',
                                            'request_address': '127.0.0.1:0'})
UID_FILE2 = '1c1c96fd2cf8330db0bfa936ce82f3b9'
MSG_BEAT = Message('/topic', 'beat', data={'uid': 'file1'})

CLIENT_CONFIG_1_ITEM = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010 explorer:9010 primary_client
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/0deg
publish_port = 0
processing_delay = 0.02
"""

CLIENT_CONFIG_2_ITEMS = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010 explorer:9010 primary_client
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/0deg
publish_port = 0
processing_delay = 0.02

[foo]
providers = bar
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/0deg
publish_port = 0
"""


@patch('trollmoves.client.hot_spare_timer_lock')
@patch('trollmoves.client.CTimer')
@patch('trollmoves.heartbeat_monitor.Monitor')
@patch('trollmoves.client.Subscriber')
def test_listener(Subscriber, Monitor, CTimer, hot_spare_timer_lock):
    """Test listener."""
    from trollmoves.client import (Listener, ongoing_transfers, file_cache,
                                   ongoing_hot_spare_timers)

    # Mock subscriber returning messages
    subscriber = MagicMock()
    Subscriber.return_value = subscriber

    # Mock heartbeat monitor
    beat_monitor = MagicMock()
    Monitor.return_value.__enter__.return_value = beat_monitor
    # Mock callback
    callback = MagicMock()
    # Mock timer lock
    timer_lock = MagicMock()
    hot_spare_timer_lock.return_value.__enter__.return_value = timer_lock

    # Mock timer
    timer = MagicMock()
    CTimer.return_value = timer

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

    # "Receive" a 'push' message
    subscriber.return_value = [MSG_PUSH]
    # Raise something to stop listener
    timer.start.side_effect = [StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 0
    assert len(ongoing_transfers) == 1
    CTimer.assert_called_once_with(0.02, callback,
                                   args=[MSG_PUSH, 'arg1', 'arg2'],
                                   kwargs=kwargs)
    assert UID_FILE1 in ongoing_hot_spare_timers
    ongoing_hot_spare_timers[UID_FILE1].start.assert_called_once()
    assert listener.subscriber is subscriber
    assert listener.running
    beat_monitor.assert_called()
    # Reset
    ongoing_transfers = dict()

    # "Receive" 'push' and 'ack' messages
    subscriber.return_value = [MSG_PUSH, MSG_ACK]
    # Raise something to stop listener
    timer.start.side_effect = [None, StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 1
    assert len(ongoing_transfers) == 0
    assert len(timer.start.mock_calls) == 3

    # Receive also a 'file' and 'beat' messages
    subscriber.return_value = [MSG_PUSH, MSG_ACK, MSG_BEAT, MSG_FILE1]
    timer.start.side_effect = [None, None, StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    assert len(file_cache) == 1
    assert len(ongoing_transfers) == 0
    # Messages with type 'beat' don't increment call-count
    assert len(timer.start.mock_calls) == 6

    # Test listener.stop()
    listener.stop()
    assert listener.running is False
    subscriber.close.assert_called_once()
    assert listener.subscriber is None

    # Reset
    ongoing_hot_spare_timers = dict()

    # Run without processing delay
    listener = Listener('127.0.0.1:0', ['/topic'], callback, 'arg1', 'arg2',
                        kwarg1='kwarg1', kwarg2='kwarg2')
    # "Receive" a 'file' message
    subscriber.return_value = [MSG_FILE1]
    # Raise something to stop listener
    callback.side_effect = [StopIteration]
    try:
        listener.run()
    except StopIteration:
        pass
    callback.assert_called_once_with(MSG_FILE1, 'arg1', 'arg2',
                                     kwarg1='kwarg1', kwarg2='kwarg2')
    assert len(ongoing_hot_spare_timers) == 0
    assert MSG_FILE1.data['uid'] in file_cache


@patch('trollmoves.client.ongoing_transfers_lock')
def test_add_to_ongoing(lock):
    """Test add_to_ongoing()."""
    from trollmoves.client import (add_to_ongoing, ongoing_transfers,
                                   ongoing_hot_spare_timers)

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    # Add a message to ongoing transfers
    res = add_to_ongoing(MSG_FILE1)
    lock_cm.assert_called_once()
    assert res is not None
    assert len(ongoing_transfers) == 1
    assert isinstance(ongoing_transfers[UID_FILE1], list)
    assert len(ongoing_transfers[UID_FILE1]) == 1

    # Add the same message again
    res = add_to_ongoing(MSG_FILE1)
    assert len(lock_cm.mock_calls) == 2
    assert res is None
    assert len(ongoing_transfers) == 1
    assert len(ongoing_transfers[UID_FILE1]) == 2

    # Another message, a new ongoing transfer is added
    res = add_to_ongoing(MSG_FILE2)
    assert len(lock_cm.mock_calls) == 3
    assert res is not None
    assert len(ongoing_transfers) == 2

    # Clear transfers
    ongoing_transfers = dict()
    # There's a timer running for hot-spare functionality
    timer = MagicMock()
    ongoing_hot_spare_timers[UID_FILE1] = timer
    res = add_to_ongoing(MSG_FILE1)
    timer.cancel.assert_called_once()
    assert len(ongoing_hot_spare_timers) == 0


@patch('trollmoves.client.cache_lock')
def test_add_to_file_cache(lock):
    """Test trollmoves.client.add_to_file_cache()."""
    from trollmoves.client import add_to_file_cache, file_cache

    # Clear file cache, the other tests have added stuff in it
    file_cache.clear()

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    # Add a file to cache
    add_to_file_cache(MSG_FILE1)
    lock_cm.assert_called_once()
    assert len(file_cache) == 1
    assert MSG_FILE1.data['uid'] in file_cache

    # Add the same file again
    add_to_file_cache(MSG_FILE1)
    assert len(lock_cm.mock_calls) == 2
    # The file should be there only once
    assert len(file_cache) == 1
    assert MSG_FILE1.data['uid'] in file_cache

    # Add another file
    add_to_file_cache(MSG_FILE2)
    assert len(lock_cm.mock_calls) == 3
    assert len(file_cache) == 2
    assert MSG_FILE2.data['uid'] in file_cache


@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.ongoing_transfers')
@patch('trollmoves.client.terminate_transfers')
@patch('trollmoves.client.send_request')
@patch('trollmoves.client.send_ack')
def test_request_push(send_ack, send_request, terminate_transfers,
                      ongoing_transfers, add_to_ongoing):
    """Test trollmoves.client.request_push()."""
    from trollmoves.client import request_push, file_cache
    from tempfile import gettempdir

    # Clear file cache, the other tests have added stuff in it
    file_cache.clear()

    ongoing_transfers[UID_FILE2].pop.return_value = MSG_FILE2
    send_request.return_value = [MSG_FILE2, 'localhost']
    publisher = MagicMock()
    kwargs = {'transfer_req_timeout': 1.0, 'req_timeout': 1.0}

    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)

    send_request.assert_called_once()
    send_ack.assert_not_called()
    # The file should be added to ongoing transfers
    add_to_ongoing.assert_called_once()
    # And removed
    ongoing_transfers[UID_FILE2].pop.assert_called_once()
    # The transferred file should be in the cache
    assert MSG_FILE2.data['uid'] in file_cache
    assert len(file_cache) == 1

    # Request the same file again. Now the transfer should not be
    # started again, and `send_ack()` should be called.
    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)

    send_ack.assert_called_once()
    send_request.assert_called_once()


def test_read_config():
    """Test config handling."""
    from trollmoves.client import read_config
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
        fid.write(CLIENT_CONFIG_1_ITEM)
    try:
        conf = read_config(config_fname)
    finally:
        os.remove(config_fname)

    # Test that required things are present
    section_name = "eumetcast_hrit_0deg_scp_hot_spare"
    assert section_name in conf
    section_keys = conf[section_name].keys()
    for key in ["delete", "working_directory", "compression",
                "heartbeat", "req_timeout", "transfer_req_timeout",
                "nameservers", "providers", "topic", "publish_port", ]:
        assert key in section_keys
    assert isinstance(conf[section_name]["providers"], list)


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config(Listener, NoisyPublisher):
    """Test trollmoves.client.reload_config(), which also builds the chains."""
    from trollmoves.client import reload_config

    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_1 = fid.name
        fid.write(CLIENT_CONFIG_1_ITEM)
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_2 = fid.name
        fid.write(CLIENT_CONFIG_2_ITEMS)

    chains = {}
    callback = MagicMock()

    try:
        reload_config(config_fname_1, chains, callback=callback,
                      pub_instance='pub')
        section_name = "eumetcast_hrit_0deg_scp_hot_spare"
        assert section_name in chains
        listeners = chains[section_name]['listeners']
        assert len(listeners) == 4
        # The same listener was used for all, so it should have been
        # started four times
        for key in listeners:
            assert listeners[key].start.call_count == 4
        NoisyPublisher.assert_called_once()
        chains[section_name]['publisher'].start.assert_called_once()

        # Reload the same config again, nothing should happen
        reload_config(config_fname_1, chains, callback=callback,
                      pub_instance='pub')
        for key in listeners:
            assert listeners[key].start.call_count == 4
        NoisyPublisher.assert_called_once()
        chains[section_name]['publisher'].start.assert_called_once()

        # Load a new config with one new item
        reload_config(config_fname_2, chains, callback=callback,
                      pub_instance='pub')
        assert len(chains) == 2
        assert "foo" in chains
        # One additional call to publisher and listener
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5

        # Load the first config again, the other chain should have been removed
        reload_config(config_fname_1, chains, callback=callback,
                      pub_instance='pub')
        assert "foo" not in chains
        # No new calls to publisher nor listener
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5
    finally:
        os.remove(config_fname_1)
        os.remove(config_fname_2)


@patch('trollmoves.client.hot_spare_timer_lock')
@patch('trollmoves.client.CTimer')
def test_add_timer(CTimer, hot_spare_timer_lock):
    """Test adding timer."""
    from trollmoves.client import add_timer, ongoing_hot_spare_timers

    # Mock timer
    timer = MagicMock()
    CTimer.return_value = timer

    kwargs = {'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'}
    add_timer(0.02, '', MSG_FILE1, 'arg1', 'arg2', **kwargs)

    CTimer.assert_called_once_with(0.02, '',
                                   args=[MSG_FILE1, 'arg1', 'arg2'],
                                   kwargs=kwargs)
    timer.start.assert_called_once()
    hot_spare_timer_lock.__enter__.assert_called_once()
    assert UID_FILE1 in ongoing_hot_spare_timers
    assert len(ongoing_hot_spare_timers) == 1
