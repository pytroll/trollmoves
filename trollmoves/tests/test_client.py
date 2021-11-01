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
import time
from threading import Thread

import pytest
from posttroll.message import Message


MSG_FILE = Message('/topic', 'file', {'uid': 'file1.png',
                                      'uri': '/tmp/file1.png'})
MSG_FILE_TAR = Message('/topic', 'file', {'uid': 'file1.tar',
                                          'uri': '/tmp/file1.tar'})
MSG_FILE_BZ2 = Message('/topic', 'file', {'uid': 'file1.png.bz2',
                                          'uri': '/tmp/file1.png.bz2'})
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
        raise AssertionError
    except OSError:
        pass
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


def test_unpack_bzip():
    """Test unpacking of bzip2 files."""
    from trollmoves.client import unpack_bzip
    from tempfile import gettempdir
    import bz2

    try:
        # Write a bz2 file
        fname = os.path.join(gettempdir(), 'asdasdasdasd')
        fname_bz2 = fname + '.bz2'
        with bz2.open(fname_bz2, 'wt') as fid:
            fid.write(100 * '123asddb')

        # No configured options
        kwargs = {}
        res = unpack_bzip(fname_bz2, **kwargs)
        assert res == fname
        assert os.path.exists(fname)

        # Mock things so we know what has been called

        # When the file exists, don't run decompression
        with patch('trollmoves.client.open') as opn:
            res = unpack_bzip(fname_bz2, **kwargs)
        opn.assert_not_called()

        # Custom block size is as a string in the config
        kwargs['block_size'] = '2048'
        with patch('os.path.exists') as exists:
            exists.return_value = False
            with patch('trollmoves.client.open') as opn:
                mock_bz2_fid = MagicMock()
                mock_bz2_fid.read.return_value = False
                with patch('trollmoves.client.bz2.BZ2File') as bz2file:
                    bz2file.return_value = mock_bz2_fid
                    res = unpack_bzip(fname_bz2, **kwargs)
        mock_bz2_fid.read.assert_called_with(2048)
    finally:
        os.remove(fname)
        os.remove(fname_bz2)


def test_unpack_tar():
    """Test unpacking of bzip2 files."""
    from trollmoves.client import unpack_tar
    from tempfile import gettempdir
    import tarfile

    try:
        # Write two test files
        test_txt_file_1 = os.path.join(gettempdir(), "unpack_test_1.txt")
        with open(test_txt_file_1, 'w') as fid:
            fid.write('test 1\n')
        test_txt_file_2 = os.path.join(gettempdir(), "unpack_test_2.txt")
        with open(test_txt_file_2, 'w') as fid:
            fid.write('test 2\n')
        # Write a test .tar file with single file
        test_tar_file = os.path.join(gettempdir(), "unpack_test.tar")
        with tarfile.open(test_tar_file, 'w') as fid:
            fid.add(test_txt_file_1, arcname=os.path.basename(test_txt_file_1))
        os.remove(test_txt_file_1)

        new_files = unpack_tar(test_tar_file)
        assert new_files == test_txt_file_1
        assert os.path.exists(test_txt_file_1)
        os.remove(test_txt_file_1)

        # Add another file to the .tar
        with tarfile.open(test_tar_file, 'a') as fid:
            fid.add(test_txt_file_2, arcname=os.path.basename(test_txt_file_2))
        os.remove(test_txt_file_2)

        new_files = unpack_tar(test_tar_file)
        assert isinstance(new_files, tuple)
        assert test_txt_file_1 in new_files
        assert test_txt_file_2 in new_files
        assert os.path.exists(test_txt_file_1)
        assert os.path.exists(test_txt_file_2)

    finally:
        if os.path.exists(test_txt_file_1):
            os.remove(test_txt_file_1)
        if os.path.exists(test_txt_file_2):
            os.remove(test_txt_file_2)
        if os.path.exists(test_tar_file):
            os.remove(test_tar_file)


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

    # The unpacker returns a full path for some reason
    unpackers['tar'].return_value = os.path.join(local_dir, 'new_file1.png')
    res = unp(copy.copy(MSG_FILE_TAR), local_dir, **kwargs)
    assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'

    # One file with 'bz2' compression
    kwargs['compression'] = 'bzip'
    unpackers['bzip'].return_value = 'file1.png'
    res = unp(copy.copy(MSG_FILE_BZ2), local_dir, **kwargs)
    assert res.data['uri'] == os.path.join(local_dir, 'file1.png')
    assert res.data['uid'] == 'file1.png'
    assert res.subject == MSG_FILE_BZ2.subject
    assert res.type == MSG_FILE_BZ2.type
    unpackers['bzip'].assert_called_with('/local/file1.png.bz2', **kwargs)

    # One file with 'xrit' compression
    kwargs['compression'] = 'xrit'
    unpackers['xrit'].return_value = 'new_file1.png'
    with patch('os.remove') as remove:
        res = unp(copy.copy(MSG_FILE_XRIT), local_dir, **kwargs)
    # Delete has not been setup, so it shouldn't been done
    remove.assert_not_called()
    assert res.data['uri'] == os.path.join(local_dir, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_XRIT.subject
    assert res.type == MSG_FILE_XRIT.type
    unpackers['xrit'].assert_called_with('/local/file1-C_', **kwargs)

    # One file with 'xrit' compression, delete the compressed file
    kwargs['delete'] = True
    unpackers['xrit'].return_value = 'new_file1.png'
    with patch('os.remove') as remove:
        res = unp(copy.copy(MSG_FILE_XRIT), local_dir, **kwargs)
    assert remove.called_once_with('/local/file1-C_')
    del kwargs['delete']

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

CLIENT_CONFIG_1_NON_PUB_ITEM_MODIFIED = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010 explorer:9010 primary_client
destination = scp:///tmp/bar
login = user
topic = /1b/hrit-segment/0deg
publish_port = 0
processing_delay = 0.02
"""

CLIENT_CONFIG_1_PUB_ITEM_MODIFIED = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010 explorer:9010 primary_client
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/0deg
publish_port = 12345
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


@pytest.fixture
def listener():
    callback = MagicMock()
    with patch('trollmoves.client.CTimer'):
        with patch('trollmoves.heartbeat_monitor.Monitor'):
            with patch('trollmoves.client.Subscriber'):
                from trollmoves.client import Listener
                listener = Listener('127.0.0.1:0', ['/topic'], callback, 'arg1', 'arg2',
                                    kwarg1='kwarg1', kwarg2='kwarg2')
                yield listener


@pytest.fixture
def delayed_listener():
    callback = MagicMock()
    with patch('trollmoves.client.CTimer'):
        with patch('trollmoves.heartbeat_monitor.Monitor'):
            with patch('trollmoves.client.Subscriber'):
                from trollmoves.client import Listener
                listener = Listener('127.0.0.1:0', ['/topic'], callback, 'arg1', 'arg2',
                                    processing_delay=0.02,
                                    kwarg1='kwarg1', kwarg2='kwarg2')
                yield listener


def test_listener_init(delayed_listener):
    """Test listener init."""
    assert delayed_listener.topics == ['/topic']
    assert delayed_listener.callback is not None
    assert delayed_listener.subscriber is None
    assert delayed_listener.address == '127.0.0.1:0'
    assert delayed_listener.running is False
    assert delayed_listener.cargs == ('arg1', 'arg2')
    kwargs = {'processing_delay': 0.02, 'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'}
    for key, itm in delayed_listener.ckwargs.items():
        assert kwargs[key] == itm


@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_push_message(add_to_file_cache, add_to_ongoing, delayed_listener):
    """Test listener push message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_PUSH]

    _run_listener_in_thread(delayed_listener)

    add_to_file_cache.assert_not_called()
    add_to_ongoing.assert_called_with(MSG_PUSH)


@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_ack_message(add_to_file_cache, clean_ongoing_transfer, delayed_listener):
    """Test listener with ack message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_ACK]

    _run_listener_in_thread(delayed_listener)

    clean_ongoing_transfer.assert_called_with("826e8142e6baabe8af779f5f490cf5f5")
    add_to_file_cache.assert_called_with(MSG_ACK)


@patch('trollmoves.client.add_timer')
@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_beat_message(add_to_file_cache, clean_ongoing_transfer, add_to_ongoing, add_timer, delayed_listener):
    """Test listener with beat message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_BEAT]

    _run_listener_in_thread(delayed_listener)

    add_to_file_cache.assert_not_called()
    add_to_ongoing.assert_not_called()
    add_timer.assert_not_called()
    clean_ongoing_transfer.assert_not_called()


@patch('trollmoves.client.add_timer')
@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
@patch('trollmoves.client.CTimer')
def test_listener_sync_file_message(
        CTimer, add_to_file_cache, clean_ongoing_transfer, add_to_ongoing, add_timer, delayed_listener):
    """Test listener with a file message from another client."""
    from trollmoves.client import get_msg_uid

    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_FILE1]

    _run_listener_in_thread(delayed_listener)

    CTimer.assert_not_called()
    add_to_file_cache.assert_called_with(MSG_FILE1)
    clean_ongoing_transfer.assert_called_with(get_msg_uid(MSG_FILE1))
    add_to_ongoing.assert_called_with(MSG_FILE1)
    add_timer.assert_not_called()


@patch('trollmoves.client.CTimer')
def test_listener_file_message(CTimer, delayed_listener):
    """Test listener with a file message from Trollmoves Server."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_FILE2]
    _run_listener_in_thread(delayed_listener)

    CTimer.assert_called()


@patch('trollmoves.client.add_timer')
def test_listener_no_delay_file_message(add_timer, listener):
    """Test listener without a delay receiving a file message."""
    listener.create_subscriber()
    listener.subscriber.return_value = [MSG_FILE2]

    _run_listener_in_thread(listener)

    listener.callback.assert_called_with(MSG_FILE2, 'arg1', 'arg2',
                                         kwarg1='kwarg1', kwarg2='kwarg2')
    add_timer.assert_not_called()


def test_listener_stop(listener):
    """Test stopping the listener."""
    listener.create_subscriber()
    assert listener.subscriber is not None
    listener.stop()
    time.sleep(1.5)
    assert listener.running is False
    assert listener.subscriber is None


def _run_listener_in_thread(listener_instance):
    thr = Thread(target=listener_instance.run)
    thr.start()
    time.sleep(0.1)
    listener_instance.running = False
    thr.join(2)


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


@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.send_request')
@patch('trollmoves.client.send_ack')
def test_request_push(send_ack, send_request, clean_ongoing_transfer):
    """Test trollmoves.client.request_push()."""
    from trollmoves.client import request_push, file_cache, ongoing_transfers
    from tempfile import gettempdir

    # Clear file cache and ongoing transfers, the other tests have added stuff in it
    file_cache.clear()
    ongoing_transfers.clear()

    clean_ongoing_transfer.return_value = [MSG_FILE2]
    send_request.return_value = [MSG_FILE2, 'localhost']
    publisher = MagicMock()
    kwargs = {'transfer_req_timeout': 1.0, 'req_timeout': 1.0}

    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)

    send_request.assert_called_once()
    send_ack.assert_called_once()
    # The file should be added to ongoing transfers
    assert UID_FILE2 in ongoing_transfers
    # And removed
    clean_ongoing_transfer.assert_called_once_with(UID_FILE2)
    # Clear the ongoing transfers as it would've been without mocking
    ongoing_transfers.clear()
    # The transferred file should be in the cache
    assert MSG_FILE2.data['uid'] in file_cache
    assert len(file_cache) == 1

    # Request the same file again. Now the transfer should not be
    # started again, and `send_ack()` should have been called again.
    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)

    assert send_ack.call_count == 2
    send_request.assert_called_once()
    # The new "ongoing" transfer should be cleared
    assert clean_ongoing_transfer.call_count == 2


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
        reload_config(config_fname_1, chains, callback=callback)
        section_name = "eumetcast_hrit_0deg_scp_hot_spare"
        assert section_name in chains
        listeners = chains[section_name].listeners
        assert len(listeners) == 4
        # The same listener was used for all, so it should have been
        # started four times
        for key in listeners:
            assert listeners[key].start.call_count == 4
        NoisyPublisher.assert_called_once()
        chains[section_name]._np.start.assert_called_once()
        chains[section_name].stop()
        # Reload the same config again, nothing should happen
        reload_config(config_fname_1, chains, callback=callback)
        for key in listeners:
            assert listeners[key].start.call_count == 4
        NoisyPublisher.assert_called_once()
        chains[section_name]._np.start.assert_called_once()
        chains[section_name].stop()

        # Load a new config with one new item
        reload_config(config_fname_2, chains, callback=callback)
        assert len(chains) == 2
        assert "foo" in chains
        # One additional call to publisher and listener
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5
        for section_name in chains:
            chains[section_name].stop()

        # Load the first config again, the other chain should have been removed
        reload_config(config_fname_1, chains, callback=callback)
        assert "foo" not in chains
        # No new calls to publisher nor listener
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5
        for section_name in chains:
            chains[section_name].stop()
    finally:
        os.remove(config_fname_1)
        os.remove(config_fname_2)


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_publisher_items_not_changed(Listener, NoisyPublisher):
    """Test trollmoves.client.reload_config() when other than publisher related items are changed."""
    from trollmoves.client import reload_config

    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_a = fid.name
        fid.write(CLIENT_CONFIG_1_ITEM)
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_b = fid.name
        fid.write(CLIENT_CONFIG_1_NON_PUB_ITEM_MODIFIED)

    chains = {}
    callback = MagicMock()

    try:
        reload_config(config_fname_a, chains, callback=callback)
        NoisyPublisher.assert_called_once()
        reload_config(config_fname_b, chains, callback=callback)
        NoisyPublisher.assert_called_once()
    finally:
        for key in chains:
            try:
                chains[key].stop()
            except AttributeError:
                pass
        os.remove(config_fname_a)
        os.remove(config_fname_b)


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_publisher_items_changed(Listener, NoisyPublisher):
    """Test trollmoves.client.reload_config() when publisher related items are changed."""
    from trollmoves.client import reload_config

    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_a = fid.name
        fid.write(CLIENT_CONFIG_1_ITEM)
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname_b = fid.name
        fid.write(CLIENT_CONFIG_1_PUB_ITEM_MODIFIED)

    chains = {}
    callback = MagicMock()

    try:
        reload_config(config_fname_a, chains, callback=callback)
        NoisyPublisher.assert_called_once()
        reload_config(config_fname_b, chains, callback=callback)
        assert NoisyPublisher.call_count == 2
    finally:
        for key in chains:
            try:
                chains[key].stop()
            except AttributeError:
                pass
        os.remove(config_fname_a)
        os.remove(config_fname_b)


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


@patch('trollmoves.client.ongoing_transfers_lock')
def test_iterate_messages(lock):
    """Test iterate_messages()."""
    from trollmoves.client import ongoing_transfers, iterate_messages
    values = ["bar", "baz"]
    ongoing_transfers["foo"] = values.copy()
    res = iterate_messages("foo")
    assert list(res) == values
    assert len(lock.__enter__.mock_calls) == 3


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain(Listener, NoisyPublisher, caplog):
    """Test the Chain object."""
    from trollmoves.client import Chain, read_config
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
        fid.write(CLIENT_CONFIG_1_ITEM)
    try:
        conf = read_config(config_fname)
    finally:
        os.remove(config_fname)

    def restart():
        return Listener()

    side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(),
                   MagicMock(), MagicMock(), MagicMock(), MagicMock(),
                   MagicMock(), MagicMock(), MagicMock(), MagicMock()]
    for lis in side_effect:
        lis.is_alive.return_value = True
        lis.death_count = 0
        lis.restart.side_effect = restart
    Listener.side_effect = side_effect

    # Init
    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, conf[name])
    NoisyPublisher.assert_called_once()
    assert chain.listeners == {}
    assert not chain.listener_died_event.is_set()

    # Setup listeners
    callback = MagicMock()
    chain.setup_listeners(callback)
    assert len(chain.listeners) == 4

    # Check running with alive listeners
    import trollmoves.client
    with patch('trollmoves.client.LISTENER_CHECK_INTERVAL', new=.1):
        trollmoves.client.LISTENER_CHECK_INTERVAL = .1
        chain.start()
        try:
            with patch.object(chain, 'restart_dead_listeners') as rdl:
                time.sleep(.2)
                assert rdl.call_count == 0
                chain.listener_died_event.set()
                time.sleep(.2)
                assert rdl.call_count == 1
                assert not chain.listener_died_event.is_set()

            chain.listener_died_event.set()
            time.sleep(.2)
            assert not chain.listener_died_event.is_set()

            # Check with listener crashing once
            listener = chain.listeners['tcp://satmottag2:9010']
            listener.is_alive.return_value = False
            listener.cause_of_death = RuntimeError('OMG, they killed the listener!')
            chain.listener_died_event.set()
            time.sleep(.2)
            listener.restart.assert_called_once()
            assert "Listener for tcp://satmottag2:9010 died 1 time: OMG, they killed the listener!" in caplog.text
            time.sleep(.6)

            # Check with listener crashing all the time
            death_count = 0
            for lis in side_effect[5:]:
                death_count += 1
                lis.is_alive.return_value = False
                lis.cause_of_death = RuntimeError('OMG, they killed the listener!')
                lis.death_count = death_count

            listener = chain.listeners['tcp://satmottag2:9010']
            listener.is_alive.return_value = False
            listener.death_count = 0
            listener.cause_of_death = RuntimeError('OMG, they killed the listener!')
            listener.restart.side_effect = restart

            chain.listener_died_event.set()
            time.sleep(2)
            assert "Listener for tcp://satmottag2:9010 switched off: OMG, they killed the listener!" in caplog.text
        finally:
            chain.stop()


CHAIN_BASIC_CONFIG = {"login": "user:pass", "topic": "/foo", "publish_port": 12345, "nameservers": None}


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_publisher_needs_restarting_no_change(Listener, NoisyPublisher):
    """Test the publisher_needs_restarting() method of Chain object when nothing changes."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    chain = Chain("foo", config.copy())
    assert chain.publisher_needs_restarting(config.copy()) is False


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_publisher_needs_restarting_non_publisher_value_modified(Listener, NoisyPublisher):
    """Test the publisher_needs_restarting() method of Chain object when a value not related to Publisher is changed."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    chain = Chain("foo", config.copy())
    config["login"] = "user2:pass2"
    assert chain.publisher_needs_restarting(config.copy()) is False


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_publisher_needs_restarting_non_publisher_value_added(Listener, NoisyPublisher):
    """Test the publisher_needs_restarting() method of Chain object when a value not related to Publisher is added."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    chain = Chain("foo", config.copy())
    config["destination"] = "file:///tmp/"
    assert chain.publisher_needs_restarting(config.copy()) is False


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_publisher_needs_restarting_nameservers_modified(Listener, NoisyPublisher):
    """Test the publisher_needs_restarting() method of Chain object when nameservers are modified."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    chain = Chain("foo", config.copy())
    config["nameservers"] = ["host1", "host2"]
    assert chain.publisher_needs_restarting(config.copy()) is True


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_publisher_needs_restarting_port_modified(Listener, NoisyPublisher):
    """Test the publisher_needs_restarting() method of Chain object when publish port is modified."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    chain = Chain("foo", config.copy())
    config["publish_port"] = 12346
    assert chain.publisher_needs_restarting(config.copy()) is True
