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
from collections import deque

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
MSG_MIRROR = Message('/topic', 'file', {'fname': 'file1', 'uri':
                                        'scp://user@host/tmp/bar/file1.txt', 'uid':
                                        'file1.txt', 'destination': 'scp://targethost.domain/tmp/bar/',
                                        'origin': 'sourcehost.domain:9201'})
COMPRESSION_CONFIG = """
[DEFAULT]
providers = 127.0.0.1:40000
destination = ftp://127.0.0.1:/tmp
topic = /topic

[empty_decompression]

[xrit_decompression]
compression = xrit
"""

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

CLIENT_CONFIG_1_ITEM_NON_PUB_PROVIDER_ITEM_MODIFIED = """
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

CLIENT_CONFIG_1_ITEM_TWO_PROVIDERS = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/0deg
publish_port = 0
processing_delay = 0.02
"""

CLIENT_CONFIG_1_ITEM_TOPIC_CHANGED = """
# Example acting as a hot spare
[eumetcast_hrit_0deg_scp_hot_spare]
providers = satmottag2:9010 satmottag:9010 explorer:9010 primary_client
destination = scp:///tmp/foo
login = user
topic = /1b/hrit-segment/zero_degrees
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

LOCAL_DIR = "/local"

CHAIN_BASIC_CONFIG = {"login": "user:pass", "topic": "/foo", "publish_port": 12345, "nameservers": None,
                      "providers": ["tcp://provider:1", "tcp://provider:2", "tcp://provider:3"]}


@pytest.fixture
def listener():
    with patch('trollmoves.client.CTimer'):
        with patch('trollmoves.heartbeat_monitor.Monitor'):
            with patch('trollmoves.client.Subscriber'):
                from trollmoves.client import Listener
                listener = Listener('127.0.0.1:0', ['/topic'], 'arg1', 'arg2',
                                    kwarg1='kwarg1', kwarg2='kwarg2')
                yield listener


@pytest.fixture
def delayed_listener():
    with patch('trollmoves.client.CTimer'):
        with patch('trollmoves.heartbeat_monitor.Monitor'):
            with patch('trollmoves.client.Subscriber'):
                from trollmoves.client import Listener
                listener = Listener('127.0.0.1:0', ['/topic'], 'arg1', 'arg2',
                                    processing_delay=0.02,
                                    kwarg1='kwarg1', kwarg2='kwarg2')
                yield listener


def _write_named_temporary_config(data):
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
        fid.write(data)
    return config_fname


def _write_to_tar(file_to_add, remove_in_file=False, filename=None):
    from tempfile import gettempdir
    import tarfile

    mode = 'a'
    if filename is None:
        mode = 'w'
        filename = os.path.join(gettempdir(), "unpack_test.tar")

    with tarfile.open(filename, mode) as fid:
        fid.add(file_to_add, arcname=os.path.basename(file_to_add))

    if remove_in_file:
        os.remove(file_to_add)

    return filename


@pytest.fixture
def client_config_1_item():
    yield _write_named_temporary_config(CLIENT_CONFIG_1_ITEM)


@pytest.fixture
def client_config_1_item_non_pub_provider_item_modified():
    yield _write_named_temporary_config(CLIENT_CONFIG_1_ITEM_NON_PUB_PROVIDER_ITEM_MODIFIED)


@pytest.fixture
def client_config_1_item_two_providers():
    yield _write_named_temporary_config(CLIENT_CONFIG_1_ITEM_TWO_PROVIDERS)


@pytest.fixture
def client_config_1_item_topic_changed():
    yield _write_named_temporary_config(CLIENT_CONFIG_1_ITEM_TOPIC_CHANGED)


@pytest.fixture
def client_config_1_pub_item_modified():
    yield _write_named_temporary_config(CLIENT_CONFIG_1_PUB_ITEM_MODIFIED)


@pytest.fixture
def client_config_2_items():
    yield _write_named_temporary_config(CLIENT_CONFIG_2_ITEMS)


@pytest.fixture
def compression_config():
    yield _write_named_temporary_config(COMPRESSION_CONFIG)


@pytest.fixture
def test_txt_file_1():
    yield _write_named_temporary_config("test 1\n")


@pytest.fixture
def test_txt_file_2():
    yield _write_named_temporary_config("test 2\n")


@pytest.fixture
def chain_config_with_one_item(client_config_1_item):
    from trollmoves.client import read_config

    try:
        conf = read_config(client_config_1_item)
    finally:
        os.remove(client_config_1_item)

    yield conf


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


def test_unpack_tar(test_txt_file_1, test_txt_file_2):
    """Test unpacking of bzip2 files."""
    try:
        # Write a test .tar file with single file
        test_tar_file = _write_to_tar(test_txt_file_1, remove_in_file=True)
        _test_and_clean_unpack_tar(test_tar_file, [test_txt_file_1])

        # Add another file to the .tar
        _ = _write_to_tar(test_txt_file_2, remove_in_file=True, filename=test_tar_file)
        _test_and_clean_unpack_tar(test_tar_file, [test_txt_file_1, test_txt_file_2])

    finally:
        os.remove(test_tar_file)


def _test_and_clean_unpack_tar(test_tar_file, output_files):
    from trollmoves.client import unpack_tar

    new_files = unpack_tar(test_tar_file)
    for f in output_files:
        assert f in new_files
        assert os.path.exists(f)
        os.remove(f)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_no_compression(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with no compression.
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'kwarg': 'value'}

    # No compression defined
    res = unp(copy.copy(MSG_FILE), LOCAL_DIR, **kwargs)
    assert res.subject == MSG_FILE.subject
    assert res.data == MSG_FILE.data
    assert res.type == MSG_FILE.type
    # A new message is returned
    assert res is not MSG_FILE
    unpackers.__getitem__.assert_not_called()


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_one_tar_file(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with one tar file.
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'tar'}
    unpackers['tar'].return_value = 'new_file1.png'
    res = unp(copy.copy(MSG_FILE_TAR), LOCAL_DIR, **kwargs)
    assert res.data['uri'] == os.path.join(LOCAL_DIR, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_TAR.subject
    assert res.type == MSG_FILE_TAR.type
    unpackers['tar'].assert_called_with(os.path.join(LOCAL_DIR, MSG_FILE_TAR.data['uid']), **kwargs)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_full_path(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with unpacker returning a full path
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'tar'}
    unpackers['tar'].return_value = os.path.join(LOCAL_DIR, 'new_file1.png')
    res = unp(copy.copy(MSG_FILE_TAR), LOCAL_DIR, **kwargs)
    assert res.data['uri'] == os.path.join(LOCAL_DIR, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_bz2_compression(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with bz2 compression
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'bzip'}
    unpackers['bzip'].return_value = 'file1.png'
    res = unp(copy.copy(MSG_FILE_BZ2), LOCAL_DIR, **kwargs)
    assert res.data['uri'] == os.path.join(LOCAL_DIR, 'file1.png')
    assert res.data['uid'] == 'file1.png'
    assert res.subject == MSG_FILE_BZ2.subject
    assert res.type == MSG_FILE_BZ2.type
    unpackers['bzip'].assert_called_with(os.path.join(LOCAL_DIR, MSG_FILE_BZ2.data['uid']), **kwargs)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_xrit_compression_no_delete(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with xrit compression and file not being deleted
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'xrit'}
    unpackers['xrit'].return_value = 'new_file1.png'
    with patch('os.remove') as remove:
        res = unp(copy.copy(MSG_FILE_XRIT), LOCAL_DIR, **kwargs)
    # Delete has not been setup, so it shouldn't been done
    remove.assert_not_called()
    assert res.data['uri'] == os.path.join(LOCAL_DIR, 'new_file1.png')
    assert res.data['uid'] == 'new_file1.png'
    assert res.subject == MSG_FILE_XRIT.subject
    assert res.type == MSG_FILE_XRIT.type
    unpackers['xrit'].assert_called_with(os.path.join(LOCAL_DIR, MSG_FILE_XRIT.data['uid']), **kwargs)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_xrit_compression_with_delete(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with xrit compression and deletion configured
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'xrit', 'delete': True}
    unpackers['xrit'].return_value = 'new_file1.png'
    with patch('os.remove') as remove:
        _ = unp(copy.copy(MSG_FILE_XRIT), LOCAL_DIR, **kwargs)
    assert remove.called_once_with(os.path.join(LOCAL_DIR, MSG_FILE_XRIT.data['uid']))
    del kwargs['delete']


def _check_unpack_result_message_files(res, new_files):
    for i, new_file in enumerate(new_files):
        assert res['dataset'][i]['uid'] == new_file
        assert res['dataset'][i]['uri'] == os.path.join(LOCAL_DIR, new_file)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_tar_multiple_files_file_message(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with tar having multiple files and message with type 'file'
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'tar'}
    new_files = ('new_file1.png', 'new_file2.png')
    unpackers['tar'].return_value = new_files
    res = unp(copy.copy(MSG_FILE_TAR), LOCAL_DIR, **kwargs)
    _check_unpack_result_message_files(res.data, new_files)
    assert res.subject == MSG_FILE_TAR.subject
    assert res.type == "dataset"
    unpackers['tar'].assert_called_with(os.path.join(LOCAL_DIR, MSG_FILE_TAR.data['uid']), **kwargs)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_tar_multiple_files_dataset_message(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with tar having multiple files and message with type 'dataset'
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'tar'}
    unpackers['tar'].return_value = None
    new_files = ('new_file1.png', 'new_file2.png')
    unpackers['tar'].side_effect = new_files
    res = unp(copy.copy(MSG_DATASET_TAR), LOCAL_DIR, **kwargs)
    _check_unpack_result_message_files(res.data, new_files)
    assert res.subject == MSG_DATASET_TAR.subject
    assert res.type == MSG_DATASET_TAR.type
    for dset in MSG_DATASET_TAR.data['dataset']:
        assert call(os.path.join(LOCAL_DIR, dset['uid']), **kwargs) in unpackers['tar'].mock_calls


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_tar_multiple_files_collection_message(unpackers):
    """Test unpacking and updating the message with new filenames.

    Case with tar having multiple files and message with type 'collection'
    """
    from trollmoves.client import unpack_and_create_local_message as unp

    kwargs = {'compression': 'tar'}
    unpackers['tar'].return_value = None
    new_files = ['new_file1.png']
    unpackers['tar'].side_effect = new_files
    res = unp(copy.copy(MSG_COLLECTION_TAR), LOCAL_DIR, **kwargs)
    _check_unpack_result_message_files(res.data['collection'][0], new_files)
    assert res.subject == MSG_COLLECTION_TAR.subject
    assert res.type == MSG_COLLECTION_TAR.type
    file_path = os.path.join(LOCAL_DIR, MSG_COLLECTION_TAR.data['collection'][0]['dataset'][0]['uid'])
    assert call(file_path, **kwargs) in unpackers['tar'].mock_calls


def test_unpack_and_create_local_message_config_no_compression(compression_config):
    """Test unpacking and updating the message with new filenames.

    Case with using a configuration file without compression
    """
    from trollmoves.client import unpack_and_create_local_message as unp
    from trollmoves.client import read_config

    try:
        config = read_config(compression_config)
        kwargs = config['empty_decompression']
        res = unp(copy.copy(MSG_FILE), LOCAL_DIR, **kwargs)
        assert res.subject == MSG_FILE.subject
        assert res.data == MSG_FILE.data
        assert res.type == MSG_FILE.type
        # A new message is returned
        assert res is not MSG_FILE
    finally:
        os.remove(compression_config)


@patch('trollmoves.client.unpackers')
def test_unpack_and_create_local_message_config_xrit_compression(unpackers, compression_config):
    """Test unpacking and updating the message with new filenames.

    Case with using a configuration file with xrit compression
    """
    from trollmoves.client import unpack_and_create_local_message as unp
    from trollmoves.client import read_config

    try:
        config = read_config(compression_config)
        kwargs = config['xrit_decompression']
        unpackers['xrit'].side_effect = None
        unpackers['xrit'].return_value = 'new_file1.png'
        res = unp(copy.copy(MSG_FILE_XRIT), LOCAL_DIR, **kwargs)
        assert res.data['uri'] == os.path.join(LOCAL_DIR, 'new_file1.png')
        assert res.data['uid'] == 'new_file1.png'
        assert res.subject == MSG_FILE_XRIT.subject
        assert res.type == MSG_FILE_XRIT.type
    finally:
        os.remove(compression_config)


@patch('trollmoves.client.request_push')
def test_listener_init(request_push, delayed_listener):
    """Test listener init."""
    assert delayed_listener.topics == ['/topic']
    assert delayed_listener.subscriber is None
    assert delayed_listener.address == '127.0.0.1:0'
    assert delayed_listener.running is False
    assert delayed_listener.cargs == ('arg1', 'arg2')
    kwargs = {'processing_delay': 0.02, 'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'}
    for key, itm in delayed_listener.ckwargs.items():
        assert kwargs[key] == itm


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_push_message(add_to_file_cache, add_to_ongoing, request_push, delayed_listener):
    """Test listener push message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_PUSH]

    _run_listener_in_thread(delayed_listener)

    add_to_file_cache.assert_not_called()
    add_to_ongoing.assert_called_with(MSG_PUSH)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_ack_message(add_to_file_cache, clean_ongoing_transfer, request_push, delayed_listener):
    """Test listener with ack message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_ACK]

    _run_listener_in_thread(delayed_listener)

    clean_ongoing_transfer.assert_called_with("826e8142e6baabe8af779f5f490cf5f5")
    add_to_file_cache.assert_called_with(MSG_ACK)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.add_request_push_timer')
@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
def test_listener_beat_message(add_to_file_cache, clean_ongoing_transfer, add_to_ongoing,
                               add_request_push_timer, request_push, delayed_listener):
    """Test listener with beat message."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_BEAT]

    _run_listener_in_thread(delayed_listener)

    add_to_file_cache.assert_not_called()
    add_to_ongoing.assert_not_called()
    add_request_push_timer.assert_not_called()
    clean_ongoing_transfer.assert_not_called()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.add_request_push_timer')
@patch('trollmoves.client.add_to_ongoing')
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.add_to_file_cache')
@patch('trollmoves.client.CTimer')
def test_listener_sync_file_message(
        CTimer, add_to_file_cache, clean_ongoing_transfer, add_to_ongoing, add_request_push_timer,
        request_push, delayed_listener):
    """Test listener with a file message from another client."""
    from trollmoves.client import get_msg_uid

    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_FILE1]

    _run_listener_in_thread(delayed_listener)

    CTimer.assert_not_called()
    add_to_file_cache.assert_called_with(MSG_FILE1)
    clean_ongoing_transfer.assert_called_with(get_msg_uid(MSG_FILE1))
    add_to_ongoing.assert_called_with(MSG_FILE1)
    add_request_push_timer.assert_not_called()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.CTimer')
def test_listener_file_message(CTimer, request_push, delayed_listener):
    """Test listener with a file message from Trollmoves Server."""
    delayed_listener.create_subscriber()
    delayed_listener.subscriber.return_value = [MSG_FILE2]
    _run_listener_in_thread(delayed_listener)

    CTimer.assert_called()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.add_request_push_timer')
def test_listener_no_delay_file_message(add_request_push_timer, request_push, listener):
    """Test listener without a delay receiving a file message."""
    listener.create_subscriber()
    listener.subscriber.return_value = [MSG_FILE2]

    _run_listener_in_thread(listener)

    request_push.assert_called_with(MSG_FILE2, 'arg1', 'arg2',
                                    kwarg1='kwarg1', kwarg2='kwarg2')
    add_request_push_timer.assert_not_called()


@patch('trollmoves.client.request_push')
def test_listener_stop(request_push, listener):
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


@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.ongoing_transfers_lock')
def test_add_to_ongoing_one_message(lock, ongoing_transfers):
    """Test add_to_ongoing() with a single message."""
    from trollmoves.client import add_to_ongoing

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


@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.ongoing_transfers_lock')
def test_add_to_ongoing_duplicate_message(lock, ongoing_transfers):
    """Test add_to_ongoing() with duplicate messages."""
    from trollmoves.client import add_to_ongoing

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    _ = add_to_ongoing(MSG_FILE1)
    res = add_to_ongoing(MSG_FILE1)
    assert len(lock_cm.mock_calls) == 2
    assert res is None
    assert len(ongoing_transfers) == 1
    assert len(ongoing_transfers[UID_FILE1]) == 2


@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.ongoing_transfers_lock')
def test_add_to_ongoing_two_messages(lock, ongoing_transfers):
    """Test add_to_ongoing()."""
    from trollmoves.client import add_to_ongoing

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    _ = add_to_ongoing(MSG_FILE1)
    res = add_to_ongoing(MSG_FILE2)
    assert len(lock_cm.mock_calls) == 2
    assert res is not None
    assert len(ongoing_transfers) == 2


@patch('trollmoves.client.ongoing_hot_spare_timers', new_callable=dict)
@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.ongoing_transfers_lock')
def test_add_to_ongoing_hot_spare_timer(lock, ongoing_transfers, ongoing_hot_spare_timers):
    """Test add_to_ongoing()."""
    from trollmoves.client import add_to_ongoing

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    # There's a timer running for hot-spare functionality
    timer = MagicMock()
    ongoing_hot_spare_timers[UID_FILE1] = timer
    _ = add_to_ongoing(MSG_FILE1)
    timer.cancel.assert_called_once()
    assert len(ongoing_hot_spare_timers) == 0


@patch('trollmoves.client.file_cache', new_callable=deque)
@patch('trollmoves.client.cache_lock')
def test_add_to_file_cache_one_file(lock, file_cache):
    """Test trollmoves.client.add_to_file_cache() with a single file."""
    from trollmoves.client import add_to_file_cache

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    # Add a file to cache
    add_to_file_cache(MSG_FILE1)
    lock_cm.assert_called_once()
    assert len(file_cache) == 1
    assert MSG_FILE1.data['uid'] in file_cache


@patch('trollmoves.client.file_cache', new_callable=deque)
@patch('trollmoves.client.cache_lock')
def test_add_to_file_cache_duplicate_file(lock, file_cache):
    """Test trollmoves.client.add_to_file_cache() with two identical files."""
    from trollmoves.client import add_to_file_cache

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    add_to_file_cache(MSG_FILE1)
    add_to_file_cache(MSG_FILE1)
    assert len(lock_cm.mock_calls) == 2
    assert len(file_cache) == 1
    assert MSG_FILE1.data['uid'] in file_cache


@patch('trollmoves.client.file_cache', new_callable=deque)
@patch('trollmoves.client.cache_lock')
def test_add_to_file_cache_two_files(lock, file_cache):
    """Test trollmoves.client.add_to_file_cache() with two separate files."""
    from trollmoves.client import add_to_file_cache

    # Mock the lock context manager
    lock_cm = MagicMock()
    lock.__enter__ = lock_cm

    add_to_file_cache(MSG_FILE1)
    add_to_file_cache(MSG_FILE2)
    assert len(lock_cm.mock_calls) == 2
    assert len(file_cache) == 2
    assert MSG_FILE2.data['uid'] in file_cache


@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.file_cache', new_callable=deque)
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.send_request')
@patch('trollmoves.client.send_ack')
def test_request_push_single_call(send_ack, send_request, clean_ongoing_transfer, file_cache, ongoing_transfers):
    """Test trollmoves.client.request_push() with a single file."""
    from trollmoves.client import request_push
    from tempfile import gettempdir

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
    # The transferred file should be in the cache
    assert MSG_FILE2.data['uid'] in file_cache
    assert len(file_cache) == 1


@patch('trollmoves.client.ongoing_transfers', new_callable=dict)
@patch('trollmoves.client.file_cache', new_callable=deque)
@patch('trollmoves.client.clean_ongoing_transfer')
@patch('trollmoves.client.send_request')
@patch('trollmoves.client.send_ack')
def test_request_push_duplicate_call(send_ack, send_request, clean_ongoing_transfer, file_cache, ongoing_transfers):
    """Test trollmoves.client.request_push() with duplicate files."""
    from trollmoves.client import request_push
    from tempfile import gettempdir

    clean_ongoing_transfer.return_value = [MSG_FILE2]
    send_request.return_value = [MSG_FILE2, 'localhost']
    publisher = MagicMock()
    kwargs = {'transfer_req_timeout': 1.0, 'req_timeout': 1.0}

    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)
    # The transfer has been completed
    ongoing_transfers.clear()
    request_push(MSG_FILE2, gettempdir(), 'login', publisher=publisher,
                 **kwargs)

    assert send_ack.call_count == 2
    send_request.assert_called_once()
    # The new "ongoing" transfer should be cleared
    assert clean_ongoing_transfer.call_count == 2
    assert len(file_cache) == 1


@patch('os.makedirs')
@patch('trollmoves.client.send_request')
def test_request_push_disable_directory_creation(send_request, os_makedirs):
    """Test trollmoves.client.request_push() with target directory creation disabled."""
    from trollmoves.client import request_push

    send_request.return_value = [MSG_FILE2, 'localhost']
    publisher = MagicMock()
    kwargs = {'transfer_req_timeout': 1.0, 'req_timeout': 1.0, 'create_target_directory': False}

    not_local_path = 'scp://host:/path/not/existing/on/this/server'
    request_push(MSG_FILE2, not_local_path, 'login', publisher=publisher,
                 **kwargs)
    os_makedirs.assert_not_called()
    dest = send_request.mock_calls[0].args[1].data["destination"]
    assert "scp://login@host/path/not/existing/on/this/server" == dest


def test_read_config(client_config_1_item):
    """Test config handling."""
    from trollmoves.client import read_config

    try:
        conf = read_config(client_config_1_item)
    finally:
        os.remove(client_config_1_item)

    # Test that required things are present
    section_name = "eumetcast_hrit_0deg_scp_hot_spare"
    assert section_name in conf
    section_keys = conf[section_name].keys()
    for key in ["delete", "working_directory", "compression",
                "heartbeat", "req_timeout", "transfer_req_timeout",
                "nameservers", "providers", "topic", "publish_port",
                "create_target_directory"]:
        assert key in section_keys
    assert isinstance(conf[section_name]["providers"], list)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_single_chain(Listener, NoisyPublisher, request_push, client_config_1_item):
    """Test trollmoves.client.reload_config() with a single chain."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        assert len(chains) == 1
        assert "eumetcast_hrit_0deg_scp_hot_spare" in chains
        assert NoisyPublisher.call_count == 1
        assert Listener.call_count == 4
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_chain_added(Listener, NoisyPublisher, request_push, client_config_1_item, client_config_2_items):
    """Test trollmoves.client.reload_config() when a chain is added."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        reload_config(client_config_2_items, chains)
        assert len(chains) == 2
        assert "eumetcast_hrit_0deg_scp_hot_spare" in chains
        assert "foo" in chains
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_2_items)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_chain_removed(Listener, NoisyPublisher, request_push,
                                     client_config_1_item, client_config_2_items):
    """Test trollmoves.client.reload_config() when a chain is added."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_2_items, chains)
        reload_config(client_config_1_item, chains)
        assert len(chains) == 1
        assert "eumetcast_hrit_0deg_scp_hot_spare" in chains
        assert "foo" not in chains
        assert NoisyPublisher.call_count == 2
        assert Listener.call_count == 5
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_2_items)


def _stop_chains(chains):
    for key in chains:
        try:
            chains[key].stop()
        except AttributeError:
            pass


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_publisher_items_not_changed(Listener, NoisyPublisher, request_push, client_config_1_item,
                                                   client_config_1_item_non_pub_provider_item_modified):
    """Test trollmoves.client.reload_config() when other than publisher related items are changed."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        NoisyPublisher.assert_called_once()
        reload_config(client_config_1_item_non_pub_provider_item_modified, chains)
        NoisyPublisher.assert_called_once()
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_1_item_non_pub_provider_item_modified)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_publisher_items_changed(Listener, NoisyPublisher, request_push, client_config_1_item,
                                               client_config_1_pub_item_modified):
    """Test trollmoves.client.reload_config() when publisher related items are changed."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        NoisyPublisher.assert_called_once()
        reload_config(client_config_1_pub_item_modified, chains)
        assert NoisyPublisher.call_count == 2
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_1_pub_item_modified)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_providers_not_changed(Listener, NoisyPublisher, request_push, client_config_1_item,
                                             client_config_1_item_non_pub_provider_item_modified):
    """Test trollmoves.client.reload_config() when other than provider related options are changed."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        num_providers = len(chains["eumetcast_hrit_0deg_scp_hot_spare"]._config['providers'])
        assert Listener.call_count == num_providers
        reload_config(client_config_1_item_non_pub_provider_item_modified, chains)
        assert Listener.call_count == num_providers
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_1_item_non_pub_provider_item_modified)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_providers_added(Listener, NoisyPublisher, request_push, client_config_1_item,
                                       client_config_1_item_two_providers):
    """Test trollmoves.client.reload_config() when providers are added."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item_two_providers, chains)
        _ = _check_providers_listeners_and_listener_calls(chains, Listener)
        reload_config(client_config_1_item, chains)
        _ = _check_providers_listeners_and_listener_calls(chains, Listener)
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item_two_providers)
        os.remove(client_config_1_item)


def _check_providers_listeners_and_listener_calls(chains, Listener, call_count=None):
    num_providers = len(chains["eumetcast_hrit_0deg_scp_hot_spare"]._config['providers'])
    if call_count is None:
        call_count = num_providers
    assert len(chains["eumetcast_hrit_0deg_scp_hot_spare"].listeners) == num_providers
    assert Listener.call_count == call_count
    return num_providers


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_providers_removed(Listener, NoisyPublisher, request_push, client_config_1_item,
                                         client_config_1_item_two_providers):
    """Test trollmoves.client.reload_config() when providers are removed."""
    from trollmoves.client import reload_config

    listener = MagicMock()
    Listener.return_value = listener

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        num_providers = _check_providers_listeners_and_listener_calls(chains, Listener)
        reload_config(client_config_1_item_two_providers, chains)
        num_providers2 = _check_providers_listeners_and_listener_calls(chains, Listener, call_count=num_providers)
        assert num_providers2 != num_providers
        assert listener.stop.call_count == num_providers - num_providers2
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_1_item_two_providers)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_reload_config_provider_topic_changed(Listener, NoisyPublisher, request_push, client_config_1_item,
                                              client_config_1_item_topic_changed):
    """Test trollmoves.client.reload_config() when the message topic is changed."""
    from trollmoves.client import reload_config

    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        num_providers = _check_providers_listeners_and_listener_calls(chains, Listener)
        reload_config(client_config_1_item_topic_changed, chains)
        num_providers2 = _check_providers_listeners_and_listener_calls(chains, Listener, call_count=2 * num_providers)
        assert num_providers2 == num_providers
    finally:
        _stop_chains(chains)
        os.remove(client_config_1_item)
        os.remove(client_config_1_item_topic_changed)


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.Chain')
def test_reload_config_chain_not_recreated(Chain, request_push, client_config_1_item,
                                           client_config_1_pub_item_modified):
    """Test that the chain is not recreated when config is modified."""
    from trollmoves.client import reload_config

    config_equals = MagicMock()
    config_equals.return_value = False
    chain = MagicMock()
    chain.config_equals = config_equals
    Chain.return_value = chain
    chains = {}

    try:
        reload_config(client_config_1_item, chains)
        Chain.assert_called_once()
        reload_config(client_config_1_pub_item_modified, chains)
        Chain.assert_called_once()
    finally:
        os.remove(client_config_1_item)
        os.remove(client_config_1_pub_item_modified)


@patch('trollmoves.client.hot_spare_timer_lock')
@patch('trollmoves.client.CTimer')
def test_add_request_push_timer(CTimer, hot_spare_timer_lock):
    """Test adding timer."""
    from trollmoves.client import add_request_push_timer, ongoing_hot_spare_timers, request_push

    # Mock timer
    timer = MagicMock()
    CTimer.return_value = timer

    kwargs = {'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'}
    add_request_push_timer(0.02, MSG_FILE1, 'arg1', 'arg2', **kwargs)

    CTimer.assert_called_once_with(0.02, request_push,
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


def _mock_listener_for_chain_tests(Listener, is_alive=True):
    def restart():
        return Listener()

    side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(),
                   MagicMock(), MagicMock(), MagicMock(), MagicMock(),
                   MagicMock(), MagicMock(), MagicMock(), MagicMock()]
    for lis in side_effect:
        lis.is_alive.return_value = is_alive
        lis.restart.side_effect = restart
        if is_alive:
            lis.death_count = 0
        else:
            lis.death_count = 3
            lis.cause_of_death = RuntimeError('OMG, they killed the listener!')
    Listener.side_effect = side_effect

    return side_effect


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_init(Listener, NoisyPublisher, chain_config_with_one_item):
    """Test the Chain object."""
    from trollmoves.client import Chain

    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, chain_config_with_one_item[name])

    NoisyPublisher.assert_called_once()
    assert chain.listeners == {}
    assert not chain.listener_died_event.is_set()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_listeners(Listener, NoisyPublisher, request_push, chain_config_with_one_item):
    """Test the Chain object."""
    from trollmoves.client import Chain

    _mock_listener_for_chain_tests(Listener)

    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, chain_config_with_one_item[name])
    chain.setup_listeners()

    assert len(chain.listeners) == 4


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_restart_dead_listeners(Listener, NoisyPublisher, request_push, caplog, chain_config_with_one_item):
    """Test the Chain object."""
    from trollmoves.client import Chain
    import trollmoves.client

    _mock_listener_for_chain_tests(Listener)

    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, chain_config_with_one_item[name])
    chain.setup_listeners()

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
        finally:
            chain.stop()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_listener_crashing_once(Listener, NoisyPublisher, request_push, caplog, chain_config_with_one_item):
    """Test the Chain object."""
    from trollmoves.client import Chain
    import trollmoves.client

    _mock_listener_for_chain_tests(Listener)

    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, chain_config_with_one_item[name])
    chain.setup_listeners()

    with patch('trollmoves.client.LISTENER_CHECK_INTERVAL', new=.1):
        trollmoves.client.LISTENER_CHECK_INTERVAL = .1
        chain.start()
        try:
            listener = chain.listeners['tcp://satmottag2:9010']
            listener.is_alive.return_value = False
            listener.cause_of_death = RuntimeError('OMG, they killed the listener!')
            chain.listener_died_event.set()
            time.sleep(.2)
            listener.restart.assert_called_once()
            assert "Listener for tcp://satmottag2:9010 died 1 time: OMG, they killed the listener!" in caplog.text
            time.sleep(.6)
        finally:
            chain.stop()


@patch('trollmoves.client.request_push')
@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_listener_crashing_all_the_time(Listener, NoisyPublisher, request_push,
                                              caplog, chain_config_with_one_item):
    """Test the Chain object."""
    from trollmoves.client import Chain
    import trollmoves.client

    def restart():
        return Listener

    _mock_listener_for_chain_tests(Listener, is_alive=False)

    name = 'eumetcast_hrit_0deg_scp_hot_spare'
    chain = Chain(name, chain_config_with_one_item[name])
    chain.setup_listeners()

    with patch('trollmoves.client.LISTENER_CHECK_INTERVAL', new=.1):
        trollmoves.client.LISTENER_CHECK_INTERVAL = .1
        chain.start()
        try:
            chain.listener_died_event.set()
            time.sleep(2)
            assert "Listener for tcp://satmottag2:9010 switched off: OMG, they killed the listener!" in caplog.text
        finally:
            chain.stop()


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_get_unchanged_providers(Listener, NoisyPublisher):
    """Test the get_unchanged_providers() method in Chain object."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    config2 = CHAIN_BASIC_CONFIG.copy()
    _ = config2["providers"].pop(1)

    chain = Chain("foo", config)
    res = chain.get_unchanged_providers(config2)
    assert isinstance(res, list)
    assert set(res).difference(config2["providers"]) == set()


@patch('trollmoves.client.NoisyPublisher')
@patch('trollmoves.client.Listener')
def test_chain_get_unchanged_providers_topic_changed(Listener, NoisyPublisher):
    """Test the get_unchanged_providers() method in Chain object when topic changes."""
    from trollmoves.client import Chain

    config = CHAIN_BASIC_CONFIG.copy()
    config2 = CHAIN_BASIC_CONFIG.copy()
    config2["topic"] = "/bar"

    chain = Chain("foo", config)
    assert chain.get_unchanged_providers(config2) == []


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


def test_replace_mda_for_mirror():
    """Test that replacing metadata items works properly for Trollmoves Mirror."""
    from trollmoves.client import replace_mda

    kwargs = {'uri': '/another/path/{filename}.txt'}
    res = replace_mda(MSG_MIRROR, kwargs)
    assert res.data['uri'] == kwargs['uri']


def test_create_local_dir():
    """Test creation of local directory."""
    from tempfile import mkdtemp
    import shutil
    from trollmoves.client import create_local_dir

    destination = "ftp://server.foo/public_path/subdir/"
    local_root = mkdtemp()
    try:
        res = create_local_dir(destination, local_root)
        assert os.path.exists(res)
    finally:
        shutil.rmtree(local_root)


def test_create_local_dir_s3():
    """Test that nothing is done when destination is a S3 bucket."""
    from trollmoves.client import create_local_dir

    destination = "s3://data-bucket/public_path/subdir/"
    res = create_local_dir(destination, "/foo/bar")
    assert res is None


def test_make_uris_local_destination():
    """Test that the published messages are formulated correctly for local destinations."""
    from trollmoves.client import make_uris

    destination = "file://localhost/directory"
    expected_uri = os.path.join(destination, "file1.png").replace("file://", "ssh://")
    msg = make_uris(MSG_FILE, destination)
    assert msg.data['uri'] == expected_uri


def test_make_uris_remote_destination():
    """Test that the published messages are formulated correctly for remote destinations."""
    from trollmoves.client import make_uris

    destination = "ftp://google.com/directory"
    expected_uri = os.path.join(destination, "file1.png")
    msg = make_uris(MSG_FILE, destination)
    assert msg.data['uri'] == expected_uri


def test_make_uris_s3_destination():
    """Test that the published messages are formulated correctly for S3 destinations."""
    from trollmoves.client import make_uris

    destination = "s3://data-bucket/directory"
    expected_uri = destination + "/" + "file1.png"
    msg = make_uris(MSG_FILE, destination)
    assert msg.data['uri'] == expected_uri
