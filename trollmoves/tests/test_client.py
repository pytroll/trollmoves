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

import os
from unittest.mock import MagicMock, patch, call
import copy

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
    unpackers['tar'].return_value = 'new_file1.png'
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
