#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022
#
# Author(s):
#
#   Trygve Aspenes <trygveas@met.no>
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
"""Test the s3downloader."""

from unittest.mock import MagicMock, PropertyMock, patch, call
from tempfile import NamedTemporaryFile
import os
import time
from threading import Thread
from collections import deque

import pytest
from posttroll.message import Message

CONFIG_YAML = """
logging:
  log_rotation_days: 1
  log_rotation_backup: 30
  logging_mode: DEBUG

subscribe-topic:
  - /yuhu
publish-topic: /idnt
endpoint_url: 'https://your.url.space'
access_key: 'your_access_key'
secret_key: 'your_secret_key'
bucket: atms-sdr
download_destination: '/destination-directory'
"""


def _write_named_temporary_config(data):
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
        fid.write(data)
    return config_fname


@pytest.fixture
def config_yaml():
    yield _write_named_temporary_config(CONFIG_YAML)


def test_read_config(config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import read_config
    config = read_config(config_yaml, debug=False)
    expected_config = {'logging': {'log_rotation_days': 1, 'log_rotation_backup': 30, 'logging_mode': 'DEBUG'},
                       'subscribe-topic': ['/yuhu'], 'publish-topic': '/idnt', 'endpoint_url': 'https://your.url.space',
                       'access_key': 'your_access_key',
                       'secret_key': 'your_secret_key', 'bucket': 'atms-sdr', 'download_destination': '/destination-directory'}
    assert config == expected_config


def test_read_config_debug(capsys, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import read_config
    read_config(config_yaml)
    captured = capsys.readouterr()
    assert '/destination-directory' in captured.out


@patch('yaml.safe_load')
def test_read_config_exception(patch_yaml, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import read_config
    patch_yaml.side_effect = FileNotFoundError
    with pytest.raises(FileNotFoundError):
        read_config(config_yaml, debug=False)


@patch('yaml.safe_load')
def test_read_config_exception2(patch_yaml, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import read_config
    import yaml
    patch_yaml.side_effect = yaml.YAMLError
    with pytest.raises(yaml.YAMLError):
        read_config(config_yaml, debug=False)


def test_setup_logging(config_yaml):
    """Setup logging"""
    from trollmoves.s3downloader import setup_logging
    from trollmoves.s3downloader import read_config
    config = read_config(config_yaml, debug=False)
    setup_logging(config, log_file=None)


def test_get_basename():
    from trollmoves.s3downloader import _get_basename
    uri = os.path.join("root", "anypath", "filename-basename")
    bn = _get_basename(uri)
    assert bn == 'filename-basename'


@patch('os.path.exists')
def test_generate_message_if_file_exists_after_download(patch_os_path_exists, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _generate_message_if_file_exists_after_download
    bn = 'filename-basename'
    to_send = {'some_key': 'with_a_value'}
    msg = Message('/publish-topic', "file", to_send)
    config = read_config(config_yaml, debug=False)
    patch_os_path_exists.return_value = True
    pubmsg = _generate_message_if_file_exists_after_download(config, bn, msg)
    assert 'with_a_value' in pubmsg


@patch('os.path.exists')
def test_generate_message_if_file_does_not_exists_after_download(patch_os_path_exists, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _generate_message_if_file_exists_after_download
    bn = 'filename-basename'
    to_send = {'some_key': 'with_a_value'}
    msg = Message('/publish-topic', "file", to_send)
    config = read_config(config_yaml, debug=False)
    patch_os_path_exists.return_value = False
    pubmsg = _generate_message_if_file_exists_after_download(config, bn, msg)
    assert pubmsg == None


@patch('trollmoves.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader._get_basename')
@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message(patch_subscribe, patch_publish_queue, patch_get_basename, patch_download_from_s3, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    to_send = {'some_key': 'with_a_value', 'uri': 'now-this-is-a-uri'}
    msg = Message('/publish-topic', "file", to_send)
    patch_subscribe.get.return_value = {'msg': msg}
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert result == True


@patch('trollmoves.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader._get_basename')
@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_none(patch_subscribe, patch_publish_queue, patch_get_basename, patch_download_from_s3, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_subscribe.get.return_value = None
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert result == True


@patch('trollmoves.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader._get_basename')
@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_download_false(patch_subscribe, patch_publish_queue, patch_get_basename, patch_download_from_s3, caplog, config_yaml):
    import logging
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = False
    caplog.set_level(logging.DEBUG)
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert 'Could not download file filename-basename for some reason. SKipping this.' in caplog.text
    assert result == True


@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_keyboardinterrupt(patch_subscribe, patch_publish_queue, config_yaml):
    import logging
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_subscribe.get.side_effect = KeyboardInterrupt
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert result == False


@patch('trollmoves.s3downloader._get_one_message')
@patch('queue.Queue')
@patch('queue.Queue')
def test_read_from_queue(patch_subscribe, patch_publish_queue, patch_get_one_message, config_yaml):
    import logging
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import read_from_queue
    config = read_config(config_yaml, debug=False)
    patch_get_one_message.return_value = False
    read_from_queue(config, patch_subscribe, patch_publish_queue)

# @patch('trollmoves.s3downloader._get_one_message')
# @patch('queue.Queue')
# @patch('queue.Queue')
# def test_read_from_queue_loop(patch_subscribe, patch_publish_queue, patch_get_one_message, config_yaml):
#     import logging
#     from trollmoves.s3downloader import read_config
#     from trollmoves.s3downloader import read_from_queue
#     config = read_config(config_yaml, debug=False)
#     to_send = {'some_key': 'with_a_value'}
#     msg = Message('/publish-topic', "file", to_send)
#     patch_get_one_message.return_value = True
#     running = PropertyMock(side_effect=[True, False])
#     read_from_queue(config, patch_subscribe, patch_publish_queue)


@patch('boto3.client')
def test_download_from_s3(patch_boto3_client, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _download_from_s3
    config = read_config(config_yaml, debug=False)
    bn = 'filename-basename'
    result = _download_from_s3(config, bn)
    assert result == True


@patch('boto3.client')
def test_download_from_s3_exception(patch_boto3_client, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _download_from_s3
    import botocore
    config = read_config(config_yaml, debug=False)
    bn = 'filename-basename'
    error_response = {'Error': {'Code': 'TEST',
                                'Message': 'Throttling',
                                }
                      }
    patch_boto3_client.return_value.download_file.side_effect = botocore.exceptions.ClientError(
        error_response=error_response, operation_name='test')
    result = _download_from_s3(config, bn)
    assert result == False
