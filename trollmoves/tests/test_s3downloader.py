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

from logging import StreamHandler
from unittest.mock import PropertyMock, patch
from tempfile import NamedTemporaryFile
import os

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
                       'secret_key': 'your_secret_key',
                       'bucket': 'atms-sdr',
                       'download_destination': '/destination-directory'}
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
    assert pubmsg is None


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
    patch_subscribe.get.return_value = msg
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert result is True


@patch('trollmoves.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader._get_basename')
@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_none(patch_sub_q, patch_pub_q, patch_get_basename, patch_download_from_s3, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_sub_q.get.return_value = None
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = _get_one_message(config, patch_sub_q, patch_pub_q)
    assert result is True


@patch('trollmoves.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader._get_basename')
@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_download_false(patch_sub_q, patch_pub_q, patch_get_bn, patch_dl_s3, caplog, config_yaml):
    import logging
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_get_bn.return_value = 'filename-basename'
    patch_dl_s3.return_value = False
    caplog.set_level(logging.DEBUG)
    result = _get_one_message(config, patch_sub_q, patch_pub_q)
    assert 'Could not download file filename-basename for some reason. SKipping this.' in caplog.text
    assert result is True


@patch('queue.Queue')
@patch('queue.Queue')
def test_get_one_message_keyboardinterrupt(patch_subscribe, patch_publish_queue, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _get_one_message
    config = read_config(config_yaml, debug=False)
    patch_subscribe.get.side_effect = KeyboardInterrupt
    result = _get_one_message(config, patch_subscribe, patch_publish_queue)
    assert result is False


@patch('trollmoves.s3downloader._get_one_message')
@patch('queue.Queue')
@patch('queue.Queue')
def test_read_from_queue(patch_subscribe, patch_publish_queue, patch_get_one_message, config_yaml):
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
    assert result is True


@patch('boto3.client')
def test_download_from_s3_exception(patch_boto3_client, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import _download_from_s3
    import botocore
    config = read_config(config_yaml, debug=False)
    bn = 'filename-basename'
    error_response = {'Error': {'Code': 'TEST',
                                'Message': 'TEST MESSAGE',
                                }
                      }
    patch_boto3_client.return_value.download_file.side_effect = botocore.exceptions.ClientError(
        error_response=error_response, operation_name='test')
    result = _download_from_s3(config, bn)
    assert result is False


@patch('posttroll.publisher.Publish')
@patch('queue.Queue')
def test_file_publisher_init(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    fp = FilePublisher(patch_publish_queue, nameservers)
    assert fp.loop is True
    assert fp.service_name == 's3downloader'
    assert fp.nameservers == nameservers
    assert fp.queue == patch_publish_queue


MSG_1 = Message('/topic', 'file', data={'uid': 'file1'})


@patch('trollmoves.s3downloader.Publish')
@patch('queue.Queue')
def test_file_publisher_run(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    patch_publish_queue.get = PropertyMock(side_effect=[[MSG_1.encode(), None], ])
    fp = FilePublisher(patch_publish_queue, nameservers)
    fp.run()
    patch_publish().__enter__().send.assert_called_once()


@patch('trollmoves.s3downloader.Publish')
@patch('queue.Queue')
def test_file_publisher_break(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    patch_publish_queue.get = PropertyMock(side_effect=[[MSG_1.encode(), None], ])
    fp = FilePublisher(patch_publish_queue, nameservers)
    fp.loop = False
    fp.run()
    patch_publish().__enter__().send.assert_not_called()


@patch('trollmoves.s3downloader.Publish')
@patch('queue.Queue')
def test_file_publisher_publish_message(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    fp = FilePublisher(patch_publish_queue, nameservers)
    assert fp._publish_message(MSG_1, patch_publish) is True
    assert fp._publish_message(None, patch_publish) is True

    fp.loop = False
    assert fp._publish_message('any message', patch_publish) is False


@patch('trollmoves.s3downloader.Publish')
def test_file_publisher_stop_loop(patch_publish):
    import queue
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    pqueue = queue.Queue()
    fp = FilePublisher(pqueue, nameservers)
    fp.stop()
    assert fp.loop is False
    message = pqueue.get()
    assert message is None


@patch('trollmoves.s3downloader.Publish')
@patch('queue.Queue')
def test_file_publisher_exception_1(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    patch_publish_queue.get.side_effect = KeyboardInterrupt
    fp = FilePublisher(patch_publish_queue, nameservers)
    with pytest.raises(KeyboardInterrupt):
        fp.run()


@patch('queue.Queue')
def test_listener_init(patch_listener_queue, config_yaml):
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import Listener
    config = read_config(config_yaml, debug=False)
    subscribe_nameserver = 'localhost'
    listenr = Listener(patch_listener_queue, config, subscribe_nameserver)
    assert listenr.loop is True
    assert listenr.queue == patch_listener_queue
    assert listenr.config == config
    assert listenr.subscribe_nameserver == subscribe_nameserver


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message(patch_get_pub_address, patch_subscriber, caplog, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import setup_logging
    import queue
    config = read_config(config_yaml, debug=False)
    setup_logging(config, None)
    subscribe_nameserver = 'localhost'

    patch_subscriber.return_value.recv = PropertyMock(side_effect=[[MSG_1, None], ])
    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)
    listener.run()

    assert 'Put the message on the queue...' in caplog.text
    assert lqueue.qsize() == 1

    message = lqueue.get()
    assert message.type == 'file'


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
@patch('queue.Queue')
def test_listener_message_break(patch_listener_queue, patch_get_pub_address, patch_subscriber, caplog, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    from trollmoves.s3downloader import setup_logging
    config = read_config(config_yaml, debug=False)
    setup_logging(config, None)
    subscribe_nameserver = 'localhost'

    patch_subscriber.return_value.recv = PropertyMock(side_effect=[[MSG_1, None], ])
    listener = Listener(patch_listener_queue, config, subscribe_nameserver)
    listener.loop = False
    listener.run()
    patch_listener_queue().put.assert_not_called()


MSG_ACK = Message('/topic', 'ack', data={'uid': 'file1'})


def test_listener_message_check_message(config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)

    assert listener.check_message(None) is False
    assert listener.check_message(MSG_ACK) is False
    assert listener.check_message(MSG_1) is True


def test_listener_message_stop(config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)

    listener.stop()
    assert listener.loop is False
    assert listener.queue.qsize() == 1
    message = lqueue.get()
    assert message is None


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_check_config(patch_get_pub_address, patch_subscriber, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    config['subscribe-topic'] = 'is-a-string-topic'
    config['subscriber_addresses'] = 'first_address, second_address'
    subscribe_nameserver = 'localhost'

    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)
    listener.run()
    assert isinstance(listener.config["subscribe-topic"], list) is True
    assert listener.config["services"] == ''


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_check_message_and_put(patch_get_pub_address, patch_subscriber, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    config['subscribe-topic'] = 'is-a-string-topic'
    config['subscriber_addresses'] = 'first_address, second_address'
    subscribe_nameserver = 'localhost'

    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)
    assert listener._check_and_put_message_to_queue(MSG_1) is True
    assert listener._check_and_put_message_to_queue(None) is True

    listener.loop = False
    assert listener._check_and_put_message_to_queue(MSG_1) is False


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_exception_1(patch_get_pub_address, patch_subscriber, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)
    patch_subscriber.side_effect = KeyError
    with pytest.raises(KeyError):
        listener.run()


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_exception_2(patch_get_pub_address, patch_subscriber, config_yaml):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    from trollmoves.s3downloader import read_config
    import queue
    config = read_config(config_yaml, debug=False)
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, config, subscribe_nameserver)
    patch_subscriber.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        listener.run()


def test_setup_logging(config_yaml):
    from trollmoves.s3downloader import setup_logging
    from trollmoves.s3downloader import read_config
    import logging

    config = read_config(config_yaml, debug=False)
    LOGGER, handler = setup_logging(config, None)
    assert isinstance(LOGGER, logging.Logger) is True
    assert logging.DEBUG == handler.level
    assert isinstance(handler, StreamHandler) is True


def test_setup_logging_file(config_yaml):
    from trollmoves.s3downloader import setup_logging
    from trollmoves.s3downloader import read_config
    import logging

    config = read_config(config_yaml, debug=False)
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name

    LOGGER, handler = setup_logging(config, config_fname)
    assert isinstance(LOGGER, logging.Logger) is True
    assert logging.DEBUG == handler.level
    assert isinstance(handler, logging.handlers.TimedRotatingFileHandler) is True

    config['logging'].pop('log_rotation_days')
    LOGGER, handler = setup_logging(config, config_fname)
    assert handler.interval == 60 * 60 * 24
    assert handler.backupCount == 30
