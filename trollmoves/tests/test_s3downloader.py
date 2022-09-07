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

import argparse
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
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    parse = parse_args(['--config-file=' + config_yaml])
    s3dl = s3downloader(parse)
    config = s3dl.read_config(debug=False)
    expected_config = {'logging': {'log_rotation_days': 1, 'log_rotation_backup': 30, 'logging_mode': 'DEBUG'},
                       'subscribe-topic': ['/yuhu'], 'publish-topic': '/idnt', 'endpoint_url': 'https://your.url.space',
                       'access_key': 'your_access_key',
                       'secret_key': 'your_secret_key',
                       'bucket': 'atms-sdr',
                       'download_destination': '/destination-directory'}
    assert config == expected_config


def test_read_config_debug(capsys, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    parse = parse_args(['--config-file=' + config_yaml])
    s3dl = s3downloader(parse)
    s3dl.read_config()
    captured = capsys.readouterr()
    assert '/destination-directory' in captured.out


@patch('yaml.safe_load')
def test_read_config_exception(patch_yaml, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    parse = parse_args(['--config-file=' + config_yaml])
    s3dl = s3downloader(parse)
    patch_yaml.side_effect = FileNotFoundError
    with pytest.raises(FileNotFoundError):
        s3dl.read_config(debug=False)


@patch('yaml.safe_load')
def test_read_config_exception2(patch_yaml, config_yaml):
    """Test read yaml config."""
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    parse = parse_args(['--config-file=' + config_yaml])
    s3dl = s3downloader(parse)
    import yaml
    patch_yaml.side_effect = yaml.YAMLError
    with pytest.raises(yaml.YAMLError):
        s3dl.read_config(debug=False)


@pytest.fixture
def s3dl(config_yaml):
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    parse = parse_args(['--config-file=' + config_yaml])
    return s3downloader(parse)


def test_get_basename(s3dl):
    uri = os.path.join("root", "anypath", "filename-basename")
    bn = s3dl._get_basename(uri)
    assert bn == 'filename-basename'


@patch('os.path.exists')
def test_generate_message_if_file_exists_after_download(patch_os_path_exists, s3dl):
    s3dl.read_config(debug=False)
    bn = 'filename-basename'
    to_send = {'some_key': 'with_a_value'}
    msg = Message('/publish-topic', "file", to_send)
    patch_os_path_exists.return_value = True
    pubmsg = s3dl._generate_message_if_file_exists_after_download(bn, msg)
    assert 'with_a_value' in pubmsg


@patch('os.path.exists')
def test_generate_message_if_file_does_not_exists_after_download(patch_os_path_exists, s3dl):
    s3dl.read_config(debug=False)
    bn = 'filename-basename'
    to_send = {'some_key': 'with_a_value'}
    msg = Message('/publish-topic', "file", to_send)
    patch_os_path_exists.return_value = False
    pubmsg = s3dl._generate_message_if_file_exists_after_download(bn, msg)
    assert pubmsg is None


@patch('trollmoves.s3downloader.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader.s3downloader._get_basename')
@patch('queue.Queue')
def test_get_one_message(patch_subscribe, patch_get_basename, patch_download_from_s3, s3dl):
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    to_send = {'some_key': 'with_a_value', 'uri': 'now-this-is-a-uri'}
    msg = Message('/publish-topic', "file", to_send)
    print("LQ", s3dl.listener_queue)
    s3dl.listener_queue = patch_subscribe
    s3dl.listener_queue.get.return_value = msg
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = s3dl._get_one_message()
    assert result is True


@patch('trollmoves.s3downloader.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader.s3downloader._get_basename')
@patch('queue.Queue')
def test_get_one_message_none(patch_sub_q, patch_get_basename, patch_download_from_s3, s3dl):
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    s3dl.listener_queue = patch_sub_q
    s3dl.listener_queue.get.return_value = None
    patch_get_basename.return_value = 'filename-basename'
    patch_download_from_s3.return_value = True
    result = s3dl._get_one_message()
    assert result is True


@patch('trollmoves.s3downloader.s3downloader._download_from_s3')
@patch('trollmoves.s3downloader.s3downloader._get_basename')
@patch('queue.Queue')
def test_get_one_message_download_false(patch_sub_q, patch_get_bn, patch_dl_s3, caplog, s3dl):
    import logging
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    patch_get_bn.return_value = 'filename-basename'
    patch_dl_s3.return_value = False
    caplog.set_level(logging.DEBUG)
    s3dl.listener_queue = patch_sub_q
    result = s3dl._get_one_message()
    assert 'Could not download file filename-basename for some reason. SKipping this.' in caplog.text
    assert result is True


@patch('queue.Queue')
def test_get_one_message_keyboardinterrupt(patch_subscribe, s3dl):
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    s3dl.listener_queue = patch_subscribe
    s3dl.listener_queue.get.side_effect = KeyboardInterrupt
    result = s3dl._get_one_message()
    assert result is False


@patch('trollmoves.s3downloader.s3downloader._get_one_message')
def test_read_from_queue(patch_get_one_message, s3dl):
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    patch_get_one_message.return_value = False
    s3dl._read_from_queue()
    # TODO: what does this tests?


@patch('boto3.client')
def test_download_from_s3(patch_boto3_client, s3dl):
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    bn = 'filename-basename'
    result = s3dl._download_from_s3(bn)
    assert result is True


@patch('boto3.client')
def test_download_from_s3_exception(patch_boto3_client, s3dl):
    import botocore
    s3dl.read_config(debug=False)
    s3dl.setup_logging()
    bn = 'filename-basename'
    error_response = {'Error': {'Code': 'TEST',
                                'Message': 'TEST MESSAGE',
                                }
                      }
    patch_boto3_client.return_value.download_file.side_effect = botocore.exceptions.ClientError(
        error_response=error_response, operation_name='test')
    result = s3dl._download_from_s3(bn)
    assert result is False


def test_setup_logging(s3dl):
    import logging
    s3dl.read_config(debug=False)

    LOGGER, handler = s3dl.setup_logging()
    assert isinstance(LOGGER, logging.Logger) is True
    assert logging.DEBUG == handler.level
    assert isinstance(handler, StreamHandler) is True


def test_setup_logging_file(config_yaml):
    import logging
    from trollmoves.s3downloader import parse_args
    from trollmoves.s3downloader import s3downloader
    with NamedTemporaryFile('w', delete=False) as fid:
        config_fname = fid.name
    parse = parse_args(['--config-file=' + config_yaml, '-l=' + config_fname])
    s3dl = s3downloader(parse)
    s3dl.read_config(debug=False)

    LOGGER, handler = s3dl.setup_logging()
    assert isinstance(LOGGER, logging.Logger) is True
    assert logging.DEBUG == handler.level
    assert isinstance(handler, logging.handlers.TimedRotatingFileHandler) is True

    s3dl.config['logging'].pop('log_rotation_days')
    LOGGER, handler = s3dl.setup_logging()
    assert handler.interval == 60 * 60 * 24
    assert handler.backupCount == 30


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
    assert message is "STOP"


@patch('trollmoves.s3downloader.Publish')
@patch('queue.Queue')
def test_file_publisher_exception_1(patch_publish_queue, patch_publish):
    from trollmoves.s3downloader import FilePublisher
    nameservers = None
    patch_publish_queue.get.side_effect = KeyboardInterrupt
    fp = FilePublisher(patch_publish_queue, nameservers)
    with pytest.raises(KeyboardInterrupt):
        fp.run()


posttroll_config = {'subscribe-topic': '/yuhu'}


@patch('queue.Queue')
def test_listener_init(patch_listener_queue):
    from trollmoves.s3downloader import Listener
    subscribe_nameserver = 'localhost'
    listenr = Listener(patch_listener_queue, posttroll_config, subscribe_nameserver)
    assert listenr.loop is True
    assert listenr.queue == patch_listener_queue
    assert listenr.config == posttroll_config
    assert listenr.subscribe_nameserver == subscribe_nameserver


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message(patch_get_pub_address, patch_subscriber, caplog):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import logging
    import queue
    subscribe_nameserver = 'localhost'
    caplog.set_level(logging.DEBUG)

    patch_subscriber.return_value.recv = PropertyMock(side_effect=[[MSG_1, None], ])
    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)
    listener.run()

    assert 'Put the message on the queue...' in caplog.text
    assert lqueue.qsize() == 1

    message = lqueue.get()
    assert message.type == 'file'


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
@patch('queue.Queue')
def test_listener_message_break(patch_listener_queue, patch_get_pub_address, patch_subscriber, caplog):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import logging
    caplog.set_level(logging.DEBUG)
    subscribe_nameserver = 'localhost'

    patch_subscriber.return_value.recv = PropertyMock(side_effect=[[MSG_1, None], ])
    listener = Listener(patch_listener_queue, posttroll_config, subscribe_nameserver)
    listener.loop = False
    listener.run()
    patch_listener_queue().put.assert_not_called()


MSG_ACK = Message('/topic', 'ack', data={'uid': 'file1'})


def test_listener_message_check_message():
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)

    assert listener.check_message(None) is False
    assert listener.check_message(MSG_ACK) is False
    assert listener.check_message(MSG_1) is True


def test_listener_message_stop():
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)

    listener.stop()
    assert listener.loop is False
    assert listener.queue.qsize() == 1
    message = lqueue.get()
    assert message is None


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_check_config(patch_get_pub_address, patch_subscriber):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    posttroll_config['subscribe-topic'] = 'is-a-string-topic'
    posttroll_config['subscriber_addresses'] = 'first_address, second_address'
    subscribe_nameserver = 'localhost'

    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)
    listener.run()
    assert isinstance(listener.config["subscribe-topic"], list) is True
    assert listener.config["services"] == ''


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_check_message_and_put(patch_get_pub_address, patch_subscriber):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    posttroll_config['subscribe-topic'] = 'is-a-string-topic'
    posttroll_config['subscriber_addresses'] = 'first_address, second_address'
    subscribe_nameserver = 'localhost'

    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)
    assert listener._check_and_put_message_to_queue(MSG_1) is True
    assert listener._check_and_put_message_to_queue(None) is True

    listener.loop = False
    assert listener._check_and_put_message_to_queue(MSG_1) is False


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_exception_1(patch_get_pub_address, patch_subscriber):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)
    patch_subscriber.side_effect = KeyError
    with pytest.raises(KeyError):
        listener.run()


@patch('posttroll.subscriber.Subscriber')
@patch('posttroll.subscriber.get_pub_address')
def test_listener_message_exception_2(patch_get_pub_address, patch_subscriber):
    """Test listener push message."""
    from trollmoves.s3downloader import Listener
    import queue
    subscribe_nameserver = 'localhost'
    lqueue = queue.Queue()
    listener = Listener(lqueue, posttroll_config, subscribe_nameserver)
    patch_subscriber.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        listener.run()
