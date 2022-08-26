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

"""s3 downloader to be used together with s3stalker from pytroll-collectors.
S3downloader listens to messages from the s3stalker, and download files to
configured destination. If the download is successful(ie. file exists on local disk
a message is published to be used further downstream.

A yaml config file is needed like this:
---
logging:
  log_rotation_days: 1
  log_rotation_backup: 30
  logging_mode: DEBUG

subscribe-topic:
  - /yuhu
publish-topic: /idnt
endpoint_url: '<your object store endpoint url'
access_key: ''
secret_key: ''
bucket: <name of the bucket>  # Not needed, else used from the message uri
download_destination: './'

"""

import os
import sys
import yaml
import boto3
import logging
import botocore
from logging import handlers
from threading import Thread
from urllib.parse import urlparse
from posttroll.message import Message
from posttroll.publisher import Publish
from posttroll.subscriber import Subscribe

LOGGER = logging.getLogger(__name__)
# ----------------------------
# Default settings for logging
# ----------------------------
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


class Listener(Thread):

    def __init__(self, queue, config, subscribe_nameserver):
        # Thread.__init__(self)
        super(Listener, self).__init__()
        self.loop = True
        self.queue = queue
        self.config = config
        self.subscribe_nameserver = subscribe_nameserver

    def stop(self):
        """Stops the file listener"""
        LOGGER.debug("Entering stop in FileListener ...")
        self.loop = False
        self.queue.put(None)

    def _check_and_put_message_to_queue(self, msg):
        if not self.loop:
            LOGGER.warning("Self.loop false in FileListener %s", self.loop)
            return False

        # Check if it is a relevant message:
        if self.check_message(msg):
            LOGGER.info("Put the message on the queue...")
            LOGGER.debug("Message = " + str(msg))
            self.queue.put(msg)
            LOGGER.debug("After queue put.")
        return True

    def run(self):
        LOGGER.debug("Entering run in FileListener ...")
        if type(self.config["subscribe-topic"]) not in (tuple, list, set):
            self.config["subscribe-topic"] = [self.config["subscribe-topic"]]
        try:
            if 'services' not in self.config:
                self.config['services'] = ''
            subscriber_addresses = None
            if 'subscriber_addresses' in self.config:
                subscriber_addresses = self.config['subscriber_addresses'].split(',')

            with Subscribe(self.config['services'], self.config['subscribe-topic'],
                           True, addresses=subscriber_addresses,
                           nameserver=self.subscribe_nameserver) as subscr:

                LOGGER.debug("Entering for loop subscr.recv")
                for msg in subscr.recv(timeout=1):
                    if not self._check_and_put_message_to_queue(msg):
                        break

        except KeyError as ke:
            LOGGER.info("Some key error. probably in config: %s", str(ke))
            raise
        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Shutting down.")
            raise

    def check_message(self, msg):

        if not msg:
            # LOGGER.debug("message is None")
            return False
        if msg.type not in ('file', 'collection', 'dataset'):
            LOGGER.debug("message type is not valid %s", str(msg.type))
            return False
        return True


class FilePublisher(Thread):

    """A publisher for result files listening to a publish queue.
    Publishes the files via posttroll"""

    def __init__(self, queue, nameservers):
        Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.service_name = 's3downloader'
        self.nameservers = nameservers

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def _publish_message(self, retv, publisher):
        if not self.loop:
            return False
        if retv is not None:
            LOGGER.info("Publish as service: %s", self.service_name)
            LOGGER.info("Publish the files...")
            publisher.send(retv)
        return True

    def run(self):
        try:
            LOGGER.debug("Using service_name: {} with nameservers {}".format(self.service_name, self.nameservers))
            with Publish(self.service_name, 0, nameservers=self.nameservers) as publisher:
                for retv in self.queue.get():
                    if not self._publish_message(retv, publisher):
                        break

        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Shutting down")
            raise

# Config management


def read_config(filename, debug=True):
    """Read the config file called *filename*.
    """
    with open(filename, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            if debug:
                import pprint
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(config)
        except FileNotFoundError:
            print("Could not find you config file:", filename)
            raise
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    return config


def setup_logging(config, log_file):
    """
    Init and setup logging
    """
    loglevel = logging.INFO
    if log_file and 'logging' in config:
        ndays = 1
        ncount = 30
        try:
            ndays = int(config['logging']["log_rotation_days"])
            ncount = int(config['logging']["log_rotation_backup"])
        except KeyError:
            pass

        handler = handlers.TimedRotatingFileHandler(os.path.join(log_file),
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    if 'logging_mode' in config['logging'] and config['logging']["logging_mode"] == "DEBUG":
        loglevel = logging.DEBUG

    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)

    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('posttroll').setLevel(loglevel)
    logging.getLogger('botocore').setLevel(loglevel)
    logging.getLogger('s3transfer').setLevel(loglevel)
    logging.getLogger('urllib3').setLevel(loglevel)
    LOGGER = logging.getLogger('S3 downloader')

    return LOGGER, handler


def _get_basename(uri):
    up = urlparse(uri)
    bn = os.path.basename(up.path)
    return bn


def _download_from_s3(config, bn):
    try:
        s3 = boto3.client('s3', endpoint_url=config['endpoint_url'],
                          aws_access_key_id=config['access_key'],
                          aws_secret_access_key=config['secret_key'])
        s3.download_file(config['bucket'], bn, os.path.join(config.get('download_destination', '.'), bn))
    except botocore.exceptions.ClientError:
        LOGGER.exception("S3 download failed.")
        return False
    return True


def _generate_message_if_file_exists_after_download(config, bn, msg):
    if os.path.exists(os.path.join(config.get('download_destination', '.'), bn)):
        LOGGER.debug("Successfully downloaded file %s to %s", bn, config.get('download_destination', '.'))
        to_send = msg.data.copy()
        to_send.pop('dataset', None)
        to_send.pop('collection', None)
        to_send.pop('filename', None)
        to_send.pop('compress', None)
        to_send.pop('tst', None)
        to_send.pop('uri', None)
        to_send.pop('uid', None)
        to_send.pop('file_list', None)
        to_send.pop('path', None)
        to_send['uri'] = os.path.join(config.get('download_destination', '.'), bn)

        pubmsg = Message(config['publish-topic'], "file", to_send).encode()
        return pubmsg
    return None


def _get_one_message(config, subscribe_queue, publish_queue):
    LOGGER.debug("Start reading from queue ... ")
    try:
        msg = subscribe_queue.get()
    except KeyboardInterrupt:
        return False
    if msg is None:
        LOGGER.debug("msg is none ... ")
        return True
    LOGGER.debug("Read from queue ... ")
    LOGGER.debug("Read from queue: {}".format(msg))
    bn = _get_basename(msg.data['uri'])
    if _download_from_s3(config, bn):
        pubmsg = _generate_message_if_file_exists_after_download(config, bn, msg)
        LOGGER.info("Sending: " + str(pubmsg))
        publish_queue.put(pubmsg)
    else:
        LOGGER.error("Could not download file %s for some reason. SKipping this.", bn)
    return True


def read_from_queue(subscribe_queue, publish_queue, config):
    # read from queue
    running = True
    while running:
        if not _get_one_message(config, subscribe_queue, publish_queue):
            running = False
