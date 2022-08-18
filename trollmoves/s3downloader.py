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
        Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config
        self.subscr = None
        self.command_name = None  # command_name
        self.subscribe_nameserver = subscribe_nameserver

    def stop(self):
        """Stops the file listener"""
        LOGGER.debug("Entering stop in FileListener ...")
        self.loop = False
        self.queue.put(None)

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
                    if not self.loop:
                        # LOGGER.debug("Self.loop false in FileListener {}".format(self.loop))
                        break

                    # LOGGER.debug("Before checking message.")
                    # Check if it is a relevant message:
                    if self.check_message(msg):
                        LOGGER.info("Put the message on the queue...")
                        LOGGER.debug("Message = " + str(msg))
                        msg_data = {}
                        msg_data['config'] = self.config
                        msg_data['msg'] = msg
                        msg_data['command_name'] = self.command_name
                        self.queue.put(msg_data)
                        LOGGER.debug("After queue put.")
                    # else:
                    #     LOGGER.warning("check_message returned False for some reason. Message is: %s", str(msg))

        except KeyError as ke:
            LOGGER.info("Some key error. probably in config:", ke)
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

    """A publisher for result files. Picks up the return value from the
    run_command when ready, and publishes the files via posttroll"""

    def __init__(self, queue, nameservers):
        Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.service_name = 's3downloader'
        self.nameservers = nameservers

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        try:
            self.loop = True
            LOGGER.debug("Using service_name: {} with nameservers {}".format(self.service_name, self.nameservers))
            with Publish(self.service_name, 0, nameservers=self.nameservers) as publisher:

                while self.loop:
                    retv = self.queue.get()

                    if retv is not None:
                        LOGGER.info("Publish as service: %s", self.service_name)
                        LOGGER.info("Publish the files...")
                        publisher.send(retv)

        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Shutting down")


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
    LOGGER = logging.getLogger('pytroll-run-command')

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
    except botocore.exceptions.ClientError as ex:
        LOGGER.exception("S3 download failed with", str(ex))
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
        msg_data = subscribe_queue.get()
    except KeyboardInterrupt:
        return False
    if msg_data is None:
        LOGGER.debug("msg is none ... ")
        return True
    LOGGER.debug("Read from queue ... ")
    msg = msg_data['msg']
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