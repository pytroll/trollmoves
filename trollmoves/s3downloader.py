#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Pytroll developers
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
import queue
import argparse
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
        self.queue.put("STOP")

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


class s3downloader():

    def __init__(self, cmd_args):
        self.config = None
        self.cmd_args = cmd_args
        self.listener_queue = queue.Queue()
        self.publisher_queue = queue.Queue()
        self.listener = None
        self.publisher = None

    def _stop(self):
        self.listener.stop()
        self.publisher.stop()
        LOGGER.info("Exiting s3downloader.")

    def start(self):
        LOGGER.info("Starting up.")
        self.listener = Listener(self.listener_queue, self.config,
                                 subscribe_nameserver=self.cmd_args.subscribe_nameserver)
        self.listener.start()

        self.publisher = FilePublisher(self.publisher_queue, self.cmd_args.nameservers)
        self.publisher.start()

        self._read_from_queue()

        self._stop()

    def read_config(self, debug=True):
        """Read the config file from cmd args.
        """
        if self.cmd_args.config_file and os.path.exists(self.cmd_args.config_file):
            with open(self.cmd_args.config_file, 'r') as stream:
                try:
                    self.config = yaml.safe_load(stream)
                    if debug:
                        import pprint
                        pp = pprint.PrettyPrinter(indent=4)
                        pp.pprint(self.config)
                except FileNotFoundError:
                    print("Could not find you config file:", self.cmd_args.config_file)
                    raise
                except yaml.YAMLError as exc:
                    print("Failed reading yaml config file: {} with: {}".format(self.cmd_args.config_file, exc))
                    raise yaml.YAMLError
        else:
            raise FileNotFoundError(self.cmd_args.config_file)

        return self.config

    def setup_logging(self):
        """
        Init and setup logging
        """
        # Set up logging
        try:
            loglevel = logging.INFO
            if self.cmd_args.log and 'logging' in self.config:
                ndays = 1
                ncount = 30
                try:
                    ndays = int(self.config['logging']["log_rotation_days"])
                    ncount = int(self.config['logging']["log_rotation_backup"])
                except KeyError:
                    pass

                handler = handlers.TimedRotatingFileHandler(self.cmd_args.log,
                                                            when='midnight',
                                                            interval=ndays,
                                                            backupCount=ncount,
                                                            encoding=None,
                                                            delay=False,
                                                            utc=True)

                handler.doRollover()
            else:
                handler = logging.StreamHandler(sys.stderr)

            if 'logging_mode' in self.config['logging'] and self.config['logging']["logging_mode"] == "DEBUG":
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
        except Exception:
            print("Logging setup failed. Check your config")
            raise

        return LOGGER, handler

    def _get_basename(self, uri):
        up = urlparse(uri)
        bn = os.path.basename(up.path)
        return bn

    def _download_from_s3(self, bn):
        try:
            s3 = boto3.client('s3', endpoint_url=self.config['endpoint_url'],
                              aws_access_key_id=self.config['access_key'],
                              aws_secret_access_key=self.config['secret_key'])
            s3.download_file(self.config['bucket'], bn, os.path.join(self.config.get('download_destination', '.'), bn))
        except botocore.exceptions.ClientError:
            LOGGER.exception("S3 download failed.")
            return False
        return True

    def _generate_message_if_file_exists_after_download(self, bn, msg):
        if os.path.exists(os.path.join(self.config.get('download_destination', '.'), bn)):
            LOGGER.debug("Successfully downloaded file %s to %s", bn, self.config.get('download_destination', '.'))
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
            to_send['uri'] = os.path.join(self.config.get('download_destination', '.'), bn)

            pubmsg = Message(self.config['publish-topic'], "file", to_send).encode()
            return pubmsg
        return None

    def _get_one_message(self):
        LOGGER.debug("Start reading from queue ... ")
        try:
            msg = self.listener_queue.get()
        except KeyboardInterrupt:
            return False
        if msg is None:
            LOGGER.debug("msg is none ... ")
            return True
        LOGGER.debug("Read from queue ... ")
        LOGGER.debug("Read from queue: {}".format(msg))
        bn = self._get_basename(msg.data['uri'])
        if self._download_from_s3(bn):
            pubmsg = self._generate_message_if_file_exists_after_download(bn, msg)
            LOGGER.info("Sending: " + str(pubmsg))
            self.publisher_queue.put(pubmsg)
        else:
            LOGGER.error("Could not download file %s for some reason. SKipping this.", bn)
        return True

    def _read_from_queue(self):
        # read from queue
        running = True
        while running:
            if not self._get_one_message():
                running = False


def parse_args(args):
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config-file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-r", "--subscribe-nameserver",
                        type=str,
                        dest='subscribe_nameserver',
                        default="localhost",
                        help="subscribe nameserver, defaults to localhost")
    parser.add_argument("-n", "--nameservers",
                        type=str,
                        dest='nameservers',
                        default=None,
                        nargs='*',
                        help="nameservers, defaults to localhost")
    return parser.parse_args(args)
