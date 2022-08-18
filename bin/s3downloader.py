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
import queue
import logging
import argparse

from trollmoves.s3downloader import read_config, setup_logging
from trollmoves.s3downloader import Listener, FilePublisher
from trollmoves.s3downloader import read_from_queue

LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":

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
    cmd_args = parser.parse_args()

    config = None
    if os.path.exists(cmd_args.config_file):
        config = read_config(cmd_args.config_file, debug=False)

    # Set up logging
    try:
        LOGGER, handler = setup_logging(config, cmd_args.log)
    except Exception:
        print("Logging setup failed. Check your config")
        raise

    LOGGER.info("Starting up.")

    listener_queue = queue.Queue()
    publisher_queue = queue.Queue()

    listener = Listener(listener_queue, config, subscribe_nameserver=cmd_args.subscribe_nameserver)
    listener.start()

    publisher = FilePublisher(publisher_queue, cmd_args.nameservers)
    publisher.start()

    read_from_queue(listener_queue, publisher_queue, config)

    listener.stop()
    publisher.stop()
    LOGGER.info("Exiting s3downloader.")
