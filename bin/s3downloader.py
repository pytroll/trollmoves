#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022-2023 Pytroll Developers
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

import sys
import logging

from trollmoves.s3downloader import parse_args
from trollmoves.s3downloader import S3Downloader

LOGGER = logging.getLogger(__name__)


def main():
    cmd_args = parse_args(sys.argv[1:])

    s3dl = S3Downloader(cmd_args)
    s3dl.read_config()
    s3dl.setup_logging()
    s3dl.start()


if __name__ == "__main__":
    main()
