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

import logging
import sys

from trollmoves.s3downloader import S3Downloader, parse_args

LOGGER = logging.getLogger(__name__)


def main():
    cmd_args = parse_args(sys.argv[1:])

    s3dl = S3Downloader(cmd_args)
    s3dl.read_config()
    s3dl.setup_logging()
    s3dl.start()


if __name__ == "__main__":
    main()
