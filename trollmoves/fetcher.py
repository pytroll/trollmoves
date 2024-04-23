"""Fetch files from other (remote) filesystems."""

import argparse
import json
import logging
import os
from contextlib import closing
from pathlib import Path

import fsspec
import yaml
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config

from trollmoves.logging import add_logging_options_to_parser, setup_logging

logger = logging.getLogger(__name__)


def fetch_file(file_to_fetch, download_dir, filesystem=None):
    """Fetch a file.

    Args:
        file_to_fetch: The Path of the file to fetch. Can be a UPath form universal_path.
        download_dir: The directory to download the file to.
        filesystem: The file system to use if provided. Should be a dictionary that will be fed to fsspec.

    Returns:
        The Path to the downloaded file.

    Example:

        >>> fetch_file("https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-FLDK/2017/02/02/0020/HS_H08_20170202_0020_B01_FLDK_R10_S0101.DAT.bz2", "/tmp/")
        ... # HS_H08_20170202_0020_B01_FLDK_R10_S0101.DAT.bz2 is now present in /tmp/


    """  # noqa
    download_dir = Path(download_dir)

    if filesystem:
        downloaded_file = _fetch_from_filesystem(file_to_fetch, download_dir, filesystem)
    else:
        downloaded_file = _fetch_from_uri(file_to_fetch, download_dir)
    logger.info(f"Fetched {str(downloaded_file)}")
    return downloaded_file


def _fetch_from_uri(file_to_fetch, download_dir):
    """Fetch a file from a uri."""
    fs_file = fsspec.open(file_to_fetch)
    filesystem = fs_file.fs
    basename = os.path.basename(fs_file.path)
    downloaded_file = download_dir / basename
    filesystem.get_file(fs_file.path, download_dir / basename)
    return downloaded_file


def _fetch_from_filesystem(path_to_fetch, download_dir, fs):
    """Fetch a file from a path and a filesystem specification."""
    filesystem = fsspec.AbstractFileSystem.from_json(json.dumps(fs))
    basename = os.path.basename(path_to_fetch)
    downloaded_file = download_dir / basename
    filesystem.get_file(path_to_fetch, download_dir / basename)
    return downloaded_file


def fetch_from_message(message, destination):
    """Fetch a file provided in a message.

    Args:
        message: A posttroll message instance with information about the files to fetch.
        destination: the directory to save the files to.

    Returns:
        The path to the downloaded file.
    """
    try:
        return fetch_file(message.data["path"], destination, message.data["filesystem"])
    except KeyError:
        return fetch_file(message.data["uri"], destination)


def fetch_from_subscriber(destination, subscriber_config, publisher_config):
    """Fetch files published using a subscriber.

    Warning:
        At the moment, messages that are not of type `file` will be ignored.

    Args:
        destination: the directory to save the files to.
        subscriber_config: the settings for the subscriber. Will be passed on as is to posttroll's
            :py:func:`~posttroll.subscriber.create_subscriber_from_dict_config`
        publisher_config: the settings for the publisher. Will be passed on as is to posttroll's
            :py:func:`~posttroll.publisher.create_publisher_from_dict_config`

    """  # noqa
    destination = Path(destination)

    pub = create_publisher_from_dict_config(publisher_config)
    pub.start()
    with closing(pub):
        sub = create_subscriber_from_dict_config(subscriber_config)
        with closing(sub):
            for message in sub.recv():
                if message.type != "file":
                    continue
                downloaded_file = fetch_from_message(message, destination)
                message.data.pop("filesystem", None)
                message.data.pop("path", None)
                message.data["uri"] = downloaded_file.as_uri()
                pub.send(str(message))


def cli(args=None):
    """Command line argument for fetching published files."""
    parser = argparse.ArgumentParser(prog="pytroll-fetcher",
                                     description="Fetches files/objects advertised as messages.",
                                     epilog="Thanks for using pytroll-fetcher!")

    parser.add_argument("config", type=str, help="The yaml config file.")
    add_logging_options_to_parser(parser)

    parsed = parser.parse_args(args)

    setup_logging("pytroll-fetcher", parsed)

    config_file = parsed.config
    with open(config_file) as fd:
        config_dict = yaml.safe_load(fd.read())

    return fetch_from_subscriber(**config_dict)
