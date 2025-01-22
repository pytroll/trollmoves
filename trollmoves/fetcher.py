"""Fetch files from other (remote) filesystems."""

import argparse
import logging
from contextlib import closing
from pathlib import Path
from urllib.parse import unquote

import yaml
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config
from pytroll_watchers.fetch import fetch_file

from trollmoves.logging import add_logging_options_to_parser, setup_logging

logger = logging.getLogger(__name__)


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
                logger.info(f"Fetching from {str(message)}")
                downloaded_file = fetch_from_message(message, destination)
                message.data.pop("filesystem", None)
                message.data.pop("path", None)
                message.data["uri"] = unquote(downloaded_file.as_uri())
                pub.send(str(message))
                logger.info(f"Published {str(message)}")


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
