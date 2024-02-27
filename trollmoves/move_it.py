"""Main module for the standalone move_it."""

import logging
import os
from functools import partial
from urllib.parse import urlparse

from trollmoves.move_it_base import create_publisher
from trollmoves.movers import MOVERS
from trollmoves.server import (AbstractMoveItServer,
                               create_message_with_request_info, unpack)

LOGGER = logging.getLogger(__name__)


class MoveItSimple(AbstractMoveItServer):
    """Wrapper class for Move It."""

    def __init__(self, cmd_args):
        """Initialize server."""
        self.name = "move_it"
        self.publisher = create_publisher(cmd_args.port, self.name)
        super().__init__(cmd_args, self.publisher)
        self.request_manager = None
        self.function_to_run_on_matching_files = partial(process_notify, publisher=self.publisher)

    def reload_cfg_file(self, filename):
        """Reload configuration file."""
        self.reload_config(filename,
                           disable_backlog=self.cmd_args.disable_backlog)

    def signal_reload_cfg_file(self, *args):
        """Handle reload signal."""
        del args
        self.reload_cfg_file(self.cmd_args.config_file)


def publish_hook(pathname, dest_url, config, publisher):
    """Publish hook for move_it."""
    dest = os.path.join(dest_url.path, os.path.basename(pathname))
    msg = create_message_with_request_info(dest, pathname, config)
    publisher.send(str(msg))


def process_notify(pathname, publisher, chain_config):
    """Execute unpacking and copying/moving of *pathname*."""
    LOGGER.info("We have a match: %s", str(pathname))
    new_path = unpack(pathname, **chain_config)
    try:
        if publisher is not None:
            publisher_hook = partial(publish_hook, publisher=publisher, config=chain_config)
        else:
            publisher_hook = None
        move_it(new_path, chain_config["destinations"].split(), publisher_hook)
    except Exception:
        LOGGER.error("Something went wrong during copy of %s",
                     pathname)
    else:
        if chain_config["delete"]:
            try:
                os.remove(pathname)
                if chain_config["delete_hook"]:
                    chain_config["delete_hook"](pathname)
                LOGGER.debug("Removed %s", pathname)
            except OSError as e__:
                if e__.errno == 2:
                    LOGGER.debug("Already deleted: %s", pathname)
                else:
                    raise

    # delete temporary file
    if pathname != new_path:
        try:
            os.remove(new_path)
        except OSError as e__:
            if e__.errno == 2:
                pass
            else:
                raise


def move_it(pathname, destinations, hook=None):
    """Check if the file pointed by *filename* is in the filelist, and move it if it is."""
    err = None
    for dest in destinations:
        LOGGER.debug("Copying to: %s", dest)
        dest_url = urlparse(dest)
        try:
            mover = MOVERS[dest_url.scheme]
        except KeyError:
            LOGGER.error("Unsupported protocol '%s'. Could not copy %s to %s",
                         str(dest_url.scheme), pathname, str(dest))
            continue
        try:
            mover(pathname, dest_url).copy()
            if hook:
                hook(pathname, dest_url)
        except Exception:
            LOGGER.exception("Something went wrong during copy of %s to %s",
                             pathname, str(dest))
            continue
        else:
            LOGGER.info("Successfully copied %s to %s", pathname,
                        str(dest))

    if err is not None:
        raise err
