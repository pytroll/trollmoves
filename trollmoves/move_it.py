"""Script for moving and unpacking files.

This program is comprised of two parts: this script and the configuration file.

The usage of this script is quite straightforward, just call it with the name
of the configuration file as argument.

The configuration file is comprised of sections describing the chain of
moving/unpacking.

Installation
------------

This scripts needs pyinotify and argparse which are available on pypi, or via
pip/easy_install (on redhat systems, install the packages python-inotify.noarch
and python-argparse.noarch).  Other than this, the script doesn't need any
installation, and can be run as is. If you wish though, you can install it to
your standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  origin=/eumetcast/received/H-000-{series:_<6s}-{platform_name:_<12s}-{channel:_<9s}-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-__
  destinations=/eumetcast/unpacked/
  compression=xrit
  delete=False
  prog=/local_disk/usr/src/PublicDecompWT/Image/Linux_32bits/xRITDecompress
  topic=/HRIT/L0/dev
  info=sensors=seviri;stream=eumetcast
  publish_port=

* 'origin' is the directory and pattern of files to watch for. For a description
  of the pattern format, see the trollsift documentation:
  http://trollsift.readthedocs.org/en/latest/index.html

* 'destinations' is the list of places to put the unpacked data in. The
  protocols supported at the moment are file and ftp, so the following are
  valid::

     destinations=/tmp/unpack/ file:///tmp/unpack ftp://user:passwd@server.smhi.se/tmp/unpack



* 'working_directory' is telling where to unpack the files before they are put
  in their final destination. This can come in handy in case the file has to be
  transfered by ftp and cannot be unpacked in the origin directory. The default
  for this parameter is the '/tmp' directory.

* Available compressions are 'xrit' and 'bzip'.

* The prog parameter is used for the 'xrit' unpacking function to know which
  external program to call for unpack xRIT files.

  .. note:: The 'xrit' unpacking function is dependent on a program that can
    unpack xRIT files. Such a program is available from the `Eumetsat.int
    <http://www.eumetsat.int/Home/Main/DataAccess/SupportSoftwareTools/index.htm?l=en>`_
    website.

* The delete parameters tells if a file needs to be deleted after
  unpacking. This defaults to False.

* 'topic', 'publish_port', and 'info' define the messaging behaviour using
  posttroll, if these options are given, 'info' being a ';' separated list of
  'key=value' items that has to be added to the message info.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout) w
ith the -l or --log option::

  move_it --log /path/to/mylogfile.log myconfig.ini

Operations
----------

This scripts operates with `inotify`, so that the watched files are handled as
soon as an `in_close_write` event occurs. This means that the incomming
directory has to be watchable with `inotify`, so that rules out NFS partitions
for example.

The configuration file is also watched in this way, so the same rules
apply. The reason for this is that the changes done in the configuration file
are applied as soon as the file is saved.

Each section of the configuration file is describing an chain of
processing. Each chain is executed in an independant thread so that problems in
one chain will not propagate to the other chains.

When the script is launched, or when a section is added or updated, existing
files matching the pattern defined in `origin` are touched in order to create a
triggering of the chain on them. To avoid any race conditions, chain is
executed tree seconds after the list of file is gathered.
"""

import logging
import logging.handlers
import os
from functools import partial
from urllib.parse import urlparse

from trollmoves.logging import setup_logging
from trollmoves.move_it_base import create_publisher
from trollmoves.movers import MOVERS
from trollmoves.server import AbstractMoveItServer, create_message_with_request_info, parse_args, unpack

LOGGER = logging.getLogger(__name__)


def main():
    """Start the server."""
    cmd_args = parse_args(default_port=None)
    logger = setup_logging("move_it", cmd_args)
    move_it_thread = MoveItSimple(cmd_args)

    try:
        move_it_thread.reload_cfg_file(cmd_args.config_file)
        move_it_thread.run()
    except KeyboardInterrupt:
        logger.debug("Stopping Move It")
    finally:
        if move_it_thread.running:
            move_it_thread.chains_stop()


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
