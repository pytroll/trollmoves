#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012, 2013, 2014, 2015
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
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

"""
Script for moving and unpacking files.

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

from trollmoves.logging import setup_logging
from trollmoves.move_it import MoveItSimple
from trollmoves.server import parse_args

LOGGER = logging.getLogger("move_it")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"


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


if __name__ == '__main__':
    main()
