#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012, 2013, 2014, 2015, 2016
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

"""Trollmoves Server.

Moving and unpacking files
==========================

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
  origin=/tmp/H-000-{series:_<6s}-{platform_name:_<12s}-{channel:_<9s}-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-{compression:1s}_
  publisher_port=9011
  compression=xrit
  prog=/home/a001673/usr/src/PublicDecompWT/Image/Linux_32bits/xRITDecompress
  topic=/1b/hrit/zds
  info=sensor=seviri;sublon=0
  request_port=9092
  working_directory=/tmp/unpacked

* 'origin' is the directory and pattern of files to watch for. For a description
  of the pattern format, see the trollsift documentation:
  http://trollsift.readthedocs.org/en/latest/index.html

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

* 'topic', 'publish_port', and 'info' define the messaging behaviour using posttroll. 'info' being a ';' separated
  list of 'key=value' items that has to be added to the message info.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a logging config file with the -c
or --log-config option::

  move_it_server --log-config /path/to/mylogconfig.yaml myconfig.ini
"""
from trollmoves.logging import setup_logging
from trollmoves.server import MoveItServer, parse_args


def main():
    """Start the server."""
    cmd_args = parse_args()
    logger = setup_logging("move_it_server", cmd_args)
    server = MoveItServer(cmd_args)

    try:
        server.reload_cfg_file(cmd_args.config_file)
        server.run()
    except KeyboardInterrupt:
        logger.debug("Stopping Trollmoves server")
    finally:
        if server.running:
            server.chains_stop()


if __name__ == '__main__':
    main()
