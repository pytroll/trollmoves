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

"""
Moving and unpacking files
==========================

This program is comprised of two parts: this script and the configuration file.

The usage of this script is quite straightforward, just call it with the name
of the configuration file as argument.

The configuration file is comprised of sections describing the chain of
moving/unpacking.

Installation
------------

This scripts needs pyinotify, argparse, posttroll and trollsift which are available on pypi, or via pip/easy_install
(on redhat systems, install the packages python-inotify.noarch and python-argparse.noarch).  Other than this,
the script doesn't need any installation, and can be run as is. If you wish though, you can install it to your
standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  providers=tellicast_server:9090
  destination=/the/directory/you/want/stuff/in /another/directory/you/want/stuff/in
  login=username:greatPassword
  topic=/1b/hrit/zds
  publish_port=0

  destination_subdir=%Y%m%d%H%M
  destination_size=20
  unpack=xrit
  unpack_prog=/path/to/xRITDecompress64
  procfile_log=/directory/you/want/processed_file_log.log
  procfile_log_size=22000
  use_ref_file=/the/directory/you/want/save/reference/file

* 'provider' is the address of the server receiving the data you want.

* 'destinations' is the list of places to put the unpacked data in.

* 'topic' gives the topic to listen to on the provider side.

* 'publish_port' defines on which port to publish incomming files. 0 means random port.

Others configuration values:

* 'destination_subdir' subdirectory inside 'destination' in which you want the unpacked data.
                       special strftime function is applied to destination_subdir

* 'destination_size' maximum number of destination subdirs can be created under destinations

*  'unpack' is the compression type of files.
            Add unpack also to move client if you want to perform unpack function on client side
*  'unpack_prog' program used to unpack data

    .. note:: The 'xrit' unpacking function is dependent on a program that can
    unpack xRIT files. Such a program is available from the `Eumetsat.int
    <http://www.eumetsat.int/Home/Main/DataAccess/SupportSoftwareTools/index.htm?l=en>`_
    website.

* 'procfile_log' [optional] path and name of procfile log.
                 If defined a log is saved in filesystem, containing all the data processed by the client
                 in order to avoid client process same files multiple times.
                 If client and server have been restarded, procfile_log is used to detect data already processed

* 'procfile_log_size' [optional] maximum number of files saved in the 'procfile_log' file

* 'use_ref_file' [optional] Path where generate reference files.
                 If defined, a reference file is generated in the specified path when an epilogue segment is processed.

                 Reference file means that an epilogue has been processed for the referenced directory,
                 referenced directory is the directory where unpacked data are stored.
                 ref_file can be used to retrigger processing or just to monitor completion of received segments.

                 reference file name: name of the epilogue segment
                 reference file content:
                                          [REF]
                                          SourcePath = referenced_directory_contains_unpacked_data
                                          FileName = file that triggered generation of ref file


Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout)
with the -l or --log option::

  move_it_client --log /path/to/mylogfile.log myconfig.ini

"""

# TODO: implement ping and server selection
import logging
import logging.handlers
import argparse

from posttroll.publisher import NoisyPublisher
from trollmoves.move_it_base import MoveItBase
from trollmoves.client import StatCollector

LOGGER = logging.getLogger("move_it_client")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


class MoveItClient(MoveItBase):

    def __init__(self, cmd_args):
        super(MoveItClient, self).__init__(cmd_args, "client")
        self._np = NoisyPublisher("move_it_client")
        self.pub = self._np.start()
        self.setup_watchers(cmd_args)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-s", "--stats",
                        help="Save stats to this file")
    parser.add_argument("-v", "--verbose", default=False, action="store_true",
                        help="Toggle verbose logging")
    return parser.parse_args()


def main():
    """Main()"""
    cmd_args = parse_args()
    client = MoveItClient(cmd_args)

    try:
        if cmd_args.stats:
            stat = StatCollector(cmd_args.stats)
            client.reload_cfg_file(cmd_args.config_file, callback=stat.collect)
        else:
            client.reload_cfg_file(cmd_args.config_file)
        client.run()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if client.running:
            client.chains_stop()


if __name__ == '__main__':
    main()

