#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Pytroll Developers
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

"""Client for Trollmoves.

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

* 'provider' is the address of the server receiving the data you want.

* 'destinations' is the list of places to put the unpacked data in.

* 'topic' gives the topic to listen to on the provider side.

* 'publish_port' defines on which port to publish incomming files. 0 means random port.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout)
with the -l or --log option::

  move_it_client --log /path/to/mylogfile.log myconfig.ini

"""

# TODO: implement ping and server selection
import logging
import argparse
import signal
import time

from trollmoves.move_it_base import MoveItBase

LOGGER = logging.getLogger("move_it_client")


class MoveItClient(MoveItBase):
    """Trollmoves client class."""

    def __init__(self, cmd_args):
        """Initialize client."""
        super(MoveItClient, self).__init__(cmd_args, "client")

    def run(self):
        """Start the transfer chains."""
        signal.signal(signal.SIGTERM, self.chains_stop)
        signal.signal(signal.SIGHUP, self.signal_reload_cfg_file)
        self.notifier.start()
        self.running = True
        while self.running:
            time.sleep(1)
            for chain_name in self.chains:
                if not self.chains[chain_name].is_alive():
                    self.chains[chain_name] = self.chains[chain_name].restart()
                self.chains[chain_name].publisher.heartbeat(30)


def parse_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-c", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-v", "--verbose", default=False, action="store_true",
                        help="Toggle verbose logging")
    return parser.parse_args()


def main():
    """Run the Trollmoves Client."""
    cmd_args = parse_args()
    client = MoveItClient(cmd_args)

    try:
        client.reload_cfg_file(cmd_args.config_file)
        client.run()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    except Exception as err:
        LOGGER.exception(err)
    finally:
        if client.running:
            client.chains_stop()


if __name__ == '__main__':
    main()
