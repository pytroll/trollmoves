#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Remove files, and send messages about it.

Example configuration items:

[msg_local_12h]
base_dir=/san2
templates=sir/*,geo_in/0deg/*,geo_in/iodc/*,geo_in/fsd/*,geo_in/0degtest/*,geo_in/iodctest/*,geo_in/fsdtest/*
hours=12

[msg_local_24h]
base_dir=/san1
templates=/globalpps/import/NWP_data/global_out/*
hours=24

[msg_serverfiles_3h]
base_dir=/data/prodtest/saf
templates=geo_out/0deg/*
hours=3

[msg_serverfiles_48h]
base_dir=/data/prodtest/saf
templates=geo_out/iodc/*,geo_out/world/*
hours=3

[azur_local_12h]
base_dir=/san1
templates=geo_in/0deg/*
hours=12

"""

from ConfigParser import ConfigParser
from datetime import datetime, timedelta
from glob import glob
import os
import time
import argparse
import logging

try:

    from posttroll.publisher import Publish
    from posttroll.message import Message

except ImportError:

    class Publish(object):

        def __enter__(self):
            return self

        def __exit__(self, etype, value, traceback):
            pass

        def send(self, msg):
            pass

    def Message(*args, **kwargs):
        del args, kwargs


if __name__ == '__main__':
    conf = ConfigParser()

    parser = argparse.ArgumentParser()
    parser.add_argument("configuration_file",
                        help="the configuration file to use")
    parser.add_argument("--dry-run",
                        help="do not actually run, just fake it",
                        action="store_true")
    parser.add_argument("-c", "--config-item",
                        help="just run this config_item")
    parser.add_argument("-l", "--logfile",
                        help="file to log to (stdout by default)")
    parser.add_argument("-v", "--verbose",
                        help="increase the verbosity of the script",
                        action="store_true")
    parser.add_argument("-q", "--quiet",
                        help="decrease the verbosity of the script",
                        action="store_true")

    args = parser.parse_args()

    logger = logging.getLogger("remove_it")

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.quiet:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.INFO)

    if args.logfile:
        handler = logging.handlers.RotatingFileHandler(args.logfile, maxBytes=1000000, backupCount=10)
    else:
        handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))

    logger.addHandler(handler)

    conf.read(args.configuration_file)

    config_items = []

    if args.config_item is None:
        config_items = conf.sections()
    elif args.config_item not in conf.sections:
        logger.error("No section named %s in %s", args.config_item, args.configuration_file)
    else:
        config_items = [args.config_item]

    logger.debug("Setting up posttroll connexion...")
    with Publish("remover") as pub:
        time.sleep(3)
        logger.debug("Ready")
        tot_size = 0
        tot_files = 0
        for section in config_items:
            info = dict(conf.items(section))
            base_dir = info.get("base_dir", "")
            logger.info("Cleaning in %s", base_dir)
            templates = (item.strip() for item in info["templates"].split(","))
            kws = {}
            for key in ["years", "months", "days", "hours", "minutes", "seconds"]:
                try:
                    kws[key] = int(info[key])
                except KeyError:
                    pass
            ref_time = datetime.utcnow() - timedelta(**kws)

            section_files = 0
            section_size = 0
            for template in templates:
                pathname = os.path.join(base_dir, template)
                logger.info("  Cleaning %s", pathname)
                flist = glob(pathname)
                for filename in flist:
                    stat = os.lstat(filename)
                    if datetime.fromtimestamp(stat.st_ctime) < ref_time:
                        if not args.dry_run:
                            try:
                                os.remove(filename)
                                pub.send(
                                    str(Message("deletion",
                                                "del", {"uri": filename})))
                                logger.debug("Removed %s", filename)
                            except (IOError, OSError) as err:
                                logger.warning("Can't remove " + filename +
                                               ": " + str(err))
                                continue
                        else:
                            logger.debug("Would remove %s", filename)
                        section_files += 1
                        section_size += stat.st_size

            logger.info("# removed files: %s", section_files)
            logger.info("MB removed: %s", section_size / 1000000)
            tot_size += section_size
            tot_files += section_files
    logger.info("Thanks for using pytroll/remove_it. See you soon on pytroll.org!")
