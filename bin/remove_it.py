#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2016 Martin Raspaud

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

Example configuration:

[DEFAULT]
mailhost=localhost
to=me@me.org,you@you.com
subject=Cleanup Error on {hostname}

[msg_local_12h]
base_dir=/san2
templates=sir/*,geo_in/0deg/*,geo_in/iodc/*,geo_in/fsd/*,geo_in/0degtest/*,geo_in/iodctest/*,geo_in/fsdtest/*
hours=12

[msg_local_24h]
base_dir=/san1
templates=/globalpps/import/NWP_data/global_out/*
days=1

[msg_serverfiles_3h]
base_dir=/data/prodtest/saf
templates=geo_out/0deg/*
hours=3

[msg_serverfiles_48h]
base_dir=/data/prodtest/saf
templates=geo_out/iodc/*,geo_out/world/*
days=2

[azur_local_12h]
base_dir=/san1
templates=geo_in/0deg/*
hours=12

"""

from ConfigParser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from glob import glob
import os
import time
import argparse
import logging
import logging.handlers
import getpass
import socket

logger = logging.getLogger("remove_it")

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


class BufferingSMTPHandler(logging.handlers.BufferingHandler):

    def __init__(self, mailhost, fromaddr, toaddrs, subject, capacity):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(
            logging.Formatter("[%(asctime)s %(levelname)-5s] %(message)s"))

    def flush(self):
        if len(self.buffer) > 0:
            try:
                import smtplib
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (
                    self.fromaddr, ",".join(self.toaddrs), self.subject)
                for record in self.buffer:
                    s = self.format(record)
                    msg = msg + s + "\r\n"
                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except:
                self.handleError(None)  # no particular record
            self.buffer = []


def main():
    conf = ConfigParser()

    parser = argparse.ArgumentParser()
    parser.add_argument("configuration_file",
                        help="the configuration file to use")
    parser.add_argument("--dry-run",
                        help="do not actually run, just fake it",
                        action="store_true")
    parser.add_argument("-c", "--config-item",
                        help="just run this config_item, can be provided several times",
                        default=[],
                        action="append")
    parser.add_argument("-l", "--logfile",
                        help="file to log to (stdout by default)")
    parser.add_argument("-v", "--verbose",
                        help="increase the verbosity of the script",
                        action="store_true")
    parser.add_argument("-q", "--quiet",
                        help="decrease the verbosity of the script",
                        action="store_true")
    parser.add_argument("-m", "--mail",
                        help="send errors and warning via mail",
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
        handler = logging.handlers.RotatingFileHandler(
            args.logfile, maxBytes=1000000, backupCount=10)
    else:
        handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter('[%(asctime)-15s %(levelname)-8s] %(message)s'))

    logger.addHandler(handler)

    conf.read(args.configuration_file)

    info = {"hostname": socket.gethostname(),
            "user": getpass.getuser()}

    if args.mail:
        try:
            mailhandler = BufferingSMTPHandler(conf.get("DEFAULT", "mailhost"),
                                               "{user}@{hostname}".format(
                                                   **info),
                                               conf.get(
                                                   "DEFAULT", "to").split(","),
                                               conf.get("DEFAULT", "subject").format(
                                                   **info),
                                               500)
        except NoOptionError:
            logger.info("Mail information missing, won't send emails")
        else:
            mailhandler.setLevel(logging.WARNING)
            logger.addHandler(mailhandler)

    logger.info("Starting cleanup as {user} on {hostname}".format(**info))

    config_items = []

    if args.config_item:
        for config_item in args.config_item:
            if config_item not in conf.sections():
                logger.error(
                    "No section named %s in %s", config_item, args.configuration_file)
            else:
                config_items.append(config_item)
    else:
        config_items = conf.sections()

    logger.debug("Setting up posttroll connexion...")
    with Publish("remover") as pub:
        time.sleep(3)
        logger.debug("Ready")
        tot_size = 0
        tot_files = 0
        for section in config_items:
            info = dict(conf.items(section))
            base_dir = info.get("base_dir", "")
            if not os.path.exists(base_dir):
                logger.warning(
                    "Path %s missing, skipping section %s", base_dir, section)
                continue
            logger.info("Cleaning in %s", base_dir)
            templates = (item.strip() for item in info["templates"].split(","))
            kws = {}
            for key in ["days", "hours", "minutes", "seconds"]:
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
                    if not os.path.exists(filename):
                        continue
                    try:
                        stat = os.lstat(filename)
                    except OSError:
                        logger.warning("Couldn't lstat path=%s", str(filename))
                        continue

                    if datetime.fromtimestamp(stat.st_ctime) < ref_time:
                        if not args.dry_run:
                            try:
                                if os.path.isdir(filename):
                                    if not os.listdir(filename):
                                        os.rmdir(filename)
                                    else:
                                        logger.info("%s not empty.", filename)
                                else:
                                    os.remove(filename)
                                    pub.send(
                                        str(Message("deletion", "del", {"uri": filename})))
                                logger.debug("    Removed %s", filename)
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
    logger.info(
        "Thanks for using pytroll/remove_it. See you soon on pytroll.org!")

if __name__ == '__main__':
    try:
        main()
    except:
        logger.exception("Something bad happened")
