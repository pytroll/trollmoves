#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2015 - 2025 Pytroll Developers
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

"""Remove files, and send messages about it."""

import argparse
import getpass
import logging
import logging.handlers
import socket
import time
from configparser import NoOptionError, RawConfigParser

from trollmoves.filescleaner import FilesCleaner

LOGGER = logging.getLogger(__name__)

try:
    from posttroll.message import Message
    from posttroll.publisher import Publish
except ImportError:

    class Publish(object):
        """A fake publish in case posttroll isn't installed."""

        def __enter__(self):
            """Fake enter."""
            return self

        def __exit__(self, etype, value, traceback):
            """Fake exit."""

        def send(self, msg):
            """Fake send."""

    def Message(*args, **kwargs):
        """Fake message object in case posttroll isn't installed."""
        del args, kwargs


class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    """A logging handler for emails."""

    def __init__(self, mailhost, fromaddr, toaddrs, subject, capacity):
        """Set up the handler."""
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(
            logging.Formatter("[%(asctime)s %(levelname)-5s] %(message)s"))

    def flush(self):
        """Flush the messages."""
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
            except Exception:
                self.handleError(None)  # no particular record
            self.buffer = []


def parse_args():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("configuration_file",
                        help="the configuration file to use")
    parser.add_argument("--dry-run",
                        help="do not actually run, just fake it",
                        action="store_true", default=False)
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

    return parser.parse_args()


def setup_logger(args):
    """Set up logging."""
    msgformat = '[%(asctime)-15s %(levelname)-8s] %(message)s'

    if args.logfile:
        handler = logging.handlers.RotatingFileHandler(
            args.logfile, maxBytes=1000000, backupCount=10)
    else:
        handler = logging.StreamHandler()

    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter('[%(asctime)-15s %(levelname)-8s] %(message)s'))

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, handlers=[handler], format=msgformat)
    elif args.quiet:
        logging.basicConfig(level=logging.ERROR, handlers=[handler], format=msgformat)
    else:
        logging.basicConfig(level=logging.WARNING, handlers=[handler], format=msgformat)


def setup_mailing(args, conf, info):
    """Set up the mailing handler."""
    if args.mail:
        try:
            mailhandler = BufferingSMTPHandler(
                conf.get("DEFAULT", "mailhost"),
                "{user}@{hostname}".format(**info),
                conf.get("DEFAULT", "to").split(","),
                conf.get("DEFAULT", "subject").format(**info),
                500)
        except NoOptionError:
            LOGGER.info("Mail information missing, won't send emails")
        else:
            mailhandler.setLevel(logging.WARNING)
            LOGGER.addHandler(mailhandler)


def get_config_items(args, conf):
    """Get the configuration items."""
    config_items = []

    if args.config_item:
        for config_item in args.config_item:
            if config_item not in conf.sections():
                LOGGER.error("No section named %s in %s",
                             config_item, args.configuration_file)
            else:
                config_items.append(config_item)
    else:
        config_items = conf.sections()

    return config_items


def run(args, conf):
    """Run the remove_it tool."""
    config_items = get_config_items(args, conf)

    LOGGER.debug("Setting up posttroll connection...")
    with Publish("remover") as pub:
        time.sleep(3)
        LOGGER.debug("Ready")
        tot_size = 0
        tot_files = 0
        for section in config_items:
            info = dict(conf.items(section))
            fcleaner = FilesCleaner(pub, section, info, dry_run=args.dry_run)
            size, num_files = fcleaner.clean_section()
            tot_size += size
            tot_files += num_files

        LOGGER.info("# removed files: %s", tot_files)
        LOGGER.info("MB removed: %s", tot_size / 1000000)


def main():
    """Parse args and run the remove_it tool."""
    conf = RawConfigParser()

    args = parse_args()

    conf.read(args.configuration_file)

    info = {"hostname": socket.gethostname(),
            "user": getpass.getuser()}

    setup_logger(args)
    setup_mailing(args, conf, info)

    LOGGER.info("Starting cleanup as %s on %s", info['user'], info['hostname'])
    run(args, conf)

    LOGGER.info("Thanks for using pytroll/remove_it. See you soon on "
                "pytroll.org!")


if __name__ == '__main__':
    main()
