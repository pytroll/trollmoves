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

"""Base class for move_it_{client,server,mirror}."""

import logging
import logging.handlers
import os

import pyinotify
from posttroll.publisher import Publisher

LOGGER = logging.getLogger("move_it_base")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


class MoveItBase(object):
    """Base class for Trollmoves."""

    def __init__(self, cmd_args, chain_type, publisher=None):
        """Initialize the class."""
        self.cmd_args = cmd_args
        self.chain_type = chain_type
        self.running = False
        self.notifier = None
        self.watchman = None
        self.publisher = publisher
        self._np = None
        self.chains = {}
        setup_logging(cmd_args, chain_type)
        LOGGER.info("Starting up.")
        self.setup_watchers(cmd_args)

    def reload_cfg_file(self, filename, *args, **kwargs):
        """Reload configuration file."""
        if self.chain_type == "client":
            from trollmoves.client import reload_config
            reload_config(filename, self.chains, *args, **kwargs)
        else:
            # Also Mirror uses the reload_config from the Server
            from trollmoves.server import reload_config
            reload_config(filename, self.chains, *args, publisher=self.publisher,
                          use_watchdog=self.cmd_args.watchdog,
                          disable_backlog=self.cmd_args.disable_backlog)

    def signal_reload_cfg_file(self, *args):
        """Handle reload signal."""
        del args
        if self.chain_type == "client":
            from trollmoves.client import reload_config
            reload_config(self.cmd_args.config_file, self.chains,
                          publisher=self.publisher)
        else:
            from trollmoves.server import reload_config
            reload_config(self.cmd_args.config_file, self.chains,
                          publisher=self.publisher,
                          use_watchdog=self.cmd_args.watchdog,
                          disable_backlog=self.cmd_args.disable_backlog)

    def chains_stop(self, *args):
        """Stop all transfer chains."""
        del args
        if self.chain_type == "client":
            from trollmoves.client import terminate
        else:
            from trollmoves.server import terminate
        self.running = False
        self.notifier.stop()
        try:
            self._np.stop()
        except AttributeError:
            pass
        terminate(self.chains)

    def setup_watchers(self, cmd_args):
        """Set up watcher for the configuration file."""
        mask = (pyinotify.IN_CLOSE_WRITE |
                pyinotify.IN_MOVED_TO |
                pyinotify.IN_CREATE)
        self.watchman = pyinotify.WatchManager()

        event_handler = EventHandler(self.reload_cfg_file,
                                     watchManager=self.watchman,
                                     tmask=mask,
                                     cmd_filename=self.cmd_args.config_file)
        self.notifier = pyinotify.ThreadedNotifier(self.watchman, event_handler)
        self.watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)


def setup_logging(cmd_args, chain_type):
    """Set up logging."""
    global LOGGER
    LOGGER = logging.getLogger('')
    if cmd_args.verbose:
        LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh_ = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh_ = logging.StreamHandler()

    formatter = logging.Formatter(LOG_FORMAT)
    fh_.setFormatter(formatter)

    LOGGER.addHandler(fh_)
    logger_name = "move_it_server"
    if chain_type == "client":
        logger_name = "move_it_client"
    elif chain_type == "mirror":
        logger_name = "move_it_mirror"
    LOGGER = logging.getLogger(logger_name)
    pyinotify.log.handlers = [fh_]


def create_publisher(port, publisher_name):
    """Create a publisher using port *port*."""
    LOGGER.info("Starting publisher on port %s.", str(port))
    return Publisher("tcp://*:" + str(port), publisher_name)


# Generic event handler
# fixme: on deletion, the file should be removed from the filecache
class EventHandler(pyinotify.ProcessEvent):
    """Handle events with a generic *fun* function."""

    def __init__(self, fun, *args, **kwargs):
        """Initialize event handler."""
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._cmd_filename = kwargs.get('cmd_filename')
        if self._cmd_filename:
            self._cmd_filename = os.path.abspath(self._cmd_filename)
        self._fun = fun
        self._watched_dirs = dict()
        self._watchManager = kwargs.get('watchManager', None)
        self._tmask = kwargs.get('tmask', None)

    def process_IN_CLOSE_WRITE(self, event):
        """On closing after writing."""
        if self._cmd_filename and os.path.abspath(
                event.pathname) != self._cmd_filename:
            return
        self._fun(event.pathname)

    def process_IN_CREATE(self, event):
        """On closing after linking."""
        if (event.mask & pyinotify.IN_ISDIR):
            self._watched_dirs.update(self._watchManager.add_watch(event.pathname, self._tmask))

        if self._cmd_filename and os.path.abspath(
                event.pathname) != self._cmd_filename:
            return
        try:
            if os.stat(event.pathname).st_nlink > 1:
                self._fun(event.pathname)
        except OSError:
            return

    def process_IN_MOVED_TO(self, event):
        """On closing after moving."""
        if self._cmd_filename and os.path.abspath(
                event.pathname) != self._cmd_filename:
            return
        self._fun(event.pathname)

    def process_IN_DELETE(self, event):
        """On delete."""
        if (event.mask & pyinotify.IN_ISDIR):
            try:
                try:
                    self._watchManager.rm_watch(self._watched_dirs[event.pathname], quiet=False)
                except pyinotify.WatchManagerError:
                    # As the directory is deleted prior removing the
                    # watch will cause a error message from
                    # pyinotify. This is ok, so just pass the
                    # exception.
                    pass
                finally:
                    del self._watched_dirs[event.pathname]
            except KeyError:
                LOGGER.warning(
                    "Dir %s not watched by inotify. Can not delete watch.",
                    event.pathname)
        return
