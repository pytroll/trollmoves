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
import signal
import time
from abc import ABC, abstractmethod
from contextlib import suppress
from threading import Lock

from posttroll.publisher import Publisher
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

LOGGER = logging.getLogger("move_it_base")


class MoveItBase(ABC):
    """Base class for Trollmoves."""

    def __init__(self, cmd_args, publisher=None):
        """Initialize the class."""
        self.cmd_args = cmd_args
        self.running = False
        self.notifier = None
        self.watchman = None
        self.publisher = publisher
        self.chains = {}
        LOGGER.info("Starting up.")
        self.setup_watchers()
        self.run_lock = Lock()

    def chains_stop(self, *args):
        """Stop all transfer chains."""
        del args
        with suppress(RuntimeError):
            self.run_lock.acquire(timeout=1)

        self.running = False
        try:
            self.notifier.stop()
        except RuntimeError as err:
            LOGGER.warning("Could not stop notifier: %s", err)
        with suppress(AttributeError):
            self.publisher.stop()
        self.terminate()

    @abstractmethod
    def terminate(self):
        """Terminate the chains and threads."""

    def setup_watchers(self):
        """Set up watcher for the configuration file."""
        config_file = self.cmd_args.config_file

        observer = Observer()
        handler = WatchdogChangeHandler(self.reload_cfg_file)

        observer.schedule(handler, config_file)

        self.notifier = observer

    def run(self):
        """Start the transfer chains."""
        try:
            signal.signal(signal.SIGTERM, self.chains_stop)
            signal.signal(signal.SIGHUP, self.signal_reload_cfg_file)
        except ValueError:
            LOGGER.warning("Signals could not be set up.")
        self.notifier.start()
        self.running = True
        while self.running:
            time.sleep(1)
            shutting_down = not self.run_lock.acquire(blocking=False)
            if shutting_down:
                break
            try:
                self._run()
            finally:
                self.run_lock.release()

    @abstractmethod
    def _run(self):
        raise NotImplementedError


def create_publisher(port, publisher_name):
    """Create a publisher using port *port* and start it."""
    LOGGER.info("Starting publisher on port %s.", str(port))
    publisher = Publisher("tcp://*:" + str(port), publisher_name)
    publisher.start()
    return publisher


class WatchdogChangeHandler(FileSystemEventHandler):
    """Trigger processing on filesystem events."""

    def __init__(self, fun):
        """Initialize the processor."""
        super().__init__()
        self.fun = fun

    def on_closed(self, event):
        """Process file creation."""
        self.fun(event.src_path)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self.fun(event.dest_path)
