# -*- coding: utf-8 -*-

# Copyright (c) 2017

# Author(s):

#   Lars Ørum Rasmussen <ras@dmi.dk>
#   Janne Kotro <janne.kotro@fmi.fi>

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

# Notes:
# - This is probably also the place to add possible alarm related plugins (fx. Nagios).
# - Timer reset from: http://code.activestate.com/recipes/577407-resettable-timer-class-a-little-enhancement-from-p/

import threading
import logging
import time

LOGGER = logging.getLogger(__name__)

# Seconds between heartbeats. A default value could be calculated after a few heartbeat.
# Newer version of posttroll is sending heartbeats including `min_interval`.
DEFAULT_MIN_INTERVAL = 30


class Monitor(threading.Thread):
    """Will monitor heartbeats.

    Will set alarm event if no heartbeat received in specified time interval.
    Will do nothing if no time interval scale defined.
    """

    def __init__(self, alarm_event, **kwargs):
        """Will set `alarm_event` if no heartbeat in time interval `heartbeat_alarm_scale` times
        heartbeat time interval.
        """
        self._alarm_scale = float(kwargs.get("heartbeat_alarm_scale", 0))
        self._alarm_event = alarm_event
        self._interval = self._alarm_scale * DEFAULT_MIN_INTERVAL
        self._finished = threading.Event()
        threading.Thread.__init__(self)

    def __call__(self, msg=None):
        """Receive a heartbeat (or not) to reset the timer.

        TODO: If possibility for blocking, add a queue.
        """
        if self._alarm_scale:
            if msg and msg.type == "beat":
                try:
                    self._interval = self._alarm_scale * float(msg.data["min_interval"])
                except (KeyError, AttributeError, TypeError, ValueError):
                    pass
            LOGGER.debug("Resetting heartbeat alarm timer to %.1f sec", self._interval)
            self._resetted = True
            self._finished.set()
            self._finished.clear()

    def start(self):
        if self._alarm_scale:
            threading.Thread.start(self)
        return self

    def stop(self):
        self._finished.set()

    #
    # Context interface.
    #
    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        return self.stop()

    #
    # Running in the thread.
    #
    def run(self):

        LOGGER.debug("Starting heartbeat monitor with alarm scale %.2f", self._alarm_scale)

        while not self._finished.is_set():
            self._resetted = True

            while self._resetted:
                self._resetted = False
                self._finished.wait(self._interval)
                time.sleep(0.05)  # prevent a race condition between a finished set / clear (?)

            if not self._finished.is_set():
                self._set_alarm()

        LOGGER.debug("Stopping heartbeat monitor")

    def _set_alarm(self):
        if self._alarm_event:
            LOGGER.debug("Missing heartbeat alarm !")
            self._alarm_event.set()
