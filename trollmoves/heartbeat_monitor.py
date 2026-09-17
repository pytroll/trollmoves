import logging
import threading
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
        LOGGER.debug("Entering the Heartbeat monitor %.2f", self._alarm_scale)
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
                # prevent a race condition between a finished set / clear (?)
                time.sleep(0.05)

            if not self._finished.is_set():
                self._set_alarm()

        LOGGER.debug("Stopping heartbeat monitor")

    def _set_alarm(self):
        if self._alarm_event:
            LOGGER.debug("Missing heartbeat alarm!")
            self._alarm_event.set()
