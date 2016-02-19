#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2013, 2014, 2015

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>

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
  destinations=/the/directory/you/want/stuff/in /another/directory/you/want/stuff/in
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

I Like To Move It Move It
I Like To Move It Move It
I Like To Move It Move It
Ya Like To (MOVE IT!)

"""

from ConfigParser import ConfigParser
import os
from urlparse import urlparse, urlunparse
import pyinotify
import logging
import logging.handlers
import time
import sys
import socket


from posttroll.publisher import NoisyPublisher
from posttroll.subscriber import Subscriber
from posttroll.message import Message
from posttroll import context

from threading import Lock, Thread
from zmq import Poller, REQ, POLLIN, NOBLOCK, LINGER, zmq_version
from Queue import Queue, Empty
from collections import deque
import netifaces


NP = None
PUB = None

queue = Queue()

LOGGER = logging.getLogger("move_it_client")

REQ_TIMEOUT = 1000
if zmq_version().startswith("2."):
    REQ_TIMEOUT *= 1000

chains = {}
listeners = {}
file_cache = deque(maxlen=100)
cache_lock = Lock()


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


# Config management
def read_config(filename):
    """Read the config file called *filename*.
    """
    cp_ = ConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        res[section].setdefault("delete", False)
        if res[section]["delete"] in ["", "False", "false", "0", "off"]:
            res[section]["delete"] = False
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)

        if "providers" not in res[section]:
            LOGGER.warning("Incomplete section " + section
                           + ": add an 'providers' item.")
            LOGGER.info("Ignoring section " + section
                        + ": incomplete.")
            del res[section]
            continue
        else:
            res[section]["providers"] = ["tcp://" + item for item in res[section]["providers"].split()]

        if "destination" not in res[section]:
            LOGGER.warning("Incomplete section " + section
                           + ": add an 'destination' item.")
            LOGGER.info("Ignoring section " + section
                        + ": incomplete.")
            del res[section]
            continue

        if "topic" in res[section]:
            try:
                res[section]["publish_port"] = int(
                    res[section]["publish_port"])
            except (KeyError, ValueError):
                res[section]["publish_port"] = 0
    return res


def reload_config(filename):
    """Rebuild chains if needed (if the configuration changed) from *filename*.
    """
    if os.path.abspath(filename) != os.path.abspath(cmd_args.config_file):
        return

    LOGGER.debug("New config file detected! " + filename)

    new_chains = read_config(filename)

    # setup new chains

    for key, val in new_chains.iteritems():
        identical = True
        if key in chains:
            for key2, val2 in new_chains[key].iteritems():
                if ((key2 not in ["listeners", "publisher"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    break
            if identical:
                continue

            if "publisher" in chains[key]:
                chains[key]["publisher"].stop()
            for provider in chains[key]["providers"]:
                chains[key]["listeners"][provider].stop()
                del chains[key]["listeners"][provider]

        chains[key] = val
        try:
            chains[key]["publisher"] = NoisyPublisher("move_it_" + key,
                                                      val["publish_port"])
        except (KeyError, NameError):
            pass

        chains[key].setdefault("listeners", {})
        try:
            for provider in chains[key]["providers"]:
                chains[key]["listeners"][provider] = Listener(provider, val["topic"], request_push,
                                                              chains[key]["destination"],
                                                              chains[key].get("login"))
                chains[key]["listeners"][provider].start()
        except Exception as err:
            LOGGER.exception(str(err))
            raise


        # create logger too!
        if "publisher" in chains[key]:
            pub = chains[key]["publisher"].start()

        if not identical:
            LOGGER.debug("Updated " + key)
        else:
            LOGGER.debug("Added " + key)

    # disable old chains

    for key in (set(chains.keys()) - set(new_chains.keys())):
        for provider, listener in chains[key]["providers"].iteritems():
            listener.stop()
            del chains[key]["providers"][provider]

        if "publisher" in chains[key]:
            pub = chains[key]["publisher"].stop()

        del chains[key]
        LOGGER.debug("Removed " + key)

    LOGGER.debug("Reloaded config from " + filename)


class Listener(Thread):

    '''PyTroll listener class for reading messages for Trollduction
    '''

    def __init__(self, address, topics, callback, *args, **kwargs):
        '''Init Listener object
        '''
        super(Listener, self).__init__()

        self.topics = topics
        self.callback = callback
        self.subscriber = None
        self.address = address
        self.create_subscriber()
        self.running = False
        self.cargs = args
        self.ckwargs = kwargs

    def create_subscriber(self):
        '''Create a subscriber instance using specified addresses and
        message types.
        '''
        if self.subscriber is None:
            if self.topics:
                LOGGER.info("Subscribing to %s with topics %s", str(self.address), str(self.topics))
                self.subscriber = Subscriber(self.address, self.topics)

    def run(self):
        '''Run listener
        '''

        self.running = True

        for msg in self.subscriber(timeout=1):
            if msg is None:
                if self.running:
                    continue
                else:
                    break
            self.callback(msg, *self.cargs, **self.ckwargs)

        LOGGER.debug("exiting listener %s", str(self.address))

    def stop(self):
        '''Stop subscriber and delete the instance
        '''
        self.running = False
        time.sleep(1)
        if self.subscriber is not None:
            self.subscriber.close()
            self.subscriber = None


def request_push(msg, destination, login):
    with cache_lock:
        # fixme: remove this
        if msg.data["uid"] in file_cache:
            urlobj = urlparse(msg.data['uri'])
            if socket.gethostbyname(urlobj.netloc) in get_local_ips():
                LOGGER.debug('Sending: %s', str(msg))
                PUB.send(str(msg))
            # fixme this should be one step up
            return

        hostname, port = msg.data["request_address"].split(":")
        requester = PushRequester(hostname, int(port))
        req = Message(msg.subject, "push", data=msg.data.copy())

        duri = urlparse(destination)
        scheme = duri.scheme or 'file'
        dest_hostname = socket.gethostname()

        # A request without credentials is build first to be printed in the logs
        req.data["destination"] = urlunparse((scheme,
                                              dest_hostname,
                                              os.path.join(destination,
                                                           msg.data['uid']),
                                              "", "", ""))
        LOGGER.info("Requesting: " + str(req))
        if login:
            # if necessary add the credentials for the real request
            req.data["destination"] = urlunparse((scheme,
                                                  login + "@" + dest_hostname,
                                                  os.path.join(destination,
                                                               msg.data['uid']),
                                                  "", "", ""))

        response = requester.send_and_recv(req, 60 * 1000) # fixme timeout should be in us for zmq2 ?
        if response and response.type == "ack":
            LOGGER.debug("Server done sending file")
            file_cache.append(msg.data["uid"])
            local_msg = Message(msg.subject, "push", data=msg.data.copy())
            local_uri = urlunparse(('file',
                                    '',
                                    os.path.join(destination,
                                                 msg.data['uid']),
                                    "", "", ""))
            local_msg.data['uri'] = local_uri
            local_msg.data['origin'] = local_msg.data['request_address']
            local_msg.data.pop('request_address')
            LOGGER.debug("publishing %s", str(local_msg))
            PUB.send(str(local_msg))
        else:
            LOGGER.error("Failed to get file from server %s: %s", str(dest_hostname), str(response))


class PushRequester(object):

    """Base requester class.
    """

    request_retries = 3

    def __init__(self, host, port):
        self._socket = None
        self._reqaddress = "tcp://" + host + ":" + str(port)
        self._poller = Poller()
        self._lock = Lock()
        self.failures = 0
        self.jammed = False

        self.connect()

    def connect(self):
        """Connect to the server
        """
        self._socket = context.socket(REQ)
        self._socket.connect(self._reqaddress)
        self._poller.register(self._socket, POLLIN)

    def stop(self):
        """Close the connection to the server
        """
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()
        self._poller.unregister(self._socket)

    def reset_connection(self):
        """Reset the socket
        """
        self.stop()
        self.connect()

    def __del__(self, *args, **kwargs):
        self.stop()

    def send_and_recv(self, msg, timeout=REQ_TIMEOUT):

        with self._lock:
            retries_left = self.request_retries
            request = str(msg)
            self._socket.send(request)
            rep = None
            while retries_left:
                socks = dict(self._poller.poll(timeout))
                if socks.get(self._socket) == POLLIN:
                    reply = self._socket.recv()
                    if not reply:
                        LOGGER.error("Empty reply!")
                        break
                    rep = Message(rawstr=reply)
                    self.failures = 0
                    self.jammed = False
                    break
                else:
                    LOGGER.warning("Timeout from " + str(self._reqaddress)
                                   + ", retrying...")
                    # Socket is confused. Close and remove it.
                    self.stop()
                    retries_left -= 1
                    if retries_left <= 0:
                        LOGGER.error("Server doesn't answer, abandoning... " +
                                     str(self._reqaddress))
                        self.connect()
                        self.failures += 1
                        if self.failures == 5:
                            LOGGER.critical("Server jammed ? %s",
                                            self._reqaddress)
                            self.jammed = True
                        break
                    LOGGER.info("Reconnecting and resending " + str(msg))
                    # Create new connection
                    self.connect()
                    self._socket.send(request)

        return rep

# Generic event handler

class EventHandler(pyinotify.ProcessEvent):

    """Handle events with a generic *fun* function.
    """

    def __init__(self, fun, *args, **kwargs):
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._fun = fun

    def process_IN_CLOSE_WRITE(self, event):
        """On closing after writing.
        """
        self._fun(event.pathname)

    def process_IN_CREATE(self, event):
        """On closing after linking.
        """
        try:
            if os.stat(event.pathname).st_nlink > 1:
                self._fun(event.pathname)
        except OSError:
            return

    def process_IN_MOVED_TO(self, event):
        """On closing after moving.
        """
        self._fun(event.pathname)


running = True

def main():
    while running:
        time.sleep(1)

def terminate(chains):
    for chain in chains.itervalues():
        for listener in chain["listeners"].values():
            listener.stop()
        if "publisher" in chain:
            chain["publisher"].stop()
    LOGGER.info("Shutting down.")
    print ("Thank you for using pytroll/move_it_client."
           " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)

if __name__ == '__main__':
    import argparse
    import signal

    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    cmd_args = parser.parse_args()

    log_format = "[%(asctime)s %(levelname)-8s] %(message)s"
    LOGGER = logging.getLogger('move_it_client')
    LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh = logging.StreamHandler()

    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)

    LOGGER.addHandler(fh)

    pyinotify.log.handlers = [fh]

    LOGGER.info("Starting up.")

    NP = NoisyPublisher("move_it_client")
    PUB = NP.start()

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)
    watchman = pyinotify.WatchManager()

    notifier = pyinotify.ThreadedNotifier(watchman, EventHandler(reload_config))
    watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def chains_stop(*args):
        global running
        running = False
        notifier.stop()
        NP.stop()
        terminate(chains)

    signal.signal(signal.SIGTERM, chains_stop)

    notifier.start()

    try:
        reload_config(cmd_args.config_file)
        main()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if running:
            chains_stop()

