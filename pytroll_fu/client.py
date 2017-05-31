#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2013, 2014, 2015, 2016

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

import logging
import os
import socket
import sys
import time
from collections import deque
from ConfigParser import ConfigParser
from threading import Lock, Thread
from urlparse import urlparse, urlunparse

import netifaces
import pyinotify
from zmq import LINGER, NOBLOCK, POLLIN, REQ, Poller, zmq_version

from posttroll import context
from posttroll.message import Message, MessageError
from posttroll.publisher import NoisyPublisher
from posttroll.subscriber import Subscriber

LOGGER = logging.getLogger(__name__)

file_cache = deque(maxlen=11000)
cache_lock = Lock()

REQ_TIMEOUT = 1000
if zmq_version().startswith("2."):
    REQ_TIMEOUT *= 1000
BIG_REQ_TIMEOUT = 10 * REQ_TIMEOUT


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
            LOGGER.warning("Incomplete section " + section +
                           ": add an 'providers' item.")
            LOGGER.info("Ignoring section " + section + ": incomplete.")
            del res[section]
            continue
        else:
            res[section]["providers"] = [
                "tcp://" + item for item in res[section]["providers"].split()
            ]

        if "destination" not in res[section]:
            LOGGER.warning("Incomplete section " + section +
                           ": add an 'destination' item.")
            LOGGER.info("Ignoring section " + section + ": incomplete.")
            del res[section]
            continue

        if "topic" in res[section]:
            try:
                res[section]["publish_port"] = int(res[section][
                    "publish_port"])
            except (KeyError, ValueError):
                res[section]["publish_port"] = 0
    return res


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
                LOGGER.info("Subscribing to %s with topics %s",
                            str(self.address), str(self.topics))
                self.subscriber = Subscriber(self.address, self.topics)
                LOGGER.debug("Subscriber %s", str(self.subscriber))

    def run(self):
        '''Run listener
        '''

        self.running = True

        for msg in self.subscriber(timeout=1):
            if not self.running:
                break
            if msg is None:
                continue
            LOGGER.debug("Receiving (SUB) %s", str(msg))
            self.callback(msg, *self.cargs, **self.ckwargs)

        LOGGER.debug("Exiting listener %s", str(self.address))

    def stop(self):
        '''Stop subscriber and delete the instance
        '''
        self.running = False
        time.sleep(1)
        if self.subscriber is not None:
            self.subscriber.close()
            self.subscriber = None


def request_push(msg, destination, login, publisher=None, **kwargs):
    with cache_lock:
        # fixme: remove this
        if msg.data["uid"] in file_cache:
            urlobj = urlparse(msg.data['uri'])
            if publisher and socket.gethostbyname(
                    urlobj.netloc) in get_local_ips():
                LOGGER.debug('Sending: %s', str(msg))
                publisher.send(str(msg))
            mtype = 'ack'
        else:
            mtype = 'push'
        hostname, port = msg.data["request_address"].split(":")
        req = Message(msg.subject, mtype, data=msg.data.copy())

        duri = urlparse(destination)
        scheme = duri.scheme or 'file'
        dest_hostname = duri.hostname or socket.gethostname()

        if mtype == 'push':
            # A request without credentials is build first to be printed in the
            # logs
            req.data["destination"] = urlunparse((
                scheme, dest_hostname, os.path.join(duri.path, msg.data[
                    'uid']), "", "", ""))
            LOGGER.info("Requesting: " + str(req))
            if login:
                # if necessary add the credentials for the real request
                req.data["destination"] = urlunparse((
                    scheme, login + "@" + dest_hostname, os.path.join(
                        duri.path, msg.data['uid']), "", "", ""))
            local_path = os.path.join(*([kwargs.get('ftp_root', '/')] +
                                        duri.path.split(os.path.sep) +
                                        [msg.data['uid']]))
            local_dir = os.path.dirname(local_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
                os.chmod(local_dir, 0o777)
            timeout = BIG_REQ_TIMEOUT
        else:
            LOGGER.debug("Sending: %s" % str(req))
            timeout = REQ_TIMEOUT

        requester = PushRequester(hostname, int(port))
        response = requester.send_and_recv(req, timeout=timeout)
        if response and response.type == "file":
            LOGGER.debug("Server done sending file")
            file_cache.append(msg.data["uid"])
            if publisher:
                if socket.gethostbyname(dest_hostname) in get_local_ips():
                    scheme_, host_ = "file", ''  # local file
                else:
                    scheme_, host_ = scheme, dest_hostname  # remote file
                local_msg = Message(msg.subject, "file", data=msg.data.copy())
                local_uri = urlunparse((scheme_, host_,
                                        local_path,
                                        "", "", ""))
                local_msg.data['uri'] = local_uri
                local_msg.data['origin'] = local_msg.data['request_address']
                local_msg.data.pop('request_address')

                for key in local_msg.data:
                    if key in kwargs:
                        replacement = dict(item.split(':')
                                           for item in kwargs[key].split('|'))
                        local_msg.data[key] = replacement[local_msg.data[key]]
                LOGGER.debug("publishing %s", str(local_msg))
                publisher.send(str(local_msg))
        elif response and response.type == "ack":
            pass
        else:
            LOGGER.error("Failed to get valid response from server %s: %s",
                         str(hostname), str(response))


def reload_config(filename, chains, callback=request_push, pub_instance=None):
    """Rebuild chains if needed (if the configuration changed) from *filename*.
    """

    LOGGER.debug("New config file detected! " + filename)

    new_chains = read_config(filename)

    # setup new chains

    for key, val in new_chains.items():
        identical = True
        if key in chains:
            for key2, val2 in new_chains[key].items():
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
                chains[key]["listeners"][provider] = Listener(
                    provider,
                    val["topic"],
                    callback,
                    pub_instance=pub_instance,
                    **chains[key])
                chains[key]["listeners"][provider].start()
        except Exception as err:
            LOGGER.exception(str(err))
            raise

        # create logger too!
        if "publisher" in chains[key]:
            chains[key]["publisher"].start()

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
            chains[key]["publisher"].stop()

        del chains[key]
        LOGGER.debug("Removed " + key)

    LOGGER.debug("Reloaded config from " + filename)


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
        self.running = True

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
        self.running = False
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
            small_timeout = 0.1
            while retries_left and self.running:
                now = time.time()
                while time.time() < now + timeout / 1000.0:
                    if not self.running:
                        return rep
                    socks = dict(self._poller.poll(small_timeout))
                    if socks.get(self._socket) == POLLIN:
                        reply = self._socket.recv()
                        if not reply:
                            LOGGER.error("Empty reply!")
                            break
                        try:
                            rep = Message(rawstr=reply)
                        except MessageError as err:
                            LOGGER.error('Message error: %s', str(err))
                            break
                        LOGGER.debug("Receiving (REQ) %s", str(rep))
                        self.failures = 0
                        self.jammed = False
                        return rep

                LOGGER.warning("Timeout from " + str(self._reqaddress) +
                               ", retrying...")
                # Socket is confused. Close and remove it.
                self.stop()
                retries_left -= 1
                if retries_left <= 0:
                    LOGGER.error("Server doesn't answer, abandoning... " + str(
                        self._reqaddress))
                    self.connect()
                    self.failures += 1
                    if self.failures == 5:
                        LOGGER.critical("Server jammed ? %s", self._reqaddress)
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


class StatCollector(object):

    def __init__(self, statfile):
        self.statfile = statfile

    def collect(self, msg, config):
        with open(self.statfile, 'a') as fd:
            fd.write("{0:s}\t{1:s}\t{2:s}\t{3:s}\n".format(msg.data[
                'uid'], msg.data['request_address'], str(time.time()), str(
                    msg.time)))


def terminate(chains):
    for chain in chains.itervalues():
        for listener in chain["listeners"].values():
            listener.stop()
        if "publisher" in chain:
            chain["publisher"].stop()
    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it_client."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)
