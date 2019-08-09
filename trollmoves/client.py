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

import logging
import os
import socket
import sys
import time
import tarfile
from collections import deque
from six.moves.configparser import RawConfigParser
from threading import Lock, Thread, Event
import six
from six.moves.urllib.parse import urlparse, urlunparse

import pyinotify
from zmq import LINGER, POLLIN, REQ, Poller

from posttroll import get_context
from posttroll.message import Message, MessageError
from posttroll.publisher import NoisyPublisher
from posttroll.subscriber import Subscriber

from trollmoves import heartbeat_monitor
from trollmoves.utils import get_local_ips
from trollmoves.utils import gen_dict_extract, translate_dict, translate_dict_value

LOGGER = logging.getLogger(__name__)

file_cache = deque(maxlen=11000)
cache_lock = Lock()

DEFAULT_REQ_TIMEOUT = 1

SERVER_HEARTBEAT_TOPIC = "/heartbeat/move_it_server"


# Config management
def read_config(filename):
    """Read the config file called *filename*.
    """
    cp_ = RawConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        res[section].setdefault("delete", False)
        if res[section]["delete"] in ["", "False", "false", "0", "off"]:
            res[section]["delete"] = False
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)
        res[section].setdefault("heartbeat", True)
        res[section].setdefault("req_timeout", DEFAULT_REQ_TIMEOUT)
        res[section].setdefault("transfer_req_timeout", 10 * DEFAULT_REQ_TIMEOUT)
        res[section].setdefault("nameservers", None)
        if res[section]["heartbeat"] in ["", "False", "false", "0", "off"]:
            res[section]["heartbeat"] = False

        if "providers" not in res[section]:
            LOGGER.warning("Incomplete section %s: add an 'providers' item.",
                           section)
            LOGGER.info("Ignoring section %s: incomplete.",
                        section)
            del res[section]
            continue
        else:
            res[section]["providers"] = [
                "tcp://" + item for item in res[section]["providers"].split()
            ]

        if "destination" not in res[section]:
            LOGGER.warning("Incomplete section %s: add an 'destination' item.",
                           section)
            LOGGER.info("Ignoring section %s: incomplete.", section)
            del res[section]
            continue

        if "topic" in res[section]:
            try:
                res[section]["publish_port"] = int(res[section][
                    "publish_port"])
            except (KeyError, ValueError):
                res[section]["publish_port"] = 0
        elif not res[section]["heartbeat"]:
            # We have no topics and therefor no subscriber (if you want to
            # subscribe everything, then explicit specify an empty topic).
            LOGGER.warning("Incomplete section %s: add an 'topic' "
                           "item or enable heartbeat.", section)
            LOGGER.info("Ignoring section %s: incomplete.", section)
            del res[section]
            continue

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
        self.running = False
        self.cargs = args
        self.ckwargs = kwargs
        self.restart_event = Event()

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

        with heartbeat_monitor.Monitor(self.restart_event, **self.ckwargs) as beat_monitor:

            self.running = True

            while self.running:
                # Loop for restart.

                LOGGER.debug("Starting listener %s", str(self.address))
                self.create_subscriber()

                for msg in self.subscriber(timeout=1):
                    if not self.running:
                        break

                    if self.restart_event.is_set():
                        self.restart_event.clear()
                        self.stop()
                        self.running = True
                        break

                    if msg is None:
                        continue

                    LOGGER.debug("Receiving (SUB) %s", str(msg))

                    beat_monitor(msg)
                    if msg.type == "beat":
                        continue

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


def unpack_tar(filename, delete=False):
    """Unpack tar files."""
    destdir = os.path.dirname(filename)
    try:
        with tarfile.open(filename) as tar:
            tar.extractall(destdir)
            members = tar.getmembers()
    except tarfile.ReadError as err:
        raise IOError(str(err))
    if delete:
        os.remove(filename)
    return (member.name for member in members)


unpackers = {'tar': unpack_tar}


def already_received(msg):
    """Check if the files from msg already are in the local cache."""
    with cache_lock:
        for filename in gen_dict_extract(msg.data, 'uid'):
            if filename not in file_cache:
                return False
        else:
            return True


def resend_if_local(msg, publisher):
    """Resend the message provided all uris point to local files."""
    for uri in gen_dict_extract(msg.data, 'uri'):
        urlobj = urlparse(uri)
        if not publisher or not socket.gethostbyname(urlobj.netloc) in get_local_ips():
            return
    else:
        LOGGER.debug('Sending: %s', str(msg))
        publisher.send(str(msg))


def create_push_req_message(msg, destination, login):
    fake_req = Message(msg.subject, 'push', data=msg.data.copy())
    duri = urlparse(destination)
    scheme = duri.scheme or 'file'
    dest_hostname = duri.hostname or socket.gethostname()
    fake_req.data["destination"] = urlunparse((scheme, dest_hostname, duri.path, "", "", ""))
    if login:
        # if necessary add the credentials for the real request
        req = Message(msg.subject, 'push', data=msg.data.copy())
        req.data["destination"] = urlunparse((scheme, login + "@" + dest_hostname, duri.path, "", "", ""))
    else:
        req = fake_req
    return req, fake_req


def create_local_dir(destination, local_root, mode=0o777):
    """Create the local directory if it doesn't exist and return that path."""
    duri = urlparse(destination)
    local_dir = os.path.join(*([local_root] + duri.path.split(os.path.sep)))

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        os.chmod(local_dir, mode)
    return local_dir


def unpack_and_create_local_message(msg, local_dir, unpack=None, delete=False):

    def unpack_callback(var):
        if not var['uid'].endswith(unpack):
            return var
        packname = var.pop('uid')
        del var['uri']
        new_names = unpackers[unpack](os.path.join(local_dir, packname), delete)

        var['dataset'] = [dict(uid=nn, uri=os.path.join(local_dir, nn)) for nn in new_names]
        return var

    if unpack is not None:
        lmsg_data = translate_dict(msg.data, ('uri', 'uid'), unpack_callback)
        if 'dataset' in lmsg_data:
            lmsg_type = 'dataset'
        elif 'collection' in lmsg_data:
            lmsg_type = 'collection'
        else:
            lmsg_type = 'file'
    else:
        lmsg_data = msg.data.copy()
        lmsg_type = msg.type

    return Message(msg.subject, lmsg_type, data=lmsg_data)


def make_uris(msg, destination, login=None):
    duri = urlparse(destination)
    scheme = duri.scheme or 'ssh'
    dest_hostname = duri.hostname or socket.gethostname()
    if socket.gethostbyname(dest_hostname) in get_local_ips():
        scheme_, host_ = "ssh", dest_hostname  # local file
    else:
        scheme_, host_ = scheme, dest_hostname  # remote file
        if login:
            # Add (only) user to uri.
            host_ = login.split(":")[0] + "@" + host_

    def uri_callback(var):
        uid = var['uid']
        path = os.path.join(duri.path, uid)
        var['uri'] = urlunparse((scheme_, host_, path, "", "", ""))
        return var
    msg.data = translate_dict(msg.data, ('uri', 'uid'), uri_callback)
    return msg


def replace_mda(msg, kwargs):
    for key in msg.data:
        if key in kwargs:
            replacement = dict(item.split(':')
                               for item in kwargs[key].split('|'))
            msg.data[key] = replacement[msg.data[key]]
    return msg


def request_push(msg, destination, login, publisher=None, unpack=None, delete=False, **kwargs):
    if already_received(msg):
        resend_if_local(msg, publisher)
        mtype = 'ack'
        req = Message(msg.subject, mtype, data=msg.data)
        LOGGER.debug("Sending: %s", str(req))
        timeout = float(kwargs["req_timeout"])
    else:
        mtype = 'push'
        req, fake_req = create_push_req_message(msg, destination, login)
        LOGGER.info("Requesting: %s", str(fake_req))
        timeout = float(kwargs["transfer_req_timeout"])
        local_dir = create_local_dir(destination, kwargs.get('ftp_root', '/'))

    LOGGER.debug("Send and recv timeout is %.2f seconds", timeout)

    hostname, port = msg.data["request_address"].split(":")
    requester = PushRequester(hostname, int(port))
    response = requester.send_and_recv(req, timeout=timeout)

    if response and response.type in ['file', 'collection', 'dataset']:
        LOGGER.debug("Server done sending file")
        with cache_lock:
            for uid in gen_dict_extract(msg.data, 'uid'):
                file_cache.append(uid)
        try:
            lmsg = unpack_and_create_local_message(response, local_dir, unpack, delete)
        except IOError:
            LOGGER.exception("Couldn't unpack %s", str(response))
            return
        if publisher:
            lmsg = make_uris(lmsg, destination, login)
            lmsg.data['origin'] = response.data['request_address']
            lmsg.data.pop('request_address', None)
            lmsg = replace_mda(lmsg, kwargs)
            lmsg.data.pop('destination', None)

            LOGGER.debug("publishing %s", str(lmsg))
            publisher.send(str(lmsg))

    elif response and response.type == "ack":
        pass
    else:
        LOGGER.error("Failed to get valid response from server %s: %s",
                     str(hostname), str(response))


def reload_config(filename, chains, callback=request_push, pub_instance=None):
    """Rebuild chains if needed (if the configuration changed) from *filename*.
    """

    LOGGER.debug("New config file detected: %s", filename)

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
            nameservers = val["nameservers"]
            if nameservers:
                nameservers = nameservers.split()
            chains[key]["publisher"] = NoisyPublisher(
                "move_it_" + key,
                port=val["publish_port"],
                nameservers=nameservers)
        except (KeyError, NameError):
            pass

        chains[key].setdefault("listeners", {})
        try:
            topics = []
            if "topic" in val:
                topics.append(val["topic"])
            if val.get("heartbeat", False):
                topics.append(SERVER_HEARTBEAT_TOPIC)
            for provider in chains[key]["providers"]:
                chains[key]["listeners"][provider] = Listener(
                    provider,
                    topics,
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
            LOGGER.debug("Updated %s", key)
        else:
            LOGGER.debug("Added %s", key)

    # disable old chains

    for key in (set(chains.keys()) - set(new_chains.keys())):
        for provider, listener in chains[key]["providers"].items():
            listener.stop()
            del chains[key]["providers"][provider]

        if "publisher" in chains[key]:
            chains[key]["publisher"].stop()

        del chains[key]
        LOGGER.debug("Removed %s", key)

    LOGGER.debug("Reloaded config from %s", filename)


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
        self._socket = get_context().socket(REQ)
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

    def send_and_recv(self, msg, timeout=DEFAULT_REQ_TIMEOUT):

        with self._lock:
            retries_left = self.request_retries
            request = str(msg)
            self._socket.send_string(request)
            rep = None
            small_timeout = 0.1
            while retries_left and self.running:
                now = time.time()
                while time.time() < now + timeout:
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
                    # During big file transfers, give some time to a friend.
                    time.sleep(0.1)

                LOGGER.warning("Timeout from %s, retrying...",
                               str(self._reqaddress))
                # Socket is confused. Close and remove it.
                self.stop()
                retries_left -= 1
                if retries_left <= 0:
                    LOGGER.error("Server %s doesn't answer, abandoning.",
                                 str(self._reqaddress))
                    self.connect()
                    self.failures += 1
                    if self.failures == 5:
                        LOGGER.critical("Server jammed: %s", self._reqaddress)
                        self.jammed = True
                    break
                LOGGER.info("Reconnecting and resending %s", str(msg))
                # Create new connection
                self.connect()
                self._socket.send_string(request)

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

    def collect(self, msg, *args, **kwargs):
        with open(self.statfile, 'a') as fd:
            fd.write(time.asctime() + " - " + str(msg) + "\n")


def terminate(chains):
    for chain in six.itervalues(chains):
        for listener in chain["listeners"].values():
            listener.stop()
        if "publisher" in chain:
            chain["publisher"].stop()
    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it_client."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)
