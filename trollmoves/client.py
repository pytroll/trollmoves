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

"""Trollmoves client."""

import logging
import os
import socket
import sys
import time
import tarfile
from collections import deque
from six.moves.configparser import RawConfigParser
from threading import Lock, Thread, Event
import hashlib
import six
from six.moves.urllib.parse import urlparse, urlunparse
import subprocess
from contextlib import suppress
import pyinotify
from zmq import LINGER, POLLIN, REQ, Poller
import bz2

from posttroll import get_context
from posttroll.message import Message, MessageError
from posttroll.publisher import NoisyPublisher
from posttroll.subscriber import Subscriber
from trollsift.parser import compose

from trollmoves import heartbeat_monitor
from trollmoves.utils import get_local_ips
from trollmoves.utils import gen_dict_extract, translate_dict
from trollmoves.movers import CTimer

LOGGER = logging.getLogger(__name__)

file_cache = deque(maxlen=11000)
cache_lock = Lock()
ongoing_transfers = dict()
ongoing_transfers_lock = Lock()
hot_spare_timer_lock = Lock()
ongoing_hot_spare_timers = dict()

DEFAULT_REQ_TIMEOUT = 1
SERVER_HEARTBEAT_TOPIC = "/heartbeat/move_it_server"
CLIENT_HEARTBEAT_TOPIC_BASE = "/heartbeat/move_it"

COMPRESSED_ENDINGS = {'xrit': ['C_'],
                      'tar': ['.tar', '.tar.gz', '.tgz', '.tar.bz2'],
                      'bzip': ['.bz2'],
                      }
BUNZIP_BLOCK_SIZE = 1024
LISTENER_CHECK_INTERVAL = 1


# Config management
def read_config(filename):
    """Read the config file called *filename*."""
    cp_ = RawConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        res[section].setdefault("delete", False)
        if res[section]["delete"] in ["", "False", "false", "0", "off"]:
            res[section]["delete"] = False
        if res[section]["delete"] in ["True", "true", "on", "1"]:
            res[section]["delete"] = True
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)
        res[section].setdefault("xritdecompressor", None)
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
                "tcp://" + item.split('/', 1)[0] for item in res[section]["providers"].split()
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
    """PyTroll listener class for reading messages for Trollduction."""

    def __init__(self, address, topics, callback, *args, die_event=None, **kwargs):
        """Init Listener object."""
        super(Listener, self).__init__()

        self.topics = topics
        self.callback = callback
        self.subscriber = None
        self.address = address
        self.running = False
        self.die_event = die_event
        self.cargs = args
        self.ckwargs = kwargs
        self.restart_event = Event()
        self.cause_of_death = None
        self.death_count = 0

    def restart(self):
        """Restart the listener, returns a new running instance."""
        self.stop()
        new_listener = self.__class__(self.address, self.topics, self.callback, *self.cargs,
                                      die_event=self.die_event, **self.ckwargs)
        new_listener.death_count = self.death_count + 1
        new_listener.start()
        return new_listener

    def create_subscriber(self):
        """Create a subscriber using specified addresses and message types."""
        if self.subscriber is None:
            if self.topics:
                LOGGER.info("Subscribing to %s with topics %s",
                            str(self.address), str(self.topics))
                self.subscriber = Subscriber(self.address, self.topics)
                LOGGER.debug("Subscriber %s", str(self.subscriber))

    def run(self):
        """Run listener."""
        try:
            with heartbeat_monitor.Monitor(self.restart_event, **self.ckwargs) as beat_monitor:

                self.running = True

                while self.running:
                    # Loop for restart.
                    LOGGER.debug("Starting listener %s", str(self.address))
                    self.create_subscriber()

                    for msg in self.subscriber(timeout=1):
                        if not self.running:
                            break

                        # If the heartbeat didn't arrive, restart the subscriber
                        if self.restart_event.is_set():
                            LOGGER.warning("Missing a heartbeat, restarting the subscriber to %s.",
                                           str(self.subscriber.addresses))
                            self.restart_event.clear()
                            self.stop()
                            self.running = True
                            break

                        if msg is None:
                            continue

                        LOGGER.debug("Receiving (SUB) %s", str(msg))

                        beat_monitor(msg)
                        if msg.type == "beat":
                            self.death_count = 0
                            continue

                        # Handle public "push" messages as a hot spare client
                        if msg.type == "push":
                            # TODO: these need to be checked and acted if
                            # the transfers are not finished on primary
                            # client and are not cleared
                            LOGGER.debug("Primary client published 'push'")
                            add_to_ongoing(msg)
                            continue

                        # Handle public "ack" messages as a hot spare client
                        if msg.type == "ack":
                            LOGGER.debug("Primary client finished transfer")
                            _ = add_to_file_cache(msg)
                            _ = clean_ongoing_transfer(get_msg_uid(msg))
                            continue

                        # Ignore "file" messages which don't have a request address
                        if msg.type == "file" and "request_address" not in msg.data:
                            LOGGER.debug("Ignoring 'file' message from primary client.")
                            add_to_ongoing(msg)
                            _ = add_to_file_cache(msg)
                            _ = clean_ongoing_transfer(get_msg_uid(msg))
                            continue

                        # If this is a hot spare client, wait for a while
                        # for a public "push" message which will update
                        # the ongoing transfers before starting processing here
                        delay = self.ckwargs.get("processing_delay", False)
                        if delay:
                            add_timer(float(delay), self.callback, msg, *self.cargs,
                                      **self.ckwargs)
                        else:
                            self.callback(msg, *self.cargs, **self.ckwargs)

                    LOGGER.debug("Exiting listener %s", str(self.address))
        except Exception as err:
            LOGGER.exception("Listener died.")
            self.cause_of_death = err
            with suppress(AttributeError):
                self.die_event.set()

    def stop(self):
        """Stop subscriber and delete the instance."""
        self.running = False
        time.sleep(1)
        if self.subscriber is not None:
            self.subscriber.close()
            self.subscriber = None


def clean_ongoing_transfer(uid):
    """Clear transfer for the given UID from the cache."""
    with ongoing_transfers_lock:
        msgs = ongoing_transfers.pop(uid, [])
        LOGGER.debug("Remove uid %s: %s", uid, str(msgs))
    return msgs


def unpack_tar(filename, **kwargs):
    """Unpack tar files."""
    destdir = os.path.dirname(filename)
    try:
        with tarfile.open(filename) as tar:
            tar.extractall(destdir)
            members = tar.getmembers()
    except tarfile.ReadError as err:
        raise IOError(str(err))
    fnames = tuple(os.path.join(destdir, member.name) for member in members)
    if len(fnames) == 1:
        return fnames[0]
    return fnames


def unpack_xrit(filename, **kwargs):
    """Unpack XRIT files."""
    if filename.endswith('__'):
        return filename
    cmd = kwargs.get('xritdecompressor')
    if cmd is None:
        raise OSError("Path to 'xRITDecompress' utility not defined. "
                      "Set it with 'xritdecompressor' config option.")
    destdir = os.path.dirname(filename)
    out_fname = os.path.join(destdir, os.path.basename(filename)[:-2] + "__")
    check_output([cmd, filename], cwd=(destdir))
    return out_fname


def unpack_bzip(filename, **kwargs):
    """Unzip .bz2 files."""
    block_size = int(kwargs.get('block_size', BUNZIP_BLOCK_SIZE))
    out_fname = filename[:-4]
    if os.path.exists(out_fname):
        return out_fname
    with open(out_fname, "wb") as dest:
        try:
            orig = bz2.BZ2File(filename, "r")
            while True:
                block = orig.read(block_size)

                if not block:
                    break
                dest.write(block)
            LOGGER.debug("Bunzipped %s to %s", filename, out_fname)
        finally:
            orig.close()
    return out_fname


def check_output(*popenargs, **kwargs):
    """Copy from python 2.7, `subprocess.check_output`."""
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    LOGGER.debug("Calling %s", str(popenargs))
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    del unused_err
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise RuntimeError(output)
    return output


unpackers = {'tar': unpack_tar,
             'xrit': unpack_xrit,
             'bzip': unpack_bzip}


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

    LOGGER.debug('Sending: %s', str(msg))
    publisher.send(str(msg))


def create_push_req_message(msg, destination, login):
    """Create a message for push request."""
    fake_req = Message(msg.subject, 'push', data=msg.data.copy())
    duri = urlparse(destination)
    scheme = duri.scheme or 'file'
    dest_hostname = duri.hostname or socket.gethostname()
    if duri.port:
        dest_hostname += ":{}".format(duri.port)
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


def unpack_and_create_local_message(msg, local_dir, **kwargs):
    """Unpack the file(s) given in the message, and return an updated message."""
    def unpack_callback(var, **kwargs):
        unpack = kwargs.get('compression')
        endings = COMPRESSED_ENDINGS[unpack]
        is_compressed = any([var['uid'].endswith(ending) for ending in endings])
        if not is_compressed:
            return var
        packname = var.pop('uid')
        del var['uri']
        new_names = unpackers[unpack](os.path.join(local_dir, packname),
                                      **kwargs)
        if kwargs.get("delete"):
            LOGGER.debug("Deleting %s", os.path.join(local_dir, packname))
            os.remove(os.path.join(local_dir, packname))
        if isinstance(new_names, tuple):
            var['dataset'] = [dict(uid=os.path.basename(nn),
                                   uri=os.path.join(local_dir, nn))
                              for nn in new_names]
        else:
            var['uid'] = os.path.basename(new_names)
            var['uri'] = os.path.join(local_dir, new_names)
        return var

    if kwargs.get('compression') in COMPRESSED_ENDINGS:
        lmsg_data = translate_dict(msg.data, ('uri', 'uid'), unpack_callback,
                                   **kwargs)
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
    """Create local URIs for the received files."""
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
    """Replace messate metadata with itmes in kwargs dict."""
    for key in msg.data:
        if key in kwargs:
            replacement = dict(item.split(':')
                               for item in kwargs[key].split('|'))
            msg.data[key] = replacement[msg.data[key]]
    return msg


def send_request(msg, req, timeout):
    """Send a request for push."""
    LOGGER.debug("Send and recv timeout is %.2f seconds", timeout)

    hostname, port = msg.data["request_address"].split(":")
    requester = PushRequester(hostname, int(port))
    return requester.send_and_recv(req, timeout=timeout), hostname


def send_ack(msg, timeout):
    """Send an ACK (no push required)."""
    req = Message(msg.subject, 'ack', data=msg.data)
    LOGGER.debug("Sending: %s", str(req))

    response, hostname = send_request(msg, req, timeout)

    if response and response.type == "ack":
        pass
    else:
        LOGGER.error("Failed to get valid response from server %s: %s",
                     str(hostname), str(response))


def terminate_transfers(uid, timeout):
    """Send ACK to remaining sources for uid and remove from the ongoing tranfers list."""
    msgs = clean_ongoing_transfer(uid)
    for msg in msgs:
        send_ack(msg, timeout)


def get_msg_uid(msg):
    """Compute the uid of the message."""
    filenames = sorted(gen_dict_extract(msg.data, 'uid'))
    m = hashlib.md5()
    for filename in filenames:
        m.update(filename.encode('utf-8'))
    return m.hexdigest()


def iterate_messages(uid):
    """Iterate over all messages for a uid."""
    while True:
        try:
            msg = get_next_msg(uid)
        except IndexError:
            return
        yield msg


def get_next_msg(uid):
    """Get the next message with this *uid* from the available sources."""
    with ongoing_transfers_lock:
        return ongoing_transfers[uid].pop(0)


def add_timer(timeout, callback, msg, *args, **kwargs):
    """Add a timer for hot spare."""
    huid = get_msg_uid(msg)
    cargs = [msg] + list(args)
    with hot_spare_timer_lock:
        timer = CTimer(timeout, callback, args=cargs, kwargs=kwargs)
        ongoing_hot_spare_timers[huid] = timer
        ongoing_hot_spare_timers[huid].start()
    LOGGER.debug("Added timer for UID %s.", huid)


def add_to_ongoing(msg):
    """Add message to ongoing transfers.

    Return True if similar message was already received, False otherwise.
    """
    huid = get_msg_uid(msg)
    with hot_spare_timer_lock:
        timer = ongoing_hot_spare_timers.pop(huid, None)
        if timer is not None:
            timer.cancel()
            LOGGER.debug("Cleared timer for UID %s.", huid)
    with ongoing_transfers_lock:
        if huid in ongoing_transfers:
            ongoing_transfers[huid].append(msg)
            return None
        ongoing_transfers[huid] = [msg]
        return huid


def add_to_file_cache(msg):
    """Add files in the message to received file cache."""
    with cache_lock:
        for uid in gen_dict_extract(msg.data, 'uid'):
            if uid not in file_cache:
                LOGGER.debug("Add %s to file cache", str(uid))
                file_cache.append(uid)


def request_push(msg_in, destination, login=None, publisher=None, **kwargs):
    """Request a push for data."""
    huid = add_to_ongoing(msg_in)
    if huid is None:
        return

    if already_received(msg_in):
        timeout = float(kwargs["req_timeout"])
        send_ack(msg_in, timeout)
        _ = clean_ongoing_transfer(huid)
        return

    for msg in iterate_messages(huid):
        try:
            _destination = compose(destination, msg.data)
        except KeyError as ke:
            LOGGER.error("Format identifier is missing from the msg.data: %s", str(ke))
            raise
        except ValueError as ve:
            LOGGER.error("Type of format identifier doesn't match the type in m msg.data: %s", str(ve))
            raise
        except AttributeError as ae:
            LOGGER.error("msg or msg.data is None: %s", str(ae))
            raise
        req, fake_req = create_push_req_message(msg, _destination, login)
        LOGGER.info("Requesting: %s", str(fake_req))
        timeout = float(kwargs["transfer_req_timeout"])
        local_dir = create_local_dir(_destination, kwargs.get('ftp_root', '/'))

        publisher.send(str(fake_req))
        response, hostname = send_request(msg, req, timeout)

        if response and response.type in ['file', 'collection', 'dataset']:
            LOGGER.debug("Server done sending file")
            add_to_file_cache(msg)
            # Send an 'ack' message so that possible hot spares know
            # the primary has completed the request
            msg = Message(msg.subject, 'ack', msg.data)
            LOGGER.debug("Sending a public 'ack' of completed transfer: %s",
                         str(msg))
            publisher.send(str(msg))
            try:
                lmsg = unpack_and_create_local_message(response, local_dir,
                                                       **kwargs)
            except IOError:
                LOGGER.exception("Couldn't unpack %s", str(response))
                continue
            if publisher:
                lmsg = make_uris(lmsg, _destination, login)
                lmsg.data['origin'] = response.data['request_address']
                lmsg.data.pop('request_address', None)
                lmsg = replace_mda(lmsg, kwargs)
                lmsg.data.pop('destination', None)

                LOGGER.debug("publishing %s", str(lmsg))
                publisher.send(str(lmsg))
            terminate_transfers(huid, float(kwargs["req_timeout"]))
            break
        else:
            LOGGER.error("Failed to get valid response from server %s: %s",
                         str(hostname), str(response))
    else:
        LOGGER.warning('Could not get a working source for requesting %s',
                       str(msg))
        terminate_transfers(huid, float(kwargs["req_timeout"]))


class Chain(Thread):
    """The Chain class."""

    def __init__(self, name, config):
        """Init a chain object."""
        super(Chain, self).__init__()
        self._config = config
        self._name = name
        self._np = None
        self.publisher = None
        self.listeners = {}
        self.listener_died_event = Event()
        self.running = True

        # Setup publisher
        try:
            nameservers = self._config["nameservers"]
            if nameservers:
                nameservers = nameservers.split()
            self._np = NoisyPublisher(
                "move_it_" + self._name,
                port=self._config["publish_port"],
                nameservers=nameservers)
            self.publisher = self._np.start()
        except (KeyError, NameError):
            pass

    def setup_listeners(self, callback):
        """Set up the listeners."""
        self.callback = callback
        try:
            topics = []
            if "topic" in self._config:
                topics.append(self._config["topic"])
            if self._config.get("heartbeat", False):
                topics.append(SERVER_HEARTBEAT_TOPIC)
                # Subscribe also to heartbeat messages of other clients
                topics.append(CLIENT_HEARTBEAT_TOPIC_BASE + '_' + self._name)
            for provider in self._config["providers"]:
                if '/' in provider.split(':')[-1]:
                    parts = urlparse(provider)
                    if parts.scheme != '':
                        provider = urlunparse((parts.scheme, parts.netloc,
                                               '', '', '', ''))
                    else:
                        # If there's no scheme, urlparse thinks the
                        # URI is a local file
                        provider = urlunparse(('tcp', parts.path,
                                               '', '', '', ''))
                    topics.append(parts.path)
                LOGGER.debug("Add listener for %s with topic %s",
                             provider, str(topics))
                listener = Listener(
                    provider,
                    topics,
                    callback,
                    publisher=self.publisher,
                    die_event=self.listener_died_event,
                    **self._config)
                listener.start()
                self.listeners[provider] = listener
        except Exception as err:
            LOGGER.exception(str(err))
            raise

    def restart_dead_listeners(self):
        """Restart dead listeners."""
        plural = ['', 's']
        for provider in list(self.listeners.keys()):
            if not self.listeners[provider].is_alive():
                cause_of_death = self.listeners[provider].cause_of_death
                death_count = self.listeners[provider].death_count
                while death_count < 3:
                    LOGGER.error("Listener for %s died %d time%s: %s", provider, death_count + 1,
                                 plural[min(death_count, 1)], str(cause_of_death))
                    self.listeners[provider] = self.listeners[provider].restart()
                    time.sleep(.5)
                    if not self.listeners[provider].is_alive():
                        death_count = self.listeners[provider].death_count
                        cause_of_death = self.listeners[provider].cause_of_death
                    else:
                        break
                if death_count >= 3:
                    with suppress(Exception):
                        self.listeners[provider].stop()
                    del self.listeners[provider]
                    LOGGER.critical("Listener for %s switched off: %s", provider, str(cause_of_death))

    def run(self):
        """Monitor the listeners."""
        try:
            while self.running:
                if self.listener_died_event.wait(LISTENER_CHECK_INTERVAL):
                    self.restart_dead_listeners()
                    self.listener_died_event.clear()
        except Exception:
            LOGGER.exception("Chain %s died!", self._name)

    def config_equals(self, other_config):
        """Check that current config is the same as `other_config`."""
        for key, val in other_config.items():
            if ((key not in ["listeners", "publisher"]) and
                ((key not in self._config) or
                    (self._config[key] != val))):
                return False
        return True

    def reset_listeners(self):
        """Reset the listeners."""
        for listener in self.listeners.values():
            listener.stop()
        self.listeners = {}

    def stop(self):
        """Stop the chain."""
        self.running = False
        if self._np:
            self._np.stop()
        self.reset_listeners()

    def restart(self):
        """Restart the chain, return a new running instance."""
        self.stop()
        new_chain = self.__class__(self._name, self._config)
        new_chain.setup_listeners(self.callback)
        new_chain.start()
        return new_chain


def reload_config(filename, chains, callback=request_push):
    """Rebuild chains if needed (if the configuration changed) from *filename*."""
    LOGGER.debug("New config file detected: %s", filename)

    new_configs = read_config(filename)

    # setup new chains

    for key, new_config in new_configs.items():
        if key in chains:
            if chains[key].config_equals(new_config):
                continue

            chains[key].stop()
            verb = "Updated"
            LOGGER.debug("Updating %s", key)
        else:
            verb = "Added"
            LOGGER.debug("Adding %s", key)
        chains[key] = Chain(key, new_config)
        chains[key].setup_listeners(callback)
        chains[key].start()

        LOGGER.debug("%s %s", verb, key)

    # disable old chains

    for key in (set(chains.keys()) - set(new_configs.keys())):
        chains[key].stop()

        del chains[key]
        LOGGER.debug("Removed %s", key)

    LOGGER.debug("Reloaded config from %s", filename)


class PushRequester(object):
    """Base requester class."""

    request_retries = 3

    def __init__(self, host, port):
        """Initialize pish request."""
        self._socket = None
        self._reqaddress = "tcp://" + host + ":" + str(port)
        self._poller = Poller()
        self._lock = Lock()
        self.failures = 0
        self.jammed = False
        self.running = True

        self.connect()

    def connect(self):
        """Connect to the server."""
        self._socket = get_context().socket(REQ)
        # self._socket.setsockopt(zmq.RCVTIMEO, 500)  # milliseconds
        self._socket.connect(self._reqaddress)
        self._poller.register(self._socket, POLLIN)

    def stop(self):
        """Close the connection to the server."""
        self.running = False
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()
        self._poller.unregister(self._socket)

    def reset_connection(self):
        """Reset the socket."""
        self.stop()
        self.connect()

    def __del__(self, *args, **kwargs):
        """Stop the push requester when deleted."""
        self.stop()

    def send_and_recv(self, msg, timeout=DEFAULT_REQ_TIMEOUT):
        """Send a message and receive a response."""
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
    """Handle events with a generic *fun* function."""

    def __init__(self, fun, *args, **kwargs):
        """Initialize handler."""
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._fun = fun

    def process_IN_CLOSE_WRITE(self, event):
        """Process on closing after writing."""
        self._fun(event.pathname)

    def process_IN_CREATE(self, event):
        """Process on closing after linking."""
        try:
            if os.stat(event.pathname).st_nlink > 1:
                self._fun(event.pathname)
        except OSError:
            return

    def process_IN_MOVED_TO(self, event):
        """Process on closing after moving."""
        self._fun(event.pathname)


class StatCollector(object):
    """StatCollector class."""

    def __init__(self, statfile):
        """Initialize collector."""
        self.statfile = statfile

    def collect(self, msg, *args, **kwargs):
        """Collect."""
        with open(self.statfile, 'a') as fd:
            fd.write(time.asctime() + " - " + str(msg) + "\n")


def terminate(chains):
    """Terminate client chains."""
    for chain in six.itervalues(chains):
        chain.stop()
    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it_client."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)
