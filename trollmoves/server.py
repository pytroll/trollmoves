#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2020
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

"""Classes and functions for Trollmoves server."""

import datetime
import errno
import fnmatch
import glob
import logging
import logging.handlers
import os
import subprocess
import sys
import tempfile
import time
from collections import deque
from threading import Lock, Thread
from configparser import RawConfigParser
from queue import Empty, Queue
from urllib.parse import urlparse
import signal

import bz2
import pyinotify
from zmq import NOBLOCK, POLLIN, PULL, PUSH, ROUTER, Poller, ZMQError
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from posttroll import get_context
from posttroll.message import Message
from posttroll.publisher import get_own_ip
from posttroll.subscriber import Subscribe
from trollsift import globify, parse

from trollmoves.client import DEFAULT_REQ_TIMEOUT
from trollmoves.movers import move_it
from trollmoves.utils import (clean_url, gen_dict_contains, gen_dict_extract,
                              is_file_local)
from trollmoves.move_it_base import MoveItBase, create_publisher, EventHandler

LOGGER = logging.getLogger(__name__)


file_cache = deque(maxlen=61000)
file_cache_lock = Lock()
START_TIME = datetime.datetime.utcnow()


class MoveItServer(MoveItBase):
    """Wrapper class for Trollmoves Server."""

    def __init__(self, cmd_args):
        """Initialize server."""
        publisher = create_publisher(cmd_args.port, "move_it_server")
        super(MoveItServer, self).__init__(cmd_args, "server", publisher=publisher)

    def run(self):
        """Start the transfer chains."""
        signal.signal(signal.SIGTERM, self.chains_stop)
        signal.signal(signal.SIGHUP, self.signal_reload_cfg_file)
        self.notifier.start()
        self.running = True
        while self.running:
            time.sleep(1)
            self.publisher.heartbeat(30)


class ConfigError(Exception):
    """Configuration error."""

    pass


class Deleter(Thread):
    """Class for deleting moved files."""

    def __init__(self, attrs):
        """Initialize Deleter."""
        Thread.__init__(self)
        self.queue = Queue()
        self.timer = None
        self.loop = True
        self._attrs = attrs or dict()

    def add(self, filename):
        """Schedule file for deletion."""
        remove_delay = int(self._attrs.get('remove_delay', 30))
        LOGGER.debug('Scheduling %s for removal in %ds', filename, remove_delay)
        self.queue.put((filename, time.time() + remove_delay))

    def run(self):
        """Start the deleter."""
        while self.loop:
            try:
                filename, the_time = self.queue.get(True, 2)
            except Empty:
                continue
            while self.loop:
                time.sleep(min(2, max(the_time - time.time(), 0)))
                if the_time <= time.time():
                    try:
                        self.delete(filename)
                    except Exception:
                        LOGGER.exception(
                            'Something went wrong when deleting %s', filename)
                    else:
                        LOGGER.debug('Removed %s.', filename)
                    break

    @staticmethod
    def delete(filename):
        """Delete the given file.

        If the file is not present, this function does *not* raise an error.
        """
        try:
            os.remove(filename)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            LOGGER.debug("File already deleted: %s", filename)

    def stop(self):
        """Stop the deleter."""
        self.loop = False
        if self.timer:
            self.timer.cancel()


class RequestManager(Thread):
    """Manage requests."""

    def __init__(self, port, attrs=None):
        """Initialize request manager."""
        Thread.__init__(self)

        self.port = port
        self._attrs = attrs
        self._loop = True
        self.out_socket = None
        self.in_socket = None
        self._poller = None
        self._station = None

        self._validate_file_pattern()
        self._set_out_socket()
        self._set_in_socket()
        self._set_station()
        self._create_poller()
        self._deleter = Deleter(attrs)

    def _set_out_socket(self):
        self.out_socket = get_context().socket(ROUTER)
        self.out_socket.bind("tcp://*:" + str(self.port))

    def _set_in_socket(self):
        self.in_socket = get_context().socket(PULL)
        self.in_socket.bind("inproc://replies" + str(self.port))

    def _set_station(self):
        try:
            self._station = self._attrs["station"]
        except (KeyError, TypeError):
            LOGGER.warning("Station is not defined in config file")
            self._station = "unknown"
        LOGGER.debug("Station is '%s'", self._station)

    def _create_poller(self):
        self._poller = Poller()
        self._poller.register(self.out_socket, POLLIN)
        self._poller.register(self.in_socket, POLLIN)

    def _validate_file_pattern(self):
        try:
            _ = globify(self._attrs["origin"])
        except ValueError as err:
            raise ConfigError('Invalid file pattern: ' + str(err))
        except KeyError:
            if 'listen' not in self._attrs:
                raise

    def start(self):
        """Start the request manager."""
        self._deleter.start()
        Thread.start(self)

    def pong(self, message):
        """Reply to ping."""
        return Message(message.subject, "pong", {"station": self._station})

    def push(self, message):
        """Reply to push request."""
        new_msg = self._move_files(message)
        if new_msg is None:
            new_msg = Message(message.subject,
                              _get_push_message_type(message),
                              data=message.data.copy())
            new_msg.data['destination'] = clean_url(new_msg.data['destination'])

        return new_msg

    def _move_files(self, message):
        error_message = None
        for data in gen_dict_contains(message.data, 'uri'):
            pathname = urlparse(data['uri']).path
            rel_path = data.get('path', None)
            error_message = self._validate_requested_file(pathname, message)
            if error_message is not None:
                break
            error_message = self._move_file(pathname, message, rel_path)
            if error_message is not None:
                break

        return error_message

    def _validate_requested_file(self, pathname, message):
        # FIXME: check against file_cache
        if 'origin' in self._attrs and not fnmatch.fnmatch(
                os.path.basename(pathname),
                os.path.basename(globify(self._attrs["origin"]))):
            LOGGER.warning('Client trying to get invalid file: %s', pathname)
            return Message(message.subject, "err", data="{0:s} not reachable".format(pathname))
        return None

    def _move_file(self, pathname, message, rel_path):
        error_message = None
        try:
            move_it(pathname, message.data['destination'], self._attrs, rel_path=rel_path)
        except Exception as err:
            error_message = Message(message.subject, "err", data=str(err))
        else:
            self._add_to_deleter(pathname)
        return error_message

    def _add_to_deleter(self, pathname):
        if self._attrs.get('compression') or self._is_delete_set():
            self._deleter.add(pathname)

    def _is_delete_set(self):
        return self._attrs.get('delete', 'False').lower() in ["1", "yes", "true", "on"]

    def ack(self, message):
        """Reply with ack to a publication."""
        new_msg = None
        for url in gen_dict_extract(message.data, 'uri'):
            pathname = urlparse(url).path
            new_msg = self._validate_requested_file(pathname, message)
            if new_msg is not None:
                break
            self._add_to_deleter(pathname)

        if new_msg is None:
            new_msg = _get_cleaned_ack_message(message)

        return new_msg

    def info(self, message):
        """Collect information from file cache to message."""
        uptime = datetime.datetime.utcnow() - START_TIME
        files, max_count = _collect_cached_files(message)

        return Message(message.subject, "info", data={"files": files, "max_count": max_count, "uptime": str(uptime)})

    def unknown(self, message):
        """Reply to any unknown request."""
        return Message(message.subject, "unknown")

    def reply_and_send(self, fun, address, message):
        """Reply to request."""
        reply = Message(message.subject, "error")
        try:
            reply = fun(message)
        except Exception:
            LOGGER.exception("Something went wrong"
                             " when processing the request: %s", str(message))
        finally:
            self._send_multipart_reply(reply, address)

    def _send_multipart_reply(self, reply, address):
        LOGGER.debug("Response: %s", str(reply))
        in_socket = get_context().socket(PUSH)
        in_socket.connect("inproc://replies" + str(self.port))
        try:
            in_socket.send_multipart([address, b'', str(reply)])
        except TypeError:
            in_socket.send_multipart([address, b'', bytes(str(reply), 'utf-8')])

    def run(self):
        """Run request manager."""
        try:
            self._run()
        except Exception:
            LOGGER.exception("Request Manager died.")

    def _run(self):
        """Run request manager."""
        while self._loop:
            try:
                socks = dict(self._poller.poll(timeout=2000))
            except ZMQError:
                LOGGER.info("Poller interrupted.")
                continue
            if socks.get(self.out_socket) == POLLIN:
                address, payload = self._get_address_and_payload()
                if payload is None:
                    continue
                self._process_request(Message(rawstr=payload), address)
            elif socks.get(self.in_socket) == POLLIN:
                self.out_socket.send_multipart(self.in_socket.recv_multipart(NOBLOCK))

    def _get_address_and_payload(self):
        address, payload = None, None
        LOGGER.debug("Received a request")
        multiparts = self.out_socket.recv_multipart(NOBLOCK)
        try:
            address, _, payload = multiparts
        except ValueError:
            LOGGER.warning("Invalid request.")
            try:
                address = multiparts[0]
            except (TypeError, IndexError):
                LOGGER.warning("Address unknown, not sending an error message back.")
            else:
                message = Message('error', 'error', "Invalid message received")
                Thread(target=self.reply_and_send, args=(self.unknown, address, message)).start()
                LOGGER.warning("Sent error message back.")
        return address, payload

    def _process_request(self, message, address):
        LOGGER.debug("processing request: %s", str(_sanitize_message_destination(message)))
        if message.type == "ping":
            Thread(target=self.reply_and_send, args=(self.pong, address, message)).start()
        elif message.type == "push":
            Thread(target=self.reply_and_send, args=(self.push, address, message)).start()
        elif message.type == "ack":
            Thread(target=self.reply_and_send, args=(self.ack, address, message)).start()
        elif message.type == "info":
            Thread(target=self.reply_and_send, args=(self.info, address, message)).start()
        else:  # unknown request
            Thread(target=self.reply_and_send, args=(self.unknown, address, message)).start()

    def stop(self):
        """Stop the request manager."""
        self._loop = False
        self._deleter.stop()
        self.out_socket.close(1)
        self.in_socket.close(1)


def _get_push_message_type(message):
    message_type = message.type
    if 'uri' in message.data:
        message_type = 'file'
    elif 'dataset' in message.data:
        message_type = 'dataset'
    elif 'collection' in message.data:
        message_type = 'collection'
    return message_type


def _get_cleaned_ack_message(message):
    new_msg = Message(message.subject, "ack", data=message.data.copy())
    try:
        new_msg.data['destination'] = clean_url(new_msg.data[
            'destination'])
    except KeyError:
        pass

    return new_msg


def _collect_cached_files(message):
    max_count = 2256  # Let's set a (close to arbitrary) limit on messages size.
    try:
        max_count = min(message.data.get("max_count", max_count), max_count)
    except AttributeError:
        pass
    files = []
    with file_cache_lock:
        for i in file_cache:
            if i.startswith(message.subject):
                files.append(i)
                if len(files) == max_count:
                    break
    return files, max_count


def _sanitize_message_destination(message):
    sanitized_message = Message(rawstr=str(message))
    try:
        _ = urlparse(message.data['destination'])
    except (KeyError, TypeError):
        pass
    else:
        sanitized_message.data['destination'] = clean_url(message.data['destination'])
    return sanitized_message


class Listener(Thread):
    """A message listener for the server."""

    def __init__(self, attrs, publisher):
        """Initialize the listener."""
        super(Listener, self).__init__()
        self.attrs = attrs
        self.publisher = publisher
        self.loop = True

    def run(self):
        """Start listening to messages."""
        with Subscribe('', topics=self.attrs['listen'], addr_listener=True) as sub:
            self._run(sub)

    def _run(self, sub):
        for msg in sub.recv(1):
            if not self.loop:
                break
            if msg is None:
                continue
            if not _files_in_message_are_local(msg):
                break
            self._send_message(msg)

    def _send_message(self, msg):
        LOGGER.debug('We have a match: %s', str(msg))
        info = self._collect_message_info(msg)
        msg = Message(self.attrs["topic"], msg.type, info)
        self.publisher.send(str(msg))
        self._add_files_to_cache(msg)
        LOGGER.debug("Message sent: %s", str(msg))

    def _collect_message_info(self, msg):
        info = _collect_attribute_info(self.attrs)
        info.update(msg.data)
        info['request_address'] = self.attrs.get(
            "request_address", get_own_ip()) + ":" + self.attrs["request_port"]
        return info

    def _add_files_to_cache(self, msg):
        with file_cache_lock:
            for filename in gen_dict_extract(msg.data, 'uid'):
                file_cache.appendleft(self.attrs["topic"] + '/' + filename)

    def stop(self):
        """Stop the listener."""
        self.loop = False


def _files_in_message_are_local(msg):
    for uri in gen_dict_extract(msg.data, 'uri'):
        urlobj = urlparse(uri)
        if not is_file_local(urlobj):
            return False
    return True


def _collect_attribute_info(attrs):
    info = attrs.get("info", {})
    if info:
        info = dict((elt.strip().split('=') for elt in info.split(";")))
        for infokey, infoval in info.items():
            if "," in infoval:
                info[infokey] = infoval.split(",")
    return info


class WatchdogHandler(FileSystemEventHandler):
    """Trigger processing on filesystem events."""

    def __init__(self, fun, publisher, pattern, attrs):
        """Initialize the processor."""
        FileSystemEventHandler.__init__(self)
        self.fun = fun
        self.publisher = publisher
        self.pattern = pattern
        self.attrs = attrs

    def on_created(self, event):
        """Process file creation."""
        self.fun(event.src_path, self.publisher, self.pattern, self.attrs)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self.fun(event.dest_path, self.publisher, self.pattern, self.attrs)


def read_config(filename):
    """Read the config file called *filename*."""
    cp_ = RawConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        _set_config_defaults(res[section])
        if not _check_origin_and_listen(res, section):
            continue
        if not _check_topic(res, section):
            continue
        _verify_publish_port(res[section])
    return res


def _set_config_defaults(conf):
    conf.setdefault("working_directory", None)
    conf.setdefault("compression", False)
    conf.setdefault("req_timeout", DEFAULT_REQ_TIMEOUT)
    conf.setdefault("transfer_req_timeout", 10 * DEFAULT_REQ_TIMEOUT)
    conf.setdefault("ssh_key_filename", None)


def _check_origin_and_listen(res, section):
    if ("origin" not in res[section]) and ('listen' not in res[section]):
        LOGGER.warning("Incomplete section %s: add an 'origin' or 'listen' item.", section)
        LOGGER.info("Ignoring section %s: incomplete.", section)
        del res[section]
        return False
    return True


def _check_topic(res, section):
    if "topic" not in res[section]:
        LOGGER.warning("Incomplete section %s: add an 'topic' item.", section)
        LOGGER.info("Ignoring section %s: incomplete.", section)
        return False
    return True


def _verify_publish_port(conf):
    try:
        conf["publish_port"] = int(conf["publish_port"])
    except (KeyError, ValueError):
        conf["publish_port"] = 0


def reload_config(filename,
                  chains,
                  notifier_builder=None,
                  manager=RequestManager,
                  publisher=None,
                  disable_backlog=False,
                  use_watchdog=False):
    """Rebuild chains if needed (if the configuration changed) from *filename*."""
    LOGGER.debug("New config file detected: %s", filename)

    new_chains = read_config(filename)

    old_glob = _update_chains(chains, new_chains, manager, use_watchdog, publisher, notifier_builder)
    _disable_removed_chains(chains, new_chains)
    LOGGER.debug("Reloaded config from %s", filename)
    _process_old_files(old_glob, disable_backlog, publisher)
    LOGGER.debug("done reloading config")


def _update_chains(chains, new_chains, manager, use_watchdog, publisher, notifier_builder):
    old_glob = []
    for chain_name, chain in new_chains.items():
        chain_updated = False
        if chain_name in chains:
            if _chains_are_identical(chains, new_chains, chain_name):
                continue
            chain_updated = True
            _stop_chain(chains[chain_name])

        if not _add_chain(chains, chain_name, chain, manager):
            continue

        fun = _create_notifier_and_get_function(notifier_builder, chains[chain_name], use_watchdog, chain, publisher)

        if 'origin' in chain:
            old_glob.append((globify(chain["origin"]), fun, chain))

        if chain_updated:
            LOGGER.debug("Updated %s", chain_name)
        else:
            LOGGER.debug("Added %s", chain_name)

    return old_glob


def _chains_are_identical(chains, new_chains, chain_name):
    identical = True
    for config_key, config_value in new_chains[chain_name].items():
        if ((config_key not in ["notifier", "publisher"]) and
            ((config_key not in chains[chain_name]) or
                (chains[chain_name][config_key] != config_value))):
            identical = False
            break
    return identical


def _stop_chain(chain):
    chain["notifier"].stop()
    try:
        chain["notifier"].join()
    except AttributeError:
        pass
    if "request_manager" in chain:
        chain["request_manager"].stop()
        LOGGER.debug('Stopped reqman')


def _add_chain(chains, chain_name, chain, manager):
    chains[chain_name] = chain.copy()
    manager_added = _create_manager(chains, chain_name, chain, manager)
    if not manager_added:
        del chains[chain]
    return manager_added


def _create_manager(chains, chain_name, chain, manager):
    try:
        chains[chain_name]["request_manager"] = manager(int(chain["request_port"]), chain)
        LOGGER.debug("Created request manager on port %s", chain["request_port"])
    except (KeyError, NameError):
        LOGGER.exception('In reading config')
    except ConfigError as err:
        LOGGER.error('Invalid config parameters in %s: %s', chain_name, str(err))
        LOGGER.warning('Remove and skip %s', chain_name)
        return False
    chains[chain_name]["request_manager"].start()
    return True


def _create_notifier_and_get_function(notifier_builder, conf, use_watchdog, chain, publisher):
    if notifier_builder is None:
        notifier_builder = _get_notifier_builder(use_watchdog, chain)
    conf["notifier"], fun = notifier_builder(chain, publisher)
    conf["notifier"].start()

    return fun


def _get_notifier_builder(use_watchdog, val):
    if 'origin' in val:
        if use_watchdog:
            LOGGER.info("Using Watchdog notifier")
            notifier_builder = create_watchdog_notifier
        else:
            LOGGER.info("Using inotify notifier")
            notifier_builder = create_inotify_notifier
    elif 'listen' in val:
        notifier_builder = create_posttroll_notifier

    return notifier_builder


def create_watchdog_notifier(attrs, publisher):
    """Create a notifier from the specified configuration attributes *attrs*."""
    pattern = globify(attrs["origin"])
    opath = os.path.dirname(pattern)

    timeout = float(attrs.get("watchdog_timeout", 1.))
    LOGGER.debug("Watchdog timeout: %.1f", timeout)
    observer = PollingObserver(timeout=timeout)
    handler = WatchdogHandler(process_notify, publisher, pattern, attrs)

    observer.schedule(handler, opath)

    return observer, process_notify


def process_notify(orig_pathname, publisher, pattern, attrs):
    """Publish what we have."""
    if not fnmatch.fnmatch(orig_pathname, pattern):
        return
    elif os.stat(orig_pathname).st_size == 0:
        LOGGER.debug("Ignoring empty file: %s", orig_pathname)
        return
    else:
        LOGGER.debug('We have a match: %s', orig_pathname)

    pathname = unpack(orig_pathname, **attrs)
    info = _get_notify_message_info(attrs, orig_pathname, pathname)
    msg = Message(attrs["topic"], 'file', info)
    publisher.send(str(msg))
    with file_cache_lock:
        file_cache.appendleft(attrs["topic"] + '/' + info["uid"])
    LOGGER.debug("Message sent: %s", str(msg))


def _get_notify_message_info(attrs, orig_pathname, pathname):
    info = _collect_attribute_info(attrs)
    info.update(parse(attrs["origin"], orig_pathname))
    info['uri'] = pathname
    info['uid'] = os.path.basename(pathname)
    info['request_address'] = attrs.get(
        "request_address", get_own_ip()) + ":" + attrs["request_port"]
    return info


def create_inotify_notifier(attrs, publisher):
    """Create a notifier from the specified configuration attributes *attrs*."""
    tmask = (pyinotify.IN_CLOSE_WRITE |
             pyinotify.IN_MOVED_TO |
             pyinotify.IN_CREATE |
             pyinotify.IN_DELETE)

    wm_ = pyinotify.WatchManager()

    pattern = globify(attrs["origin"])
    opath = os.path.dirname(pattern)

    if 'origin_inotify_base_dir_skip_levels' in attrs:
        """If you need to inotify monitor for new directories within the origin
        this attribute tells the server how many levels to skip from the origin
        before staring to inorify monitor a directory

        Eg. origin=/tmp/{platform_name_dir}_{start_time_dir:%Y%m%d_%H%M}_{orbit_number_dir:05d}/
                   {sensor}_{platform_name}_{start_time:%Y%m%d_%H%M}_{orbit_number:05d}.{data_processing_level:3s}

        and origin_inotify_base_dir_skip_levels=-2

        this means the inotify monitor will use opath=/tmp"""
        pattern_list = pattern.split('/')
        pattern_join = os.path.join(*pattern_list[:int(attrs['origin_inotify_base_dir_skip_levels'])])
        opath = os.path.join("/", pattern_join)
        LOGGER.debug("Using %s as base path for pyinotify add_watch.", opath)

    def process_notify_publish(pathname):
        pattern = globify(attrs["origin"])
        return process_notify(pathname, publisher, pattern, attrs)

    tnotifier = pyinotify.ThreadedNotifier(
        wm_, EventHandler(process_notify_publish, watchManager=wm_, tmask=tmask))

    wm_.add_watch(opath, tmask)

    return tnotifier, process_notify


def create_posttroll_notifier(attrs, publisher):
    """Create a notifier listening to posttroll messages from *attrs*."""
    listener = Listener(attrs, publisher)

    return listener, None


def _disable_removed_chains(chains, new_chains):
    for key in (set(chains.keys()) - set(new_chains.keys())):
        chains[key]["notifier"].stop()
        try:
            # Join the Watchdog thread
            chains[key]["notifier"].join()
        except AttributeError:
            pass
        del chains[key]
        LOGGER.debug("Removed %s", key)


def _process_old_files(old_glob, disable_backlog, publisher):
    if old_glob and not disable_backlog:
        time.sleep(3)
        for pattern, fun, attrs in old_glob:
            process_old_files(pattern, fun, publisher, attrs)


def process_old_files(pattern, fun, publisher, kwargs):
    """Process files from *pattern* with function *fun*."""
    fnames = glob.glob(pattern)
    if fnames:
        # time.sleep(3)
        LOGGER.debug("Touching old files")
        for fname in fnames:
            if os.path.exists(fname):
                fun(fname, publisher, pattern, kwargs)


def xrit(pathname, destination=None, cmd="./xRITDecompress"):
    """Unpacks xrit data."""
    opath, ofile = os.path.split(pathname)
    destination = destination or tempfile.gettempdir()
    dest_url = urlparse(destination)
    expected = os.path.join((destination or opath), ofile[:-2] + "__")
    if dest_url.scheme in ("", "file"):
        check_output([cmd, pathname], cwd=(destination or opath))
    else:
        LOGGER.exception("Can not extract file %s to %s, destination "
                         "has to be local.", pathname, destination)
    LOGGER.info("Successfully extracted %s to %s", pathname, destination)
    return expected


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


BLOCK_SIZE = 1024


def bzip(origin, destination=None):
    """Unzip files."""
    ofile = os.path.split(origin)[1]
    destfile = os.path.join(destination or tempfile.gettempdir(), ofile[:-4])
    if os.path.exists(destfile):
        return destfile
    with open(destfile, "wb") as dest:
        try:
            orig = bz2.BZ2File(origin, "r")
            while True:
                block = orig.read(BLOCK_SIZE)

                if not block:
                    break
                dest.write(block)
            LOGGER.debug("Bunzipped %s to %s", origin, destfile)
        finally:
            orig.close()
    return destfile


def unpack(pathname,
           compression=None,
           working_directory=None,
           prog=None,
           delete="False",
           **kwargs):
    """Unpack *pathname*."""
    del kwargs
    if compression:
        try:
            unpack_fun = eval(compression)
            if prog is not None:
                new_path = unpack_fun(pathname, working_directory, prog)
            else:
                new_path = unpack_fun(pathname, working_directory)
        except Exception:
            LOGGER.exception("Could not decompress %s", pathname)
        else:
            if delete.lower() in ["1", "yes", "true", "on"]:
                os.remove(pathname)
            return new_path
    return pathname


def terminate(chains, publisher=None):
    """Terminate the given *chains* and stop the *publisher*."""
    for chain in chains.values():
        chain["notifier"].stop()
        try:
            # Join the Watchdog thread
            chain["notifier"].join()
        except AttributeError:
            pass
        if "request_manager" in chain:
            chain["request_manager"].stop()

    if publisher:
        publisher.stop()

    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it_server."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)
