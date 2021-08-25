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

import bz2
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

import pyinotify
from zmq import NOBLOCK, POLLIN, PULL, PUSH, ROUTER, Poller, ZMQError
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from posttroll import get_context
from posttroll.message import Message
from posttroll.publisher import get_own_ip
from posttroll.subscriber import Subscribe
from configparser import RawConfigParser
from queue import Empty, Queue
from six.moves.urllib.parse import urlparse
from trollmoves.client import DEFAULT_REQ_TIMEOUT
from trollmoves.movers import move_it
from trollmoves.utils import (clean_url, gen_dict_contains, gen_dict_extract,
                              is_file_local)
from trollsift import globify, parse

LOGGER = logging.getLogger(__name__)


file_cache = deque(maxlen=61000)
file_cache_lock = Lock()
START_TIME = datetime.datetime.utcnow()


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

        self._loop = True
        self.out_socket = get_context().socket(ROUTER)
        self.out_socket.bind("tcp://*:" + str(port))
        self.port = port
        self.in_socket = get_context().socket(PULL)
        self.in_socket.bind("inproc://replies" + str(port))

        self._poller = Poller()
        self._poller.register(self.out_socket, POLLIN)
        self._poller.register(self.in_socket, POLLIN)
        self._attrs = attrs
        try:
            # Checking the validity of the file pattern
            globify(attrs["origin"])
        except ValueError as err:
            raise ConfigError('Invalid file pattern: ' + str(err))
        except KeyError:
            if 'listen' not in attrs:
                raise
        self._deleter = Deleter(attrs)

        try:
            self._station = self._attrs["station"]
        except (KeyError, TypeError):
            LOGGER.warning("Station is not defined in config file")
            self._station = "unknown"
        LOGGER.debug("Station is '%s'", self._station)

    def start(self):
        """Start the request manager."""
        self._deleter.start()
        Thread.start(self)

    def pong(self, message):
        """Reply to ping."""
        return Message(message.subject, "pong", {"station": self._station})

    def push(self, message):
        """Reply to push request."""
        for the_dict in gen_dict_contains(message.data, 'uri'):
            uri = urlparse(the_dict['uri'])
            rel_path = the_dict.get('path', None)
            pathname = uri.path
            # FIXME: check against file_cache
            if 'origin' in self._attrs and not fnmatch.fnmatch(
                    os.path.basename(pathname),
                    os.path.basename(globify(self._attrs["origin"]))):
                LOGGER.warning('Client trying to get invalid file: %s', pathname)
                return Message(message.subject,
                               "err",
                               data="{0:s} not reachable".format(pathname))
            try:
                move_it(pathname, message.data['destination'], self._attrs, rel_path=rel_path)
            except Exception as err:
                return Message(message.subject, "err", data=str(err))
            else:
                if (self._attrs.get('compression') or self._attrs.get(
                        'delete', 'False').lower() in ["1", "yes", "true", "on"]):
                    self._deleter.add(pathname)

            if 'dataset' in message.data:
                mtype = 'dataset'
            elif 'collection' in message.data:
                mtype = 'collection'
            elif 'uid' in message.data:
                mtype = 'file'
            else:
                raise KeyError('No known metadata in message.')

        new_msg = Message(message.subject,
                          mtype,
                          data=message.data.copy())
        new_msg.data['destination'] = clean_url(new_msg.data[
            'destination'])
        return new_msg

    def ack(self, message):
        """Reply with ack to a publication."""
        for url in gen_dict_extract(message.data, 'uri'):
            uri = urlparse(url)
            pathname = uri.path

            if 'origin' in self._attrs and not fnmatch.fnmatch(
                    os.path.basename(pathname),
                    os.path.basename(globify(self._attrs["origin"]))):
                LOGGER.warning('Client trying to get invalid file: %s', pathname)
                return Message(message.subject,
                               "err",
                               data="{0:s} not reacheable".format(pathname))

            if (self._attrs.get('compression') or self._attrs.get(
                    'delete', 'False').lower() in ["1", "yes", "true", "on"]):
                self._deleter.add(pathname)
        new_msg = Message(message.subject, "ack", data=message.data.copy())
        try:
            new_msg.data['destination'] = clean_url(new_msg.data[
                'destination'])
        except KeyError:
            pass
        return new_msg

    def info(self, message):
        """Collect information from file cache to message."""
        topic = message.subject
        max_count = 2256  # Let's set a (close to arbitrary) limit on messages size.
        try:
            max_count = min(message.data.get("max_count", max_count), max_count)
        except AttributeError:
            pass
        uptime = datetime.datetime.utcnow() - START_TIME
        files = []
        with file_cache_lock:
            for i in file_cache:
                if i.startswith(topic):
                    files.append(i)
                    if len(files) == max_count:
                        break
        return Message(message.subject, "info", data={"files": files, "max_count": max_count, "uptime": str(uptime)})

    def unknown(self, message):
        """Reply to any unknown request."""
        return Message(message.subject, "unknown")

    def reply_and_send(self, fun, address, message):
        """Reply to request."""
        in_socket = get_context().socket(PUSH)
        in_socket.connect("inproc://replies" + str(self.port))

        reply = Message(message.subject, "error")
        try:
            reply = fun(message)
        except Exception:
            LOGGER.exception("Something went wrong"
                             " when processing the request: %s", str(message))
        finally:
            LOGGER.debug("Response: %s", str(reply))
            try:
                in_socket.send_multipart([address, b'', str(reply)])
            except TypeError:
                in_socket.send_multipart([address, b'', bytes(str(reply),
                                                              'utf-8')])

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
                        Thread(target=self.reply_and_send,
                               args=(self.unknown, address, message)).start()
                        LOGGER.warning("Sent error message back.")
                    continue

                message = Message(rawstr=payload)
                fake_msg = Message(rawstr=str(message))
                try:
                    urlparse(message.data['destination'])
                except (KeyError, TypeError):
                    pass
                else:
                    fake_msg.data['destination'] = clean_url(message.data[
                        'destination'])

                LOGGER.debug("processing request: %s", str(fake_msg))
                if message.type == "ping":
                    Thread(target=self.reply_and_send,
                           args=(self.pong, address, message)).start()
                elif message.type == "push":
                    Thread(target=self.reply_and_send,
                           args=(self.push, address, message)).start()
                elif message.type == "ack":
                    Thread(target=self.reply_and_send,
                           args=(self.ack, address, message)).start()
                elif message.type == "info":
                    Thread(target=self.reply_and_send,
                           args=(self.info, address, message)).start()
                else:  # unknown request
                    Thread(target=self.reply_and_send,
                           args=(self.unknown, address, message)).start()
            elif socks.get(self.in_socket) == POLLIN:
                self.out_socket.send_multipart(
                    self.in_socket.recv_multipart(NOBLOCK))

            else:  # timeout
                pass

    def stop(self):
        """Stop the request manager."""
        self._loop = False
        self._deleter.stop()
        self.out_socket.close(1)
        self.in_socket.close(1)


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
            for msg in sub.recv(1):
                if msg is None:
                    if not self.loop:
                        break
                    else:
                        continue

                # check that files are local
                for uri in gen_dict_extract(msg.data, 'uri'):
                    urlobj = urlparse(uri)
                    if not is_file_local(urlobj):
                        break
                else:
                    LOGGER.debug('We have a match: %s', str(msg))

                    # pathname = unpack(orig_pathname, **attrs)

                    info = self.attrs.get("info", {})
                    if info:
                        info = dict((elt.strip().split('=') for elt in info.split(";")))
                        for infokey, infoval in info.items():
                            if "," in infoval:
                                info[infokey] = infoval.split(",")

                    # info.update(parse(attrs["origin"], orig_pathname))
                    # info['uri'] = pathname
                    # info['uid'] = os.path.basename(pathname)
                    info.update(msg.data)
                    info['request_address'] = self.attrs.get(
                        "request_address", get_own_ip()) + ":" + self.attrs["request_port"]
                    old_data = msg.data
                    msg = Message(self.attrs["topic"], msg.type, info)
                    self.publisher.send(str(msg))
                    with file_cache_lock:
                        for filename in gen_dict_extract(old_data, 'uid'):
                            file_cache.appendleft(self.attrs["topic"] + '/' + filename)
                    LOGGER.debug("Message sent: %s", str(msg))
                    if not self.loop:
                        break

    def stop(self):
        """Stop the listener."""
        self.loop = False


def create_posttroll_notifier(attrs, publisher):
    """Create a notifier listening to posttroll messages from *attrs*."""
    listener = Listener(attrs, publisher)

    return listener, None


def process_notify(orig_pathname, publisher, pattern, attrs):
    """Publish what we have."""
    if not fnmatch.fnmatch(orig_pathname, pattern):
        return
    elif (os.stat(orig_pathname).st_size == 0):
        # Want to avoid files with size 0.
        LOGGER.debug("Ignoring empty file: %s", orig_pathname)
        return
    else:
        LOGGER.debug('We have a match: %s', orig_pathname)

    pathname = unpack(orig_pathname, **attrs)

    info = attrs.get("info", {})
    if info:
        info = dict((elt.strip().split('=') for elt in info.split(";")))
        for infokey, infoval in info.items():
            if "," in infoval:
                info[infokey] = infoval.split(",")

    info.update(parse(attrs["origin"], orig_pathname))
    info['uri'] = pathname
    info['uid'] = os.path.basename(pathname)
    info['request_address'] = attrs.get(
        "request_address", get_own_ip()) + ":" + attrs["request_port"]
    msg = Message(attrs["topic"], 'file', info)
    publisher.send(str(msg))
    with file_cache_lock:
        file_cache.appendleft(attrs["topic"] + '/' + info["uid"])
    LOGGER.debug("Message sent: %s", str(msg))


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


def read_config(filename):
    """Read the config file called *filename*."""
    cp_ = RawConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)
        res[section].setdefault("req_timeout", DEFAULT_REQ_TIMEOUT)
        res[section].setdefault("transfer_req_timeout", 10 * DEFAULT_REQ_TIMEOUT)
        res[section].setdefault("ssh_key_filename", None)
        if ("origin" not in res[section]) and ('listen' not in res[section]):
            LOGGER.warning("Incomplete section %s: add an 'origin' or "
                           "'listen' item.", section)
            LOGGER.info("Ignoring section %s: incomplete.", section)
            del res[section]
            continue

        # if "publisher_port" not in res[section]:
        #    LOGGER.warning("Incomplete section " + section
        #                   + ": add an 'publisher_port' item.")
        #    LOGGER.info("Ignoring section " + section
        #                + ": incomplete.")
        #    del res[section]
        #    continue

        if "topic" not in res[section]:
            LOGGER.warning("Incomplete section %s: add an 'topic' item.",
                           section)
            LOGGER.info("Ignoring section %s: incomplete.",
                        section)
            continue
        else:
            try:
                res[section]["publish_port"] = int(res[section][
                    "publish_port"])
            except (KeyError, ValueError):
                res[section]["publish_port"] = 0
    return res


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

    old_glob = []

    for key, val in new_chains.items():
        identical = True
        if key in chains:
            for key2, val2 in new_chains[key].items():
                if ((key2 not in ["notifier", "publisher"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    break
            if identical:
                continue

            chains[key]["notifier"].stop()
            try:
                # Join the Watchdog thread
                chains[key]["notifier"].join()
            except AttributeError:
                pass
            if "request_manager" in chains[key]:
                chains[key]["request_manager"].stop()
                LOGGER.debug('Stopped reqman')

        chains[key] = val.copy()
        try:
            chains[key]["request_manager"] = manager(
                int(val["request_port"]), val)
            LOGGER.debug("Created request manager on port %s",
                         val["request_port"])
        except (KeyError, NameError):
            LOGGER.exception('In reading config')
        except ConfigError as err:
            LOGGER.error('Invalid config parameters in %s: %s', key, str(err))
            LOGGER.warning('Remove and skip %s', key)
            del chains[key]
            continue

        if notifier_builder is None:
            if 'origin' in val:
                if use_watchdog:
                    LOGGER.info("Using Watchdog notifier")
                    notifier_builder = create_watchdog_notifier
                else:
                    LOGGER.info("Using inotify notifier")
                    notifier_builder = create_inotify_notifier
            elif 'listen' in val:
                notifier_builder = create_posttroll_notifier

        chains[key]["notifier"], fun = notifier_builder(val, publisher)
        chains[key]["request_manager"].start()
        chains[key]["notifier"].start()
        if 'origin' in val:
            old_glob.append((globify(val["origin"]), fun, val))

        if not identical:
            LOGGER.debug("Updated %s", key)
        else:
            LOGGER.debug("Added %s", key)

    for key in (set(chains.keys()) - set(new_chains.keys())):
        chains[key]["notifier"].stop()
        try:
            # Join the Watchdog thread
            chains[key]["notifier"].join()
        except AttributeError:
            pass
        del chains[key]
        LOGGER.debug("Removed %s", key)

    LOGGER.debug("Reloaded config from %s", filename)
    if old_glob and not disable_backlog:
        time.sleep(3)
        for pattern, fun, attrs in old_glob:
            process_old_files(pattern, fun, publisher, attrs)

    LOGGER.debug("done reloading config")

# Unpackers

# xrit


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


# bzip

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


def process_old_files(pattern, fun, publisher, kwargs):
    """Process files from *pattern* with function *fun*."""
    fnames = glob.glob(pattern)
    if fnames:
        # time.sleep(3)
        LOGGER.debug("Touching old files")
        for fname in fnames:
            if os.path.exists(fname):
                fun(fname, publisher, pattern, kwargs)


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
