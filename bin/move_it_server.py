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

This scripts needs pyinotify and argparse which are available on pypi, or via
pip/easy_install (on redhat systems, install the packages python-inotify.noarch
and python-argparse.noarch).  Other than this, the script doesn't need any
installation, and can be run as is. If you wish though, you can install it to
your standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  origin=/tmp/H-000-{series:_<6s}-{platform_name:_<12s}-{channel:_<9s}-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-{compression:1s}_
  publisher_port=9011
  compression=xrit
  prog=/home/a001673/usr/src/PublicDecompWT/Image/Linux_32bits/xRITDecompress
  topic=/1b/hrit/zds
  info=sensor=seviri;sublon=0
  request_port=9092
  working_directory=/tmp/unpacked

* 'origin' is the directory and pattern of files to watch for. For a description
  of the pattern format, see the trollsift documentation:
  http://trollsift.readthedocs.org/en/latest/index.html

* 'working_directory' is telling where to unpack the files before they are put
  in their final destination. This can come in handy in case the file has to be
  transfered by ftp and cannot be unpacked in the origin directory. The default
  for this parameter is the '/tmp' directory.

* Available compressions are 'xrit' and 'bzip'.

* The prog parameter is used for the 'xrit' unpacking function to know which
  external program to call for unpack xRIT files.

  .. note:: The 'xrit' unpacking function is dependent on a program that can
    unpack xRIT files. Such a program is available from the `Eumetsat.int
    <http://www.eumetsat.int/Home/Main/DataAccess/SupportSoftwareTools/index.htm?l=en>`_
    website.

* 'topic', 'publish_port', and 'info' define the messaging behaviour using posttroll. 'info' being a ';' separated
  list of 'key=value' items that has to be added to the message info.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout) w
ith the -l or --log option::

  move_it_server --log /path/to/mylogfile.log myconfig.ini

I Like To Move It Move It
I Like To Move It Move It
I Like To Move It Move It
Ya Like To (MOVE IT!)

TODO: unpacking

"""

from ConfigParser import ConfigParser
import os
from urlparse import urlparse, urlunparse
import pyinotify
import fnmatch
import shutil
import logging
import logging.handlers
import subprocess
import time
import glob
import sys
import traceback

from posttroll.publisher import Publisher, get_own_ip
from posttroll.message import Message
from posttroll import context

from trollsift import parse, globify
from threading import Thread, Lock
from zmq import Poller, REP, POLLIN, ZMQError, NOBLOCK, LINGER

LOGGER = logging.getLogger("move_it_server")

chains = {}

# Config management

#PUB = Publisher("tcp://*:9090", "move_it_server")
PUB = None
class RequestManager(Thread):

    """Manage requests.
    """

    def __init__(self, port, attrs=None):
        Thread.__init__(self)

        self._loop = True
        self._port = port
        self._lock = Lock()
        self._socket = context.socket(REP)
        self._socket.bind("tcp://*:" + str(self._port))
        self._poller = Poller()
        self._poller.register(self._socket, POLLIN)
        self._attrs = attrs

    def send(self, message):
        """Send a message
        """
        if message.binary:
            LOGGER.debug("Response: " + " ".join(str(message).split()[:6]))
        else:
            LOGGER.debug("Response: " + str(message))
        self._socket.send(str(message))

    def pong(self):
        """Reply to ping
        """
        return Message(subject, "pong", {"station": self._station})

    def push(self, message):
        """Reply to scanline request
        """
        #Thread(target=move_it, args=[message, self._attrs]).start()
        try:
            move_it(message, self._attrs)
        except Exception as err:
            return Message(message.subject, "err", data=str(err))
        return Message(message.subject, "ack", data=message.data.copy())


    def unknown(self, message):
        """Reply to any unknown request.
        """
        del message
        return Message(message.subject, "unknown")

    def run(self):
        while self._loop:
            try:
                socks = dict(self._poller.poll(timeout=2000))
            except ZMQError:
                LOGGER.info("Poller interrupted.")
                continue
            if self._socket in socks and socks[self._socket] == POLLIN:
                LOGGER.debug("Received a request, waiting for the lock")
                with self._lock:
                    message = Message(rawstr=self._socket.recv(NOBLOCK))
                    urlobj = urlparse(message.data['destination'])
                    fake_msg = Message(rawstr=str(message))
                    fake_msg.data['destination'] = urlunparse((urlobj.scheme,
                                                              urlobj.hostname,
                                                              urlobj.path,
                                                              "", "", ""))
                    LOGGER.debug("processing request: " + str(fake_msg))
                    reply = Message(message.subject, "error")
                    try:
                        if message.type == "ping":
                            reply = self.pong()
                        elif (message.type == "push"):
                            reply = self.push(message)
                        else:  # unknown request
                            reply = self.unknown(message)
                    except:
                        LOGGER.exception("Something went wrong"
                                         " when processing the request:")
                    finally:
                        self.send(reply)
                LOGGER.debug("Lock released from manager")
            else:  # timeout
                pass

    def stop(self):
        """Stop the request manager.
        """
        self._loop = False
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()


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

        if "origin" not in res[section]:
            LOGGER.warning("Incomplete section " + section
                           + ": add an 'origin' item.")
            LOGGER.info("Ignoring section " + section
                        + ": incomplete.")
            del res[section]
            continue

        #if "publisher_port" not in res[section]:
        #    LOGGER.warning("Incomplete section " + section
        #                   + ": add an 'publisher_port' item.")
        #    LOGGER.info("Ignoring section " + section
        #                + ": incomplete.")
        #    del res[section]
        #    continue

        if "topic" not in res[section]:
            LOGGER.warning("Incomplete section " + section
                           + ": add an 'topic' item.")
            LOGGER.info("Ignoring section " + section
                        + ": incomplete.")
            continue
        else:
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

    old_glob = []

    for key, val in new_chains.iteritems():
        identical = True
        if key in chains:
            for key2, val2 in new_chains[key].iteritems():
                if ((key2 not in ["notifier", "publisher"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    break
            if identical:
                continue

            chains[key]["notifier"].stop()
            if "request_manager" in chains[key]:
                chains[key]["request_manager"].stop()

        chains[key] = val.copy()
        try:
            chains[key]["request_manager"] = RequestManager(int(val["request_port"]), val)
            LOGGER.debug("Created request manager on port %s", val["request_port"])
        except (KeyError, NameError):
            pass
        chains[key]["notifier"] = create_notifier(val)
        chains[key]["request_manager"].start()
        chains[key]["notifier"].start()
        old_glob.append(globify(val["origin"]))

        if "publisher" in chains[key]:
            def copy_hook(pathname, dest, val=val, pub=pub):
                fname = os.path.basename(pathname)

                destination = urlunparse((dest.scheme,
                                          dest.hostname,
                                          os.path.join(dest.path, fname),
                                          dest.params,
                                          dest.query,
                                          dest.fragment))
                info = val.get("info", "")
                if info:
                    info = dict((elt.strip().split('=')
                                 for elt in info.split(";")))
                    for infokey, infoval in info.items():
                        if "," in infoval:
                            info[infokey] = infoval.split(",")
                else:
                    info = {}
                info.update(parse(val["origin"], pathname))
                info['uri'] = destination
                info['uid'] = fname
                msg = Message(val["topic"], 'file', info)
                pub.send(str(msg))
                LOGGER.debug("Message sent bla: " + str(msg))

            chains[key]["copy_hook"] = copy_hook

            def delete_hook(pathname, val=val, pub=pub):
                fname = os.path.basename(pathname)
                info = val.get("info", "")
                if info:
                    info = dict((elt.strip().split('=')
                                 for elt in info.split(";")))
                info['uri'] = pathname
                info['uid'] = fname
                msg = Message(val["topic"], 'del', info)
                pub.send(str(msg))
                LOGGER.debug("Message sent: " + str(msg))

            chains[key]["delete_hook"] = delete_hook

        if not identical:
            LOGGER.debug("Updated " + key)
        else:
            LOGGER.debug("Added " + key)

    for key in (set(chains.keys()) - set(new_chains.keys())):
        chains[key]["notifier"].stop()
        del chains[key]
        LOGGER.debug("Removed " + key)

    LOGGER.debug("Reloaded config from " + filename)
    if old_glob:
        fnames = []
        for pattern in old_glob:
            fnames += glob.glob(pattern)
        if fnames:
            time.sleep(3)
            LOGGER.debug("Touching old files")
            for fname in fnames:
                if os.path.exists(fname):
                    fp_ = open(fname, "ab")
                    fp_.close()
        old_glob = []
    LOGGER.debug("done reloading config")
# Unpackers

# xrit


def check_output(*popenargs, **kwargs):
    """Copy from python 2.7, `subprocess.check_output`.
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    LOGGER.debug("Calling " + str(popenargs))
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
    """Unpacks xrit data.
    """
    opath, ofile = os.path.split(pathname)
    destination = destination or "/tmp/"
    dest_url = urlparse(destination)
    expected = os.path.join((destination or opath), ofile[:-2] + "__")
    if pathname.endswith("__"):
        return pathname
    if os.path.exists(expected):
        return expected
    if dest_url.scheme in ("", "file"):
        res = check_output([cmd, pathname], cwd=(destination or opath))
        del res
    else:
        LOGGER.exception("Can not extract file " + pathname
                         + " to " + destination
                         + ", destination has to be local.")
    LOGGER.info("Successfully extracted" + pathname +
                " to " + destination)
    return expected


# bzip

import bz2
BLOCK_SIZE = 1024


def bzip(origin, destination=None):
    """Unzip files.
    """
    ofile = os.path.split(origin)[1]
    destfile = os.path.join(destination or "/tmp/", ofile[:-4])
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
            LOGGER.debug("Bunzipped " + origin + " to " + destfile)
        finally:
            orig.close()
    return destfile



def unpack(pathname, compression=None, working_directory=None, prog=None, **kwargs):
    del kwargs
    if compression:
        try:
            unpack_fun = eval(compression)
            if prog is not None:
                new_path = unpack_fun(pathname,
                                      working_directory,
                                      prog)
            else:
                new_path = unpack_fun(pathname,
                                      working_directory)
            return new_path
        except:
            LOGGER.exception("Could not decompress " + pathname)
    return pathname


# Mover


def move_it(message, attrs=None, hook=None):
    """Check if the file pointed by *filename* is in the filelist, and move it
    if it is.
    """
    uri = urlparse(message.data["uri"])
    dest = message.data["destination"]

    pathname = uri.path

    LOGGER.debug(str(attrs))
    urlobj = urlparse(message.data['destination'])
    clean_dest = urlunparse((urlobj.scheme,
                             urlobj.hostname,
                             urlobj.path,
                             "", "", ""))

    LOGGER.debug("Copying to: " + clean_dest)
    dest_url = urlparse(dest)
    try:
        mover = MOVERS[dest_url.scheme]
    except KeyError, e:
        LOGGER.error("Unsupported protocol '" + str(dest_url.scheme)
                     + "'. Could not copy " + pathname + " to "
                     + str(dest))
        raise
    try:
        mover(pathname, dest_url).copy()
        if hook:
            hook(pathname, dest_url)
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.error("Something went wrong during copy of %s to %s: %s",
                     pathname, str(dest), str(err))
        LOGGER.debug("".join(traceback.format_tb(exc_traceback)))
        raise err
    else:
        LOGGER.info("Successfully copied " + pathname +
                    " to " + str(dest))

        #url_destination = urlparse(dest)
        #mdata = message.data.copy()
        #mdata.pop("destination")

        #mdata["uid"] = os.path.basename(pathname)
        #mdata["uri"] = urlunparse((url_destination.scheme,
        #                           url_destination.hostname,
        #                           os.path.join(url_destination.path, mdata["uid"]),
        #                           url_destination.params,
        #                           url_destination.query,
        #                           url_destination.fragment))

        #msg = Message(message.subject, "file", mdata)
        #LOGGER.debug("publishing %s", str(msg))
        #PUB.send(str(msg))


class Mover(object):

    """Base mover object. Doesn't do anything as it has to be subclassed.
    """

    def __init__(self, origin, destination):
        if isinstance(destination, str):
            self.destination = urlparse(destination)
        else:
            self.destination = destination

        self.origin = origin

    def copy(self):
        """Copy it !
        """

        raise NotImplementedError("Copy for scheme " + self.destination.scheme +
                                  " not implemented (yet).")

    def move(self):
        """Move it !
        """

        raise NotImplementedError("Move for scheme " + self.destination.scheme +
                                  " not implemented (yet).")


class FileMover(Mover):

    """Move files in the filesystem.
    """

    def copy(self):
        """Copy
        """
        try:
            os.link(self.origin, self.destination.path)
        except OSError:
            shutil.copy(self.origin, self.destination.path)

    def move(self):
        """Move it !
        """
        shutil.move(self.origin, self.destination.path)

from ftplib import FTP, all_errors


class FtpMover(Mover):

    """Move files over ftp.
    """

    def move(self):
        """Push it !
        """
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Push it !
        """
        connection = FTP(timeout=10)
        connection.connect(self.destination.hostname,
                           self.destination.port or 21)
        if self.destination.username and self.destination.password:
            connection.login(self.destination.username,
                             self.destination.password)
        else:
            connection.login()

        file_obj = file(self.origin, 'rb')
        connection.cwd(self.destination.path)
        connection.storbinary('STOR ' + os.path.basename(self.origin),
                              file_obj)

        try:
            connection.quit()
        except all_errors:
            connection.close()

MOVERS = {'ftp': FtpMover,
          'file': FileMover,
          '': FileMover}


# Generic event handler
# fixme: on deletion, the file should be removed from the filecache
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


def create_notifier(attrs):
    """Create a notifier from the specified configuration attributes *attrs*.
    """

    tmask = (pyinotify.IN_CLOSE_WRITE |
             pyinotify.IN_MOVED_TO |
             pyinotify.IN_CREATE)

    wm_ = pyinotify.WatchManager()

    opath, ofile = os.path.split(globify(attrs["origin"]))

    def fun(orig_pathname):
        """Publish what we have
        """
        fname = os.path.basename(orig_pathname)

        if not fnmatch.fnmatch(orig_pathname, globify(attrs["origin"])):
            return

        pathname = unpack(orig_pathname, **attrs)

        info = attrs.get("info", {})
        if info:
            info = dict((elt.strip().split('=')
                         for elt in info.split(";")))
            for infokey, infoval in info.items():
                if "," in infoval:
                    info[infokey] = infoval.split(",")

        info.update(parse(attrs["origin"], orig_pathname))
        info['uri'] = pathname
        info['uid'] = os.path.basename(pathname)
        info['request_address'] = get_own_ip() + ":" + attrs["request_port"]
        msg = Message(attrs["topic"], 'file', info)
        PUB.send(str(msg))
        LOGGER.debug("Message sent: " + str(msg))

    tnotifier = pyinotify.ThreadedNotifier(wm_, EventHandler(fun))

    wm_.add_watch(opath, tmask)

    return tnotifier


def terminate(chains):
    for chain in chains.itervalues():
        chain["notifier"].stop()
        if "request_manager" in chain:
            chain["request_manager"].stop()

    PUB.stop()

    LOGGER.info("Shutting down.")
    print ("Thank you for using pytroll/move_it_server."
           " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)

running = True

def stopped(notifier, **kwargs):
    notifier.stop()
    return not running

if __name__ == '__main__':
    import argparse
    import signal

    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-p", "--port",
                        help="The port to publish on. 9010 is the default",
                        default=9010)
    cmd_args = parser.parse_args()

    log_format = "[%(asctime)s %(levelname)-8s] %(message)s"
    LOGGER = logging.getLogger('move_it_server')
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

    LOGGER.info("Starting publisher on port %s.", str(cmd_args.port))

    PUB = Publisher("tcp://*:" + str(cmd_args.port), "move_it_server")

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)
    watchman = pyinotify.WatchManager()

    notifier = pyinotify.Notifier(watchman, EventHandler(reload_config))
    watchman.add_watch(os.path.dirname(cmd_args.config_file), mask)

    def chains_stop(*args):
        global running
        running = False
        terminate(chains)

    signal.signal(signal.SIGTERM, chains_stop)

    try:
        reload_config(cmd_args.config_file)
        notifier.loop(stopped)
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if running:
            chains_stop()
