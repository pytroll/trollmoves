#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012, 2013, 2014, 2015
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

"""
Script for moving and unpacking files.

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
  origin=/eumetcast/received/H-000-{series:_<6s}-{platform_name:_<12s}-{channel:_<9s}-{segment:_<9s}-{nominal_time:%Y%m%d%H%M}-__
  destinations=/eumetcast/unpacked/
  compression=xrit
  delete=False
  prog=/local_disk/usr/src/PublicDecompWT/Image/Linux_32bits/xRITDecompress
  topic=/HRIT/L0/dev
  info=sensors=seviri;stream=eumetcast
  publish_port=

* 'origin' is the directory and pattern of files to watch for. For a description
  of the pattern format, see the trollsift documentation:
  http://trollsift.readthedocs.org/en/latest/index.html

* 'destinations' is the list of places to put the unpacked data in. The
  protocols supported at the moment are file and ftp, so the following are
  valid::

     destinations=/tmp/unpack/ file:///tmp/unpack ftp://user:passwd@server.smhi.se/tmp/unpack



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

* The delete parameters tells if a file needs to be deleted after
  unpacking. This defaults to False.

* 'topic', 'publish_port', and 'info' define the messaging behaviour using
  posttroll, if these options are given, 'info' being a ';' separated list of
  'key=value' items that has to be added to the message info.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a file to log to (instead of stdout) w
ith the -l or --log option::

  move_it --log /path/to/mylogfile.log myconfig.ini

Operations
----------

This scripts operates with `inotify`, so that the watched files are handled as
soon as an `in_close_write` event occurs. This means that the incomming
directory has to be watchable with `inotify`, so that rules out NFS partitions
for example.

The configuration file is also watched in this way, so the same rules
apply. The reason for this is that the changes done in the configuration file
are applied as soon as the file is saved.

Each section of the configuration file is describing an chain of
processing. Each chain is executed in an independant thread so that problems in
one chain will not propagate to the other chains.

When the script is launched, or when a section is added or updated, existing
files matching the pattern defined in `origin` are touched in order to create a
triggering of the chain on them. To avoid any race conditions, chain is
executed tree seconds after the list of file is gathered.
"""

import fnmatch
import glob
import logging
import logging.handlers
import os
import subprocess
import sys
import time
from configparser import RawConfigParser
from urllib.parse import urlparse, urlunparse
import argparse
import signal

import bz2
import pyinotify
from trollsift import globify, parse
# messaging is optional
try:
    from posttroll.publisher import NoisyPublisher
    from posttroll.message import Message
except ImportError:
    print("\nNOTICE! Import of posttroll failed, "
          "messaging will not be used.\n")

from trollmoves.movers import MOVERS


LOGGER = logging.getLogger("move_it")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s] %(message)s"


chains = {}

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
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)

        if "origin" not in res[section]:
            LOGGER.warning("Incomplete section %s: add an 'origin' item.",
                           section)
            LOGGER.info("Ignoring section %s: incomplete.", section)
            del res[section]
            continue
        if "destinations" not in res[section]:
            LOGGER.warning("Incomplete section %s: add an 'destinations' item.",
                           section)
            LOGGER.info("Ignoring section %s: incomplete.", section)
            del res[section]
            continue
        else:
            res[section]["destinations"] = res[section]["destinations"].split()

        if "topic" in res[section]:
            try:
                res[section]["publish_port"] = int(
                    res[section]["publish_port"])
            except (KeyError, ValueError):
                res[section]["publish_port"] = 0
    return res


def reload_config(filename, disable_backlog=False):
    """Rebuild chains if needed (if the configuration changed) from *filename*."""
    LOGGER.debug("New config file detected! %s", filename)

    new_chains = read_config(filename)

    old_glob = []

    config_changed = False
    for key, val in new_chains.items():
        identical = True
        if key in chains:
            for key2, val2 in new_chains[key].items():
                if ((key2 not in ["notifier", "publisher"]) and
                    ((key2 not in chains[key]) or
                     (chains[key][key2] != val2))):
                    identical = False
                    config_changed = True
                    break
            if identical:
                continue

            chains[key]["notifier"].stop()
            if "publisher" in chains[key]:
                chains[key]["publisher"].stop()

        chains[key] = val
        try:
            chains[key]["publisher"] = NoisyPublisher("move_it_" + key,
                                                      val["publish_port"])
        except (KeyError, NameError):
            pass
        chains[key]["notifier"] = create_notifier(val)
        # create logger too!
        if "publisher" in chains[key]:
            pub = chains[key]["publisher"].start()
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
                try:
                    info.update(parse(os.path.basename(val["origin"]),
                                      os.path.basename(pathname)))
                except ValueError:
                    info.update(parse(os.path.basename(os.path.splitext(val["origin"])[0]),
                                      os.path.basename(pathname)))

                info['uri'] = destination
                info['uid'] = fname
                msg = Message(val["topic"], 'file', info)
                pub.send(str(msg))
                LOGGER.debug("Message sent: %s", str(msg))

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
                LOGGER.debug("Message sent: %s", str(msg))

            chains[key]["delete_hook"] = delete_hook

        if not identical:
            LOGGER.debug("Updated %s", key)
        else:
            LOGGER.debug("Added %s", key)

    for key in (set(chains.keys()) - set(new_chains.keys())):
        chains[key]["notifier"].stop()
        del chains[key]
        LOGGER.debug("Removed %s", key)

    if config_changed:
        LOGGER.debug("Reloaded config from %s", filename)
    else:
        LOGGER.debug("No changes to reload in %s", filename)

    if old_glob and not disable_backlog:
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
        LOGGER.info("Old files transferred")

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
    """Unpack xrit data."""
    opath, ofile = os.path.split(pathname)
    destination = destination or "/tmp/"
    dest_url = urlparse(destination)
    if dest_url.scheme in ("", "file"):
        _ = check_output([cmd, pathname], cwd=(destination or opath))
    else:
        LOGGER.exception("Can not extract file %s to %s, destination has to be local.",
                         pathname, destination)
    LOGGER.info("Successfully extracted %s to %s", pathname,
                destination)
    return os.path.join((destination or opath), ofile[:-2] + "__")


# bzip

BLOCK_SIZE = 1024


def bzip(origin, destination=None):
    """Unzip files."""
    ofile = os.path.split(origin)[1]
    destfile = os.path.join(destination or "/tmp/", ofile[:-4])
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

# Mover


def move_it(pathname, destinations, hook=None):
    """Check if the file pointed by *filename* is in the filelist, and move it if it is."""
    err = None
    for dest in destinations:
        LOGGER.debug("Copying to: %s", dest)
        dest_url = urlparse(dest)
        try:
            mover = MOVERS[dest_url.scheme]
        except KeyError:
            LOGGER.error("Unsupported protocol '%s'. Could not copy %s to %s",
                         str(dest_url.scheme), pathname, str(dest))
            continue
        try:
            mover(pathname, dest_url).copy()
            if hook:
                hook(pathname, dest_url)
        except Exception:
            LOGGER.exception("Something went wrong during copy of %s to %s",
                             pathname, str(dest))
            continue
        else:
            LOGGER.info("Successfully copied %s to %s", pathname,
                        str(dest))

    if err is not None:
        raise err


# Generic event handler

class EventHandler(pyinotify.ProcessEvent):
    """Handle events with a generic *fun* function."""

    def __init__(self, fun, *args, **kwargs):
        """Initialize event handler."""
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._fun = fun

    def process_IN_CLOSE_WRITE(self, event):
        """Process on closing a writable file."""
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


def create_notifier(attrs):
    """Create a notifier from the specified configuration attributes *attrs*."""
    tmask = (pyinotify.IN_CLOSE_WRITE |
             pyinotify.IN_MOVED_TO |
             pyinotify.IN_CREATE)

    wm_ = pyinotify.WatchManager()

    opath, ofile = os.path.split(globify(attrs["origin"]))

    def fun(pathname):
        """Execute unpacking and copying/moving of *pathname*."""
        efile = os.path.basename(pathname)
        if fnmatch.fnmatch(efile, ofile):
            LOGGER.info("We have a match: %s", str(pathname))
            if attrs["compression"]:
                try:
                    unpack_fun = eval(attrs["compression"])
                    if "prog" in attrs:
                        new_path = unpack_fun(pathname,
                                              attrs["working_directory"],
                                              attrs["prog"])
                    else:
                        new_path = unpack_fun(pathname,
                                              attrs["working_directory"])
                except Exception:
                    LOGGER.exception("Could not decompress %s", pathname)
                    return

            else:
                new_path = pathname
            try:
                move_it(new_path, attrs["destinations"],
                        attrs.get("copy_hook", None))
            except Exception:
                LOGGER.error("Something went wrong during copy of %s",
                             pathname)
            else:
                if attrs["delete"]:
                    try:
                        os.remove(pathname)
                        if attrs["delete_hook"]:
                            attrs["delete_hook"](pathname)
                        LOGGER.debug("Removed %s", pathname)
                    except OSError as e__:
                        if e__.errno == 2:
                            LOGGER.debug("Already deleted: %s", pathname)
                        else:
                            raise

            # delete temporary file
            if pathname != new_path:
                try:
                    os.remove(new_path)
                except OSError as e__:
                    if e__.errno == 2:
                        pass
                    else:
                        raise

    tnotifier = pyinotify.ThreadedNotifier(wm_, EventHandler(fun))

    wm_.add_watch(opath, tmask)

    return tnotifier


def terminate(chainss):
    """Terminate transfer chains."""
    for chain in chainss.itervalues():
        chain["notifier"].stop()
        if "publisher" in chain:
            chain["publisher"].stop()
    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)


def parse_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log",
                        help="The file to log to. stdout otherwise.")
    parser.add_argument("-d", "--disable-backlog", default=False,
                        action="store_true",
                        help="Disable handling of backlog. Default: resend exising files.")
    return parser.parse_args()


def setup_logging(cmd_args):
    """Set up logging."""
    global LOGGER
    LOGGER = logging.getLogger('move_it')
    LOGGER.setLevel(logging.DEBUG)

    if cmd_args.log:
        fh_ = logging.handlers.TimedRotatingFileHandler(
            os.path.join(cmd_args.log),
            "midnight",
            backupCount=7)
    else:
        fh_ = logging.StreamHandler()

    formatter = logging.Formatter(LOG_FORMAT)
    fh_.setFormatter(formatter)

    LOGGER.addHandler(fh_)

    pyinotify.log.handlers = [fh_]


def main():
    """Run move_it."""
    cmd_args = parse_args()
    setup_logging(cmd_args)

    LOGGER.info("Starting up.")

    mask = (pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE)
    watchman = pyinotify.WatchManager()

    notifier = pyinotify.Notifier(watchman, EventHandler(reload_config))
    watchman.add_watch(cmd_args.config_file, mask)

    def chains_stop(*args):
        del args
        terminate(chains)

    signal.signal(signal.SIGTERM, chains_stop)

    try:
        reload_config(cmd_args.config_file,
                      disable_backlog=cmd_args.disable_backlog)
        notifier.loop()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        terminate(chains)


if __name__ == '__main__':
    main()
