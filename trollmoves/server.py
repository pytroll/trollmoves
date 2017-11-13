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

import bz2
import errno
import fnmatch
import glob
import logging
import logging.handlers
import os
import shutil
import subprocess
import sys
import time
import traceback
from ConfigParser import ConfigParser
from ftplib import FTP, all_errors
from Queue import Empty, Queue
from threading import Thread
from urlparse import urlparse, urlunparse

import pyinotify
from zmq import NOBLOCK, POLLIN, PULL, PUSH, ROUTER, Poller, ZMQError

from posttroll import context
from posttroll.message import Message
from posttroll.publisher import get_own_ip
from trollsift import globify, parse

LOGGER = logging.getLogger(__name__)


class ConfigError(Exception):
    pass


class Deleter(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.queue = Queue()
        self.timer = None
        self.loop = True

    def add(self, filename):
        LOGGER.debug('Scheduling %s for removal', filename)
        self.queue.put((filename, time.time() + 30))

    def run(self):
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
                    except:
                        LOGGER.exception(
                            'Something went wrong when deleting %s:', filename)
                    else:
                        LOGGER.debug('Removed %s.', filename)
                    break

    @staticmethod
    def delete(filename):
        try:
            os.remove(filename)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

    def stop(self):
        self.loop = False
        if self.timer:
            self.timer.cancel()


class RequestManager(Thread):
    """Manage requests.
    """

    def __init__(self, port, attrs=None):
        Thread.__init__(self)

        self._loop = True
        self.out_socket = context.socket(ROUTER)
        self.out_socket.bind("tcp://*:" + str(port))
        self.port = port
        self.in_socket = context.socket(PULL)
        self.in_socket.bind("inproc://replies" + str(port))

        self._poller = Poller()
        self._poller.register(self.out_socket, POLLIN)
        self._poller.register(self.in_socket, POLLIN)
        self._attrs = attrs
        try:
            self._pattern = globify(attrs["origin"])
        except ValueError as err:
            raise ConfigError('Invalid file pattern: ' + str(err))
        self._deleter = Deleter()

        try:
            self._station = self._attrs["station"]
        except (KeyError, TypeError):
            LOGGER.warning("Station is not defined in config file")
            self._station = "unknown"
        LOGGER.debug("Station is '%s'" % self._station)

    def start(self):
        self._deleter.start()
        Thread.start(self)

    def pong(self, message):
        """Reply to ping
        """
        return Message(message.subject, "pong", {"station": self._station})

    def push(self, message):
        """Reply to push request
        """
        uri = urlparse(message.data["uri"])
        pathname = uri.path

        if not fnmatch.fnmatch(
                os.path.basename(pathname),
                os.path.basename(globify(self._attrs["origin"]))):
            LOGGER.warning('Client trying to get invalid file: %s', pathname)
            return Message(message.subject,
                           "err",
                           data="{0:s} not reachable".format(pathname))
        try:
            move_it(message, self._attrs)
        except Exception as err:
            return Message(message.subject, "err", data=str(err))
        else:
            if (self._attrs.get('compression') or self._attrs.get(
                    'delete', 'False').lower() in ["1", "yes", "true", "on"]):
                self._deleter.add(pathname)
            new_msg = Message(message.subject,
                              "file",
                              data=message.data.copy())
            new_msg.data['destination'] = clean_url(new_msg.data[
                'destination'])
            return new_msg

    def ack(self, message):
        """Reply with ack to a publication
        """
        uri = urlparse(message.data["uri"])
        pathname = uri.path

        if not fnmatch.fnmatch(
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

    def unknown(self, message):
        """Reply to any unknown request.
        """
        ### del message
        return Message(message.subject, "unknown")

    def reply_and_send(self, fun, address, message):
        in_socket = context.socket(PUSH)
        in_socket.connect("inproc://replies" + str(self.port))

        reply = Message(message.subject, "error")
        try:
            reply = fun(message)
        except:
            LOGGER.exception("Something went wrong"
                             " when processing the request: %s", str(message))
        finally:
            LOGGER.debug("Response: " + str(message))
            in_socket.send_multipart([address, b'', str(reply)])

    def run(self):
        while self._loop:
            try:
                socks = dict(self._poller.poll(timeout=2000))
            except ZMQError:
                LOGGER.info("Poller interrupted.")
                continue
            if socks.get(self.out_socket) == POLLIN:
                LOGGER.debug("Received a request")
                address, empty, payload = self.out_socket.recv_multipart(
                    NOBLOCK)
                message = Message(rawstr=payload)
                fake_msg = Message(rawstr=str(message))
                try:
                    urlparse(message.data['destination'])
                except (KeyError, TypeError):
                    pass
                else:
                    fake_msg.data['destination'] = clean_url(message.data[
                        'destination'])

                LOGGER.debug("processing request: " + str(fake_msg))
                if message.type == "ping":
                    Thread(target=self.reply_and_send,
                           args=(self.pong, address, message)).start()
                elif message.type == "push":
                    Thread(target=self.reply_and_send,
                           args=(self.push, address, message)).start()
                elif message.type == "ack":
                    Thread(target=self.reply_and_send,
                           args=(self.ack, address, message)).start()
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


def create_notifier(attrs, publisher):
    """Create a notifier from the specified configuration attributes *attrs*.
    """

    tmask = (pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO |
             pyinotify.IN_CREATE)

    wm_ = pyinotify.WatchManager()

    pattern = globify(attrs["origin"])
    opath = os.path.dirname(pattern)

    def fun(orig_pathname):
        """Publish what we have."""
        if not fnmatch.fnmatch(orig_pathname, pattern):
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
        LOGGER.debug("Message sent: " + str(msg))

    tnotifier = pyinotify.ThreadedNotifier(wm_, EventHandler(fun))

    wm_.add_watch(opath, tmask)

    return tnotifier, fun


def clean_url(url):
    """Remove login info from *url*."""
    urlobj = urlparse(url)
    return urlunparse((urlobj.scheme, urlobj.hostname,
                       urlobj.path, "", "", ""))


def read_config(filename):
    """Read the config file called *filename*.
    """
    cp_ = ConfigParser()
    cp_.read(filename)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        res[section].setdefault("working_directory", None)
        res[section].setdefault("compression", False)

        if "origin" not in res[section]:
            LOGGER.warning("Incomplete section " + section +
                           ": add an 'origin' item.")
            LOGGER.info("Ignoring section " + section + ": incomplete.")
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
            LOGGER.warning("Incomplete section " + section +
                           ": add an 'topic' item.")
            LOGGER.info("Ignoring section " + section + ": incomplete.")
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
                  notifier_builder=create_notifier,
                  manager=RequestManager,
                  publisher=None):
    """Rebuild chains if needed (if the configuration changed) from *filename*.
    """

    LOGGER.debug("New config file detected! " + filename)

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
        chains[key]["notifier"], fun = notifier_builder(val, publisher)
        chains[key]["request_manager"].start()
        chains[key]["notifier"].start()
        old_glob.append((globify(val["origin"]), fun))

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
        time.sleep(3)
        for pattern, fun in old_glob:
            process_old_files(pattern, fun)

    LOGGER.debug("done reloading config")

# Unpackers

# xrit


def check_output(*popenargs, **kwargs):
    """Copy from python 2.7, `subprocess.check_output`."""
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
    """Unpacks xrit data."""
    opath, ofile = os.path.split(pathname)
    destination = destination or "/tmp/"
    dest_url = urlparse(destination)
    expected = os.path.join((destination or opath), ofile[:-2] + "__")
    if dest_url.scheme in ("", "file"):
        check_output([cmd, pathname], cwd=(destination or opath))
    else:
        LOGGER.exception("Can not extract file " + pathname + " to " +
                         destination + ", destination has to be local.")
    LOGGER.info("Successfully extracted " + pathname + " to " + destination)
    return expected

# bzip

BLOCK_SIZE = 1024


def bzip(origin, destination=None):
    """Unzip files."""
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
        except:
            LOGGER.exception("Could not decompress " + pathname)
        else:
            if delete.lower() in ["1", "yes", "true", "on"]:
                os.remove(pathname)
            return new_path
    return pathname

# Mover


def move_it(message, attrs=None, hook=None):
    """Check if the file pointed by *filename* is in the filelist, and move it
    if it is.
    """
    uri = urlparse(message.data["uri"])
    dest = message.data["destination"]

    pathname = uri.path
    fake_dest = clean_url(dest)

    LOGGER.debug("Copying to: " + fake_dest)
    dest_url = urlparse(dest)
    try:
        mover = MOVERS[dest_url.scheme]
    except KeyError:
        LOGGER.error("Unsupported protocol '" + str(dest_url.scheme) +
                     "'. Could not copy " + pathname + " to " + str(dest))
        raise

    try:
        mover(pathname, dest_url, attrs=attrs).copy()
        if hook:
            hook(pathname, dest_url)
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.error("Something went wrong during copy of %s to %s: %s",
                     pathname, str(fake_dest), str(err))
        LOGGER.debug("".join(traceback.format_tb(exc_traceback)))
        raise err
    else:
        LOGGER.info("Successfully copied " + pathname + " to " + str(
            fake_dest))

# TODO: implement the creation of missing directories.


class Mover(object):
    """Base mover object. Doesn't do anything as it has to be subclassed.
    """

    def __init__(self, origin, destination, attrs=None):
        if isinstance(destination, str):
            self.destination = urlparse(destination)
        else:
            self.destination = destination

        self.origin = origin
        self.attrs = attrs or {}

    def copy(self):
        """Copy it !
        """

        raise NotImplementedError("Copy for scheme " + self.destination.scheme
                                  + " not implemented (yet).")

    def move(self):
        """Move it !
        """

        raise NotImplementedError("Move for scheme " + self.destination.scheme
                                  + " not implemented (yet).")


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
        connection.connect(self.destination.hostname, self.destination.port or
                           21)
        if self.destination.username and self.destination.password:
            connection.login(self.destination.username,
                             self.destination.password)
        else:
            connection.login()

        def cd_tree(current_dir):
            if current_dir != "":
                try:
                    connection.cwd(current_dir)
                except IOError:
                    cd_tree("/".join(current_dir.split("/")[:-1]))
                    connection.mkd(current_dir)
                    connection.cwd(current_dir)

        LOGGER.debug('cd to %s', os.path.dirname(self.destination.path))
        cd_tree(os.path.dirname(self.destination.path))
        with open(self.origin, 'rb') as file_obj:
            connection.storbinary('STOR ' + os.path.basename(self.origin),
                                  file_obj)

        try:
            connection.quit()
        except all_errors:
            connection.close()


class ScpMover(Mover):

    """Move files over ssh with scp.
    """

    def move(self):
        """Push it !"""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Push it !"""
        from paramiko import SSHClient
        from scp import SCPClient

        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.destination.hostname,
                    username=self.destination.username)

        LOGGER.debug('hostname %s', self.destination.hostname)
        LOGGER.debug('dest path %s ', os.path.dirname(self.destination.path))
        LOGGER.debug('origin %s ', self.origin)

        scp = SCPClient(ssh.get_transport())
        scp.put(self.origin, self.destination.path)

        scp.close()
        ssh.close()


class SftpMover(Mover):

    """Move files over sftp.
    """

    def move(self):
        """Push it !"""
        self.copy()
        os.remove(self.origin)

    def _agent_auth(self, transport):
        """Attempt to authenticate to the given transport using any of the private
        keys available from an SSH agent ... or from a local private RSA key file
        (assumes no pass phrase).

        PFE: http://code.activestate.com/recipes/576810-copy-files-over-ssh-using-paramiko/
        """
        import paramiko

        agent = paramiko.Agent()

        private_key_file = self.attrs.get("ssh_private_key_file", None)
        if private_key_file:
            private_key_file = os.path.expanduser(private_key_file)
            LOGGER.info("Loading keys from local file %s", private_key_file)
            agent_keys = (paramiko.RSAKey.from_private_key_file(private_key_file),)
        else:
            LOGGER.info("Loading keys from SSH agent")
            agent_keys = agent.get_keys()
        if len(agent_keys) == 0:
            raise IOError("No available keys")

        for key in agent_keys:
            LOGGER.debug('Trying ssh key %s', key.get_fingerprint().encode('hex'))
            try:
                transport.auth_publickey(self.destination.username, key)
                LOGGER.debug('... ssh key success!')
                return
            except paramiko.SSHException:
                continue

        # We found no valid key
        raise IOError("RSA key auth failed!")

    def copy(self):
        """Push it !"""
        import paramiko

        transport = paramiko.Transport((self.destination.hostname, 22))
        transport.start_client()

        self._agent_auth(transport)

        if not transport.is_authenticated():
            raise IOError("RSA key auth failed!")

        sftp = transport.open_session()
        sftp = paramiko.SFTPClient.from_transport(transport)
        ###sftp.get_channel().settimeout(300)

        try:
            sftp.mkdir(os.path.dirname(self.destination.path))
        except IOError:
            # Assuming remote directory exist
            pass
        sftp.put(self.origin, self.destination.path)
        transport.close()

MOVERS = {'ftp': FtpMover,
          'file': FileMover,
          '': FileMover,
          'scp': ScpMover,
          'sftp': SftpMover
          }


# Generic event handler
# fixme: on deletion, the file should be removed from the filecache
class EventHandler(pyinotify.ProcessEvent):
    """Handle events with a generic *fun* function.
    """

    def __init__(self, fun, *args, **kwargs):
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        self._cmd_filename = kwargs.get('cmd_filename')
        if self._cmd_filename:
            self._cmd_filename = os.path.abspath(self._cmd_filename)
        self._fun = fun

    def process_IN_CLOSE_WRITE(self, event):
        """On closing after writing."""
        if self._cmd_filename and os.path.abspath(
                event.pathname) != self._cmd_filename:
            return
        self._fun(event.pathname)

    def process_IN_CREATE(self, event):
        """On closing after linking."""
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


def process_old_files(pattern, fun):
    fnames = glob.glob(pattern)
    if fnames:
        # time.sleep(3)
        LOGGER.debug("Touching old files")
        for fname in fnames:
            if os.path.exists(fname):
                fun(fname)


def terminate(chains, publisher=None):
    for chain in chains.values():
        chain["notifier"].stop()
        if "request_manager" in chain:
            chain["request_manager"].stop()

    if publisher:
        publisher.stop()

    LOGGER.info("Shutting down.")
    print("Thank you for using pytroll/move_it_server."
          " See you soon on pytroll.org!")
    time.sleep(1)
    sys.exit(0)
