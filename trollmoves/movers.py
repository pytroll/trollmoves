#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2023
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

"""Movers for the move_it scripts."""

import logging
import netrc
import os
import shutil
import sys
import time
import traceback
import socket
from ftplib import FTP, all_errors, error_perm
from threading import Event, Lock, Thread, current_thread
from urllib.parse import urlparse

try:
    from s3fs import S3FileSystem
except ImportError:
    S3FileSystem = None

from trollmoves.utils import clean_url

LOGGER = logging.getLogger(__name__)


def move_it(pathname, destination, attrs=None, hook=None, rel_path=None, backup_targets=None):
    """Check if the file pointed by *pathname* is in the filelist, and move it if it is.

    The *destination* provided is used, and if *rel_path* is provided, it will
    be appended to the destination path.

    """
    try:
        dest_url = urlparse(destination)
    except AttributeError:
        dest_url = destination
    if rel_path is not None:
        new_path = os.path.join(dest_url.path, rel_path)
    else:
        new_path = dest_url.path
    new_dest = dest_url._replace(path=new_path)
    fake_dest = clean_url(new_dest)

    LOGGER.debug("new_dest = %s", new_dest)
    LOGGER.debug("Copying to: %s", fake_dest)
    try:
        LOGGER.debug("Scheme = %s", str(dest_url.scheme))
        mover = MOVERS[dest_url.scheme]
    except KeyError:
        LOGGER.error("Unsupported protocol '" + str(dest_url.scheme) +
                     "'. Could not copy " + pathname + " to " + str(destination))
        raise

    try:
        m = mover(pathname, new_dest, attrs=attrs, backup_targets=backup_targets)
        m.copy()
        last_dest = m.destination
        if last_dest != new_dest:
            new_dest = last_dest
            fake_dest = clean_url(new_dest)
        if hook:
            hook(pathname, new_dest)
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.error("Something went wrong during copy of %s to %s: %s",
                     pathname, str(fake_dest), str(err))
        LOGGER.debug("".join(traceback.format_tb(exc_traceback)))
        raise err
    else:
        LOGGER.info("Successfully copied %s to %s",
                    pathname, str(fake_dest))
    return m.destination


class Mover:
    """Base mover object. Doesn't do anything as it has to be subclassed."""

    def __init__(self, origin, destination, attrs=None, backup_targets=None):
        """Initialize the Mover."""
        LOGGER.debug("destination = %s", str(destination))
        try:
            self.destination = urlparse(destination)
        except AttributeError:
            self.destination = destination

        self._dest_username = self.destination.username
        self._dest_password = self.destination.password

        LOGGER.debug("Destination: %s", str(destination))
        self.origin = origin
        self.attrs = attrs or {}
        self.backup_targets = backup_targets

    def copy(self):
        """Copy the file."""
        raise NotImplementedError("Copy for scheme " + self.destination.scheme +
                                  " not implemented (yet).")

    def move(self):
        """Move the file."""
        raise NotImplementedError("Move for scheme " + self.destination.scheme +
                                  " not implemented (yet).")

    def get_connection(self, hostname, port, username=None):
        """Get the connection."""
        with self.active_connection_lock:
            LOGGER.debug("Destination username and passwd: %s %s",
                         self._dest_username, self._dest_password)
            LOGGER.debug('Getting connection to %s@%s:%s',
                         username, hostname, port)
            try:
                connection, timer = self.active_connections[(hostname, port, username)]
                if not self.is_connected(connection):
                    del self.active_connections[(hostname, port, username)]
                    LOGGER.debug('Resetting connection')
                    connection = self.open_connection()
                timer.cancel()
            except KeyError:
                connection = self.open_connection()

            timer = CTimer(int(self.attrs.get('connection_uptime', 30)),
                           self.delete_connection, (connection,))
            timer.start()
            self.active_connections[(self.destination.hostname, port, username)] = connection, timer

            return connection

    def delete_connection(self, connection):
        """Delete active connection *connection*."""
        with self.active_connection_lock:
            LOGGER.debug('Closing connection to %s@%s:%s',
                         self._dest_username, self.destination.hostname, self.destination.port)
            try:
                if current_thread().finished.is_set():
                    return
            except AttributeError:
                pass
            try:
                self.close_connection(connection)
            finally:
                for key, (current_connection, current_timer) in self.active_connections.items():
                    if current_connection == connection:
                        del self.active_connections[key]
                        current_timer.cancel()
                        break


class FileMover(Mover):
    """Move files in the filesystem."""

    def copy(self):
        """Copy the file."""
        dirname = os.path.dirname(self.destination.path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        try:
            os.link(self.origin, self.destination.path)
        except OSError:
            shutil.copy(self.origin, self.destination.path)

    def move(self):
        """Move the file."""
        shutil.move(self.origin, self.destination.path)


class CTimer(Thread):
    """Call a function after a specified number of seconds.

    ::

        t = CTimer(30.0, f, args=(), kwargs={})
        t.start()
        t.cancel() # stop the timer's action if it's still waiting

    """

    def __init__(self, interval, function, args=(), kwargs=None):
        """Initialize the timer."""
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs or {}
        self.finished = Event()

    def cancel(self):
        """Stop the timer if it hasn't finished yet."""
        self.finished.set()

    def run(self):
        """Run the timer."""
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()


class FtpMover(Mover):
    """Move files over ftp."""

    active_connections = dict()
    active_connection_lock = Lock()

    def _get_netrc_authentication(self):
        """Get login authentications from netrc file if available."""
        try:
            secrets = netrc.netrc()
        except (netrc.NetrcParseError, FileNotFoundError) as e__:
            LOGGER.warning('Failed retrieve authentification details from netrc file! Exception: %s', str(e__))
            return

        LOGGER.debug("Destination hostname: %s", self.destination.hostname)
        LOGGER.debug("hosts: %s", str(list(secrets.hosts.keys())))
        LOGGER.debug("Check if hostname matches any listed in the netrc file")
        if self.destination.hostname in list(secrets.hosts.keys()):
            self._dest_username, account, self._dest_password = secrets.authenticators(self.destination.hostname)
            LOGGER.debug('Got username and password from netrc file!')

    def open_connection(self):
        """Open the connection and login."""
        connection = FTP(timeout=10)
        LOGGER.debug("Connect...")
        connection.connect(self.destination.hostname,
                           self.destination.port or 21)

        if not self._dest_username or not self._dest_password:
            # Check if usernams, password is stored in the $(HOME)/.netrc file:
            self._get_netrc_authentication()
            LOGGER.debug("Authentication retrieved from netrc file!")

        if self._dest_username and self._dest_password:
            connection.login(self._dest_username, self._dest_password)
        else:
            connection.login()

        return connection

    @staticmethod
    def is_connected(connection):
        """Check if the connection *connection* is active."""
        try:
            connection.voidcmd("NOOP")
            return True
        except all_errors:
            return False
        except IOError:
            return False

    @staticmethod
    def close_connection(connection):
        """Close connection *connection*."""
        try:
            connection.quit()
        except all_errors:
            connection.close()

    def move(self):
        """Upload the file and delete afterwards."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Upload the file."""
        connection = self.get_connection(self.destination.hostname, self.destination.port, self._dest_username)

        def cd_tree(current_dir):
            if current_dir != "":
                try:
                    connection.cwd(current_dir)
                except (IOError, error_perm):
                    cd_tree("/".join(current_dir.split("/")[:-1]))
                    connection.mkd(current_dir)
                    connection.cwd(current_dir)

        LOGGER.debug('cd to %s', os.path.dirname(self.destination.path))
        destination_dirname, destination_filename = os.path.split(self.destination.path)
        cd_tree(destination_dirname)
        if not destination_filename:
            destination_filename = os.path.basename(self.origin)
        with open(self.origin, 'rb') as file_obj:
            connection.storbinary('STOR ' + destination_filename,
                                  file_obj)


class ScpMover(Mover):
    """Move files over ssh with scp."""

    active_connections = dict()
    active_connection_lock = Lock()

    def open_connection(self):
        """Open a connection."""
        from paramiko import SSHClient, SSHException
        import copy
        retries = 3
        ssh_key_filename = self.attrs.get("ssh_key_filename", None)
        try:
            timeout = float(self.attrs.get("ssh_connection_timeout", None))
        except TypeError:
            timeout = None
        backup_targets = copy.deepcopy(self.backup_targets)
        backup_targets_message = ""
        try:
            num_backup_targets = len(backup_targets)
        except TypeError:
            num_backup_targets = None
        while retries > 0:
            retries -= 1
            try:
                ssh_connection = SSHClient()
                ssh_connection.load_system_host_keys()
                ssh_connection.connect(self.destination.hostname,
                                       username=self._dest_username,
                                       port=self.destination.port or 22,
                                       key_filename=ssh_key_filename,
                                       timeout=timeout)
                LOGGER.debug("Successfully connected to %s:%s as %s",
                             self.destination.hostname,
                             self.destination.port or 22,
                             self._dest_username)
            except SSHException as sshe:
                LOGGER.exception("Failed to init SSHClient: %s", str(sshe))
            except socket.timeout as sto:
                LOGGER.exception("SSH connection timed out: %s", str(sto))
            except Exception as err:
                LOGGER.exception("Unknown exception at init SSHClient: %s", str(err))
            else:
                return ssh_connection

            ssh_connection.close()
            time.sleep(2)
            LOGGER.debug("Retrying ssh connect ...")
            if retries == 0 and backup_targets:
                backup_target = backup_targets.pop(0)
                self.destination = self.destination._replace(netloc=f"{self.destination.username}@{backup_target}")
                LOGGER.info("Changing destination to backup target: %s", self.destination.hostname)
                retries = 3
                backup_targets_message = f" to primary and {num_backup_targets} backup host(s)"
        raise IOError(f"Failed to ssh connect after 3 attempts{backup_targets_message}.")

    @staticmethod
    def is_connected(connection):
        """Check if the connection *connection* is active."""
        LOGGER.debug("checking ssh connection")
        try:
            is_active = connection.get_transport().is_active()
            if is_active:
                LOGGER.debug("SSH connection is active.")
            return is_active
        except AttributeError:
            return False

    @staticmethod
    def close_connection(connection):
        """Close connection *connection*."""
        if isinstance(connection, tuple):
            connection[0].close()
        else:
            connection.close()

    def move(self):
        """Upload the file and delete it afterwards."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Upload the file."""
        from scp import SCPClient

        ssh_connection = self.get_connection(self.destination.hostname,
                                             self.destination.port or 22,
                                             self._dest_username)
        try:
            scp = SCPClient(ssh_connection.get_transport())
        except Exception as err:
            LOGGER.error("Failed to initiate SCPClient: %s", str(err))
            ssh_connection.close()
            raise

        try:
            scp.put(self.origin, self.destination.path)
        except OSError as osex:
            if osex.errno == 2:
                LOGGER.error("No such file or directory. File not transfered: "
                             "%s. Original error message: %s",
                             self.origin, str(osex))
            else:
                LOGGER.error("OSError in scp.put: %s", str(osex))
                raise
        except Exception as err:
            LOGGER.error("Something went wrong with scp: %s", str(err))
            LOGGER.error("Exception name %s", type(err).__name__)
            LOGGER.error("Exception args %s", str(err.args))
            raise
        finally:
            scp.close()


class SftpMover(Mover):
    """Move files over sftp."""

    def move(self):
        """Push the file."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Copy files.

        Uses high level paramiko functions.
        """
        import paramiko
        with paramiko.SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.connect(self.destination.hostname,
                        port=self.destination.port or 22,
                        username=self._dest_username,
                        allow_agent=True,
                        key_filename=self.attrs.get("ssh_private_key_file"))
            with ssh.open_sftp() as sftp:
                sftp.put(self.origin, self.destination.path)


class S3Mover(Mover):
    """Move files to S3 cloud storage.

    The transfer is initiated by Trollmoves Client by having destination that starts with "s3://".

    All the connection configurations and such may be done using the `fsspec` configuration system:

    https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration

    An example configuration could be for example placed in `~/.config/fsspec/s3.json`::

        {
            "s3": {
                "client_kwargs": {"endpoint_url": "https://s3.server.foo.com"},
                "secret": "VERYBIGSECRET",
                "key": "ACCESSKEY"
            }
        }

    However, using the this procedure may not be useful if having several
    endpoints/buckets with their own access/secret keys. Instead one can use
    aws profiles (placed in `.aws/config`) to for instance set the
    access/secret keys for various endpoints and then keep the actual url of
    the endpoints in the yaml configuration (see examples/dispatch.yaml).

    See documentation on profiles here:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file


    NB! Special behaviour on destination filepath:

    If the destination prefix (~filepath) has a trailing slash ('/') the
    original filename will be appended (analogous to moving a file from one
    directory to another keeping the same filename).

    If the destination prefix does not have a trailing slash the operation will
    be analogous to moving a file from one directory to a new destination
    changing the filename. The new destination filename will be the last part
    of the provided destination following the last slash ('/').

    """

    def copy(self):
        """Copy the file to a bucket."""
        if S3FileSystem is None:
            raise ImportError("S3Mover requires 's3fs' to be installed.")
        s3 = S3FileSystem(**self.attrs)
        destination_file_path = self._get_destination()
        LOGGER.debug('destination_file_path = %s', destination_file_path)
        _create_s3_destination_path(s3, destination_file_path)
        LOGGER.debug('Before call to put: destination_file_path = %s', destination_file_path)
        LOGGER.debug('self.origin = %s', self.origin)
        s3.put(self.origin, destination_file_path)

    def _get_destination(self):
        bucket_parts = []
        bucket_parts.append(self.destination.netloc)

        if self.destination.path != '/':
            bucket_parts.append(self.destination.path.strip('/'))
        if self.destination.path.endswith('/'):
            bucket_parts.append(os.path.basename(self.origin))

        return '/'.join(bucket_parts)

    def move(self):
        """Move the file."""
        self.copy()
        os.remove(self.origin)


def _create_s3_destination_path(s3, destination_file_path):
    destination_path = os.path.dirname(destination_file_path)
    if not s3.exists(destination_path):
        s3.mkdirs(destination_path)


MOVERS = {'ftp': FtpMover,
          'file': FileMover,
          '': FileMover,
          'scp': ScpMover,
          'sftp': SftpMover,
          's3': S3Mover,
          }
