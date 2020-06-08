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

"""Movers for the move_it scripts."""

import logging
import os
import shutil
import sys
import time
import traceback
from ftplib import FTP, all_errors, error_perm
from threading import Event, Lock, Thread, current_thread
import netrc

from six import string_types
from six.moves.urllib.parse import urlparse


from trollmoves.utils import clean_url
from paramiko import SSHClient, SSHException, AutoAddPolicy
from scp import SCPClient

LOGGER = logging.getLogger(__name__)


def move_it(pathname, destination, attrs=None, hook=None, rel_path=None):
    """Check if the file pointed by *pathname* is in the filelist, and move it
    if it is.

    The *destination* provided is used, and if *rel_path* is provided, it will
    be appended to the destination path.

    """
    dest_url = urlparse(destination)
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
        LOGGER.error("Unsupported protocol '" + str(dest_url.scheme)
                     + "'. Could not copy " + pathname + " to " + str(destination))
        raise

    try:
        mover(pathname, new_dest, attrs=attrs).copy()
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


class Mover(object):
    """Base mover object. Doesn't do anything as it has to be subclassed."""

    def __init__(self, origin, destination, attrs=None):
        if isinstance(destination, string_types):
            self.destination = urlparse(destination)
        else:
            self.destination = destination

        self._dest_username = self.destination.username
        self._dest_password = self.destination.password

        LOGGER.debug("Destination: %s", str(destination))
        self.origin = origin
        self.attrs = attrs or {}

    def copy(self):
        """Copy it !"""
        raise NotImplementedError("Copy for scheme " + self.destination.scheme
                                  + " not implemented (yet).")

    def move(self):
        """Move it !"""
        raise NotImplementedError("Move for scheme " + self.destination.scheme
                                  + " not implemented (yet).")

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
            self.active_connections[(hostname, port, username)] = connection, timer

            return connection

    def delete_connection(self, connection):
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
                for key, val in self.active_connections.items():
                    if val[0] == connection:
                        del self.active_connections[key]
                        break


class FileMover(Mover):
    """Move files in the filesystem.
    """

    def copy(self):
        """Copy
        """
        dirname = os.path.dirname(self.destination.path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        try:
            os.link(self.origin, self.destination.path)
        except OSError:
            shutil.copy(self.origin, self.destination.path)

    def move(self):
        """Move it !
        """
        shutil.move(self.origin, self.destination.path)


class CTimer(Thread):
    """Call a function after a specified number of seconds.

    ::

        t = CTimer(30.0, f, args=(), kwargs={})
        t.start()
        t.cancel() # stop the timer's action if it's still waiting

    """

    def __init__(self, interval, function, args=(), kwargs={}):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()

    def cancel(self):
        """Stop the timer if it hasn't finished yet"""
        self.finished.set()

    def run(self):
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()


class FtpMover(Mover):
    """Move files over ftp.
    """

    active_connections = dict()
    active_connection_lock = Lock()

    def _get_netrc_authentication(self):
        """Get login authentications from netrc file if available"""
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
        try:
            connection.voidcmd("NOOP")
            return True
        except all_errors:
            return False
        except IOError:
            return False

    @staticmethod
    def close_connection(connection):
        try:
            connection.quit()
        except all_errors:
            connection.close()

    def move(self):
        """Push it !
        """
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Push it !
        """
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
        cd_tree(os.path.dirname(self.destination.path))
        with open(self.origin, 'rb') as file_obj:
            connection.storbinary('STOR ' + os.path.basename(self.origin),
                                  file_obj)


class ScpMover(Mover):

    """Move files over ssh with scp.
    """
    active_connections = dict()
    active_connection_lock = Lock()

    def open_connection(self):

        retries = 3
        ssh_key_filename = self.attrs.get("ssh_key_filename", None)
        while retries > 0:
            retries -= 1
            try:
                ssh_connection = SSHClient()
                ssh_connection.set_missing_host_key_policy(AutoAddPolicy())
                ssh_connection.load_system_host_keys()
                ssh_connection.connect(self.destination.hostname,
                                       username=self._dest_username,
                                       port=self.destination.port or 22,
                                       key_filename=ssh_key_filename)
                LOGGER.debug("Successfully connected to %s:%s as %s",
                             self.destination.hostname,
                             self.destination.port or 22,
                             self._dest_username)
            except SSHException as sshe:
                LOGGER.error("Failed to init SSHClient: %s", str(sshe))
            except Exception as err:
                LOGGER.error("Unknown exception at init SSHClient: %s",
                             str(err))
            else:
                return ssh_connection

            ssh_connection.close()
            time.sleep(2)
            LOGGER.debug("Retrying ssh connect ...")
        raise IOError("Failed to ssh connect after 3 attempts")

    @staticmethod
    def is_connected(connection):
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
        if isinstance(connection, tuple):
            connection[0].close()
        else:
            connection.close()

    def move(self):
        """Push it !"""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Push it !"""

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
            LOGGER.debug('Trying ssh key %s',
                         key.get_fingerprint().encode('hex'))
            try:
                transport.auth_publickey(self._dest_username, key)
                LOGGER.debug('... ssh key success!')
                return
            except paramiko.SSHException:
                continue

        # We found no valid key
        raise IOError("RSA key auth failed!")

    def copy(self):
        """Push it !"""
        import paramiko

        transport = paramiko.Transport((self.destination.hostname,
                                        self.destination.port or 22))
        transport.start_client()

        self._agent_auth(transport)

        if not transport.is_authenticated():
            raise IOError("RSA key auth failed!")

        sftp = transport.open_session()
        sftp = paramiko.SFTPClient.from_transport(transport)
        # sftp.get_channel().settimeout(300)

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
