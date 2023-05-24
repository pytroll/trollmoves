#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019
#
# Author(s):
#
#   Trygve Aspenes <trygveas@met.no>
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
"""Test the ssh server."""

import shutil
from unittest.mock import Mock, MagicMock, patch
import unittest
from tempfile import NamedTemporaryFile, mkdtemp
import errno
from urllib.parse import urlparse

from paramiko import SSHException
import pytest
import logging
import socket
import sys

import trollmoves

logger = logging.getLogger()

class TestSSHMovers(unittest.TestCase):
    """Tests for SSH Mover."""

    def setUp(self):
        """Create temporary directories and setup configurations."""
        self.origin_dir = mkdtemp()
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as temporary_file:
            self.origin = temporary_file.name

        self.dest_dir = mkdtemp()

        self.hostname = 'localhost'
        self.login = 'user'
        self.port = 22

        self.destination_no_login = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        self.destination_with_login = 'scp://' + self.login + '@' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir  # noqa
        self.destination_no_port = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
        self.destination_no_login_no_port = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir

        self._attrs_empty = {}
        self._attrs_connection_uptime = {'connection_uptime': 0}

    def tearDown(self):
        """Remove temporary directories."""
        try:
            shutil.rmtree(self.origin_dir, ignore_errors=True)
            shutil.rmtree(self.dest_dir, ignore_errors=True)
        except OSError:
            pass

    def test_scp(self):
        """Check ScpMover init."""
        with patch('trollmoves.movers.ScpMover') as sm:
            sm_instanse = sm.return_value
            sm_instanse.run.return_value = {u'dataObjectID': u'test1'}

            trollmoves.movers.ScpMover(self.origin, self.destination_no_login, attrs=self._attrs_empty)

            sm.assert_called_once_with(self.origin, self.destination_no_login, attrs=self._attrs_empty)

    def test_scp_open_connection(self):
        """Check scp open_connection."""
        with patch('trollmoves.movers.ScpMover') as smgc:
            smgc.return_value.open_connection.return_value = 'testing'

            scp_mover = trollmoves.movers.ScpMover(self.origin, self.destination_no_login, attrs=self._attrs_empty)

            self.assertEqual(scp_mover.open_connection(), 'testing')

    def test_scp_get_connection(self):
        """Check scp get_connection."""
        with patch('trollmoves.movers.ScpMover') as smgc:
            smgc.return_value.get_connection.return_value = 'testing'

            scp_mover = trollmoves.movers.ScpMover(self.origin, self.destination_no_login_no_port,
                                                   attrs=self._attrs_empty)

            self.assertEqual(scp_mover.get_connection(self.hostname, self.port, self.login), 'testing')

    @patch('paramiko.SSHClient', autospec=True)
    def test_scp_open_connection_login_name(self, mock_sshclient):
        """Check scp open_connection() with login name."""
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock()
        mock_sshclient.return_value = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_with_login,
                             attrs=self._attrs_connection_uptime)

        scp_mover.open_connection()

        mocked_client.connect.assert_called_once_with(
            self.hostname,
            port=self.port,
            username=self.login,
            key_filename=None,
            timeout=None)

    @patch('paramiko.SSHClient', autospec=True)
    def test_scp_open_connection_without_ssh_port(self, mock_sshclient):
        """Check scp open_connection() without ssh port in destination.

        Should be using default ssh port 22.
        """
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock()
        mock_sshclient.return_value = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)

        scp_mover.open_connection()

        mocked_client.connect.assert_called_once_with(
            self.hostname,
            port=22,
            username=self.login,
            key_filename=None,
            timeout=None)

    @patch('paramiko.SSHClient.connect', autospec=True)
    def test_scp_open_connection_ssh_exception(self, mock_sshclient_connect):
        """Check scp get_connection failing for SSHException."""
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock(side_effect=SSHException)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)

        with pytest.raises(IOError, match='Failed to ssh connect after 3 attempts'):
            scp_mover.open_connection()

    @patch('paramiko.SSHClient.connect', autospec=True)
    def test_scp_open_connection_socket_timeout_exception(self, mock_sshclient_connect):
        """Check scp get_connection failing with socket timeout."""
        from trollmoves.movers import ScpMover
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

        mocked_client = MagicMock(side_effect=socket.timeout)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={'ssh_connection_timeout': 1})
        try:
            with self.assertLogs(logger, level=logging.INFO) as lc, self.assertRaises(IOError):
                scp_mover.open_connection()
            self.assertIn(("SSH connection timed out:"), lc.output[0])
        finally:
            logger.removeHandler(stream_handler)

    @patch('paramiko.SSHClient', autospec=True)
    def test_scp_open_connection_backup_targets(self, mock_sshclient):
        """Check scp get_connection using backup targets."""
        from trollmoves.movers import ScpMover
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

        mocked_client = MagicMock(side_effect=socket.timeout)
        mock_sshclient.return_value.connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={'ssh_connection_timeout': 1},
                             backup_targets=['backup_host1',
                                             'backup_host2'])
        try:
            with self.assertLogs(logger, level=logging.INFO) as lc, self.assertRaises(IOError):
                scp_mover.open_connection()
            assert "SSH connection timed out:" in lc.output[0]
            assert "Changing destination to backup target: backup_host1" in lc.output[3]
            assert "Changing destination to backup target: backup_host2" in lc.output[7]
            mock_sshclient.return_value.connect.assert_called_with("backup_host2", username="user", port=22,
                                                                    key_filename=None, timeout=1)
        finally:
            logger.removeHandler(stream_handler)

    @patch('paramiko.SSHClient.connect', autospec=True)
    def test_scp_open_connection_generic_exception(self, mock_sshclient_connect):
        """Check scp open_connection() failure when a generic exception happens."""
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock(side_effect=Exception)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)

        with pytest.raises(IOError, match='Failed to ssh connect after 3 attempts'):
            scp_mover.open_connection()

    @patch('paramiko.SSHClient.connect', autospec=True)
    def test_scp_is_connected_exception(self, mock_sshclient_connect):
        """Check scp is_connected() exception resulting in no connection."""
        from trollmoves.movers import ScpMover

        mocked_client = Mock()
        mock_sshclient_connect.return_value = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)
        connection = scp_mover.get_connection(self.hostname, 22, username=self.login)
        connection.get_transport.side_effect = AttributeError

        result = scp_mover.is_connected(connection)

        assert result is False

    @patch('paramiko.SSHClient', autospec=True)
    @patch('scp.SCPClient', autospec=True)
    def test_scp_copy(self, mock_scp_client, mock_sshclient):
        """Check scp copy."""
        from trollmoves.movers import ScpMover

        mocked_scp_client = MagicMock()
        mock_scp_client.return_value = mocked_scp_client

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        scp_mover.copy()

        mocked_scp_client.put.assert_called_once_with(self.origin, urlparse(self.destination_no_port).path)

    @patch('paramiko.SSHClient', autospec=True)
    @patch('scp.SCPClient', autospec=True)
    def test_scp_copy_generic_exception(self, mock_scp_client, mock_sshclient):
        """Check scp copy for generic exception."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.side_effect = Exception

        with pytest.raises(Exception):
            scp_mover.copy()

    @patch('scp.SCPClient', autospec=True)
    def test_scp_copy_oserror_exception(self, mock_scp_client):
        """Check scp copy for OSError."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = OSError

        with pytest.raises(OSError):
            scp_mover.copy()

    @patch('scp.SCPClient', autospec=True)
    def test_scp_copy_oserror_exception_errno_2(self, mock_scp_client):
        """Check scp copy OSError errno 2."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = OSError(errno.ENOENT, 'message')

        result = scp_mover.copy()

        assert result is None

    @patch('scp.SCPClient', autospec=True)
    def test_scp_copy_put_exception(self, mock_scp_client):
        """Check scp client.put() raising Exception."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = Exception('Test message')

        with pytest.raises(Exception):
            scp_mover.copy()

    @patch('paramiko.SSHClient', autospec=True)
    @patch('scp.SCPClient', autospec=True)
    def test_scp_move(self, mock_scp_client, mock_sshclient):
        """Check scp move."""
        from trollmoves.movers import ScpMover

        mocked_scp_client = MagicMock()
        mock_scp_client.return_value = mocked_scp_client

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        scp_mover.move()

        mocked_scp_client.put.assert_called_once_with(self.origin, urlparse(self.destination_no_port).path)


if __name__ == '__main__':
    unittest.main()
