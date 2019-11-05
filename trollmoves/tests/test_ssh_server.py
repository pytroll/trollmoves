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
from six.moves.urllib.parse import urlparse

import trollmoves

# from paramiko import SSHClient
from paramiko import SSHException

# from trollmoves.movers import ScpMover


class TestSSHMovers(unittest.TestCase):

    def setUp(self):
        self.origin_dir = mkdtemp()
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            self.origin = the_file.name

        self.dest_dir = mkdtemp()

        self.hostname = 'localhost'
        self.login = 'user'
        self.port = 22

    def tearDown(self):
        try:
            shutil.rmtree(self.origin_dir, ignore_errors=True)
            shutil.rmtree(self.dest_dir, ignore_errors=True)
        except OSError:
            pass

    def test_scp(self):
        """Check ScpMover init."""
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        _attrs = {}

        with patch('trollmoves.movers.ScpMover') as sm:
            sm_instanse = sm.return_value
            sm_instanse.run.return_value = {u'dataObjectID': u'test1'}

            trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
            sm.assert_called_once_with(origin, destination, attrs=_attrs)

    def test_scp_open_connection(self):
        """Check scp open_connection."""
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        _attrs = {}

        with patch('trollmoves.movers.ScpMover') as smgc:
            smgc.return_value.open_connection.return_value = 'testing'
            scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
            self.assertEqual(scp_mover.open_connection(), 'testing')

    def test_scp_get_connection(self):
        """Check scp get_connection."""
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        _attrs = {}

        with patch('trollmoves.movers.ScpMover') as smgc:
            smgc.return_value.get_connection.return_value = 'testing'
            scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
            self.assertEqual(scp_mover.get_connection(self.hostname, self.port, self.login), 'testing')

    @patch('trollmoves.movers.SSHClient', autospec=True)
    def test_scp_open_connection_2(self, mock_sshclient):
        """Check scp get_connection 2."""

        mocked_client = MagicMock()
        mock_sshclient.return_value = mocked_client
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.login + '@' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        _attrs = {'connection_uptime': 0}
        scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
        scp_mover.open_connection()

        mocked_client.connect.assert_called_once_with(
            self.hostname,
            port=self.port,
            username=self.login,
            key_filename=None)

    @patch('trollmoves.movers.SSHClient', autospec=True)
    def test_scp_open_connection_3(self, mock_sshclient3):
        """Check scp get_connection 3 without ssh port in destination.
        Using default ssh port 22"""

        mocked_client3 = MagicMock()
        mock_sshclient3.return_value = mocked_client3
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
        _attrs = {'connection_uptime': 0}
        scp_mover3 = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
        scp_mover3.open_connection()
        mocked_client3.connect.assert_called_once_with(
            self.hostname,
            port=22,
            username=self.login,
            key_filename=None)

    @patch('trollmoves.movers.SSHClient.connect', autospec=True)
    def test_scp_open_connection_exception(self, mock_sshclient_connect):
        """Check scp get_connection exception."""

        try:
            mocked_client = MagicMock(side_effect=SSHException)
            mock_sshclient_connect.side_effect = mocked_client
            with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
                origin = the_file.name
            destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
            _attrs = {'connection_uptime': 0}
            scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
            scp_mover.get_connection(self.hostname, 22, username=self.login)
        except IOError as ioe:
            assert str(ioe) == 'Failed to ssh connect after 3 attempts'

    @patch('trollmoves.movers.SSHClient.connect', autospec=True)
    def test_scp_open_connection_exception_2(self, mock_sshclient_connect):
        """Check scp get_connection exception."""

        try:
            mocked_client = MagicMock(side_effect=Exception)
            mock_sshclient_connect.side_effect = mocked_client
            with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
                origin = the_file.name
            destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
            _attrs = {'connection_uptime': 0}
            scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
            scp_mover.get_connection(self.hostname, 22, username=self.login)
        except IOError as ioe:
            assert str(ioe) == 'Failed to ssh connect after 3 attempts'

    @patch('trollmoves.movers.SSHClient.connect', autospec=True)
    def test_scp_is_connected_exception(self, mock_sshclient_connect):
        """Check scp is_connected exception."""

        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
        _attrs = {'connection_uptime': 0}
        scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
        connection = scp_mover.get_connection(self.hostname, 22, username=self.login)
        connection.get_transport.side_effect = AttributeError
        result = scp_mover.is_connected(connection)
        assert result is False

    @patch('trollmoves.movers.SSHClient', autospec=True)
    @patch('trollmoves.movers.SCPClient', autospec=True)
    def test_scp_copy(self, mock_scp_client, mock_sshclient):
        """Check scp copy"""

        mocked_scp_client = MagicMock()
        mock_scp_client.return_value = mocked_scp_client
        mocked_client = MagicMock()
        mock_sshclient.return_value = mocked_client
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
        _attrs = {}
        scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
        scp_mover.copy()
        mocked_scp_client.put.assert_called_once_with(origin, urlparse(destination).path)


if __name__ == '__main__':
    unittest.main()
