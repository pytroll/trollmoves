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

import trollmoves

# from paramiko import SSHClient
# from paramiko import SSHException

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

    def ssh_connect():
        response_mock = Mock()
        return response_mock

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
        scp_mover.get_connection(self.hostname, port=self.port, username=self.login)

        mocked_client.connect.assert_called_once_with(
            self.hostname,
            port=self.port,
            username=self.login,
            key_filename=None)

    @patch('trollmoves.movers.SSHClient', autospec=True)
    def test_scp_open_connection_3(self, mock_sshclient):
        """Check scp get_connection 3 without ssh port in destination.
        Using default ssh port 22"""

        mocked_client = MagicMock()
        mock_sshclient.return_value = mocked_client
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        destination = 'scp://' + self.login + '@' + self.hostname + '/' + self.dest_dir
        _attrs = {'connection_uptime': 0}
        scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
        scp_mover.get_connection(self.hostname, 22, username=self.login)
        mocked_client.connect.assert_called_once_with(
            self.hostname,
            port=22,
            username=self.login,
            key_filename=None)


if __name__ == '__main__':
    unittest.main()
