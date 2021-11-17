#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Pytroll
#
# Author(s):
#
#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
#
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test the movers."""

from unittest.mock import patch

ORIGIN = '/path/to/mydata/filename.ext'
USERNAME = 'username'
PASSWORD = 'passwd'
ACCOUNT = None


def _get_ftp(destination):
    from trollmoves.movers import FtpMover

    with patch('trollmoves.movers.FTP') as ftp:
        ftp_mover = FtpMover(ORIGIN, destination)
        ftp_mover.open_connection()

    return ftp


@patch('netrc.netrc')
def test_open_ftp_connection_with_netrc_no_netrc(netrc):
    """Check getting ftp connection when .netrc is missing."""
    netrc.side_effect = FileNotFoundError('Failed retrieve authentification details from netrc file')

    ftp = _get_ftp('ftp://localhost.smhi.se/data/satellite/archive/')

    ftp.return_value.login.assert_called_once_with()


@patch('netrc.netrc')
def test_open_ftp_connection_with_netrc(netrc):
    """Check getting the netrc authentication for ftp connection."""
    netrc.return_value.hosts = {'localhost.smhi.se': (USERNAME, ACCOUNT, PASSWORD)}
    netrc.return_value.authenticators.return_value = (USERNAME, ACCOUNT, PASSWORD)
    netrc.side_effect = None

    ftp = _get_ftp('ftp://localhost.smhi.se/data/satellite/archive/')

    ftp.return_value.login.assert_called_once_with(USERNAME, PASSWORD)


def test_open_ftp_connection_credentials_in_url():
    """Check getting ftp connection with credentials in the URL."""
    ftp = _get_ftp('ftp://auser:apasswd@localhost.smhi.se/data/satellite/archive/')

    ftp.return_value.login.assert_called_once_with('auser', 'apasswd')
