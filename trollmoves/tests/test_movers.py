#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020 Pytroll

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of

# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test the movers
"""

from unittest.mock import patch


@patch('netrc.netrc')
def test_open_connection(netrc):
    """Check getting the netrc authentication."""
    from trollmoves.movers import FtpMover

    username = 'myusername'
    password = 'mypasswd'
    account = None

    origin = '/path/to/mydata/filename.ext'

    netrc.side_effect = FileNotFoundError('Failed retrieve authentification details from netrc file')

    with patch('trollmoves.movers.FTP') as mymock:
        destination = 'ftp://localhost.smhi.se/data/satellite/archive/'
        ftp_mover = FtpMover(origin, destination)
        ftp_mover.open_connection()

        mymock.return_value.login.assert_called_once_with()

    netrc.return_value.hosts = {'localhost.smhi.se': ('myusername', None, 'mypasswd')}
    netrc.return_value.authenticators.return_value = (username, account, password)
    netrc.side_effect = None

    with patch('trollmoves.movers.FTP') as mymock:
        destination = 'ftp://localhost.smhi.se/data/satellite/archive/'
        ftp_mover = FtpMover(origin, destination)
        ftp_mover.open_connection()

        mymock.return_value.login.assert_called_once_with(username, password)

    with patch('trollmoves.movers.FTP') as mymock:
        destination = 'ftp://auser:apasswd@localhost.smhi.se/data/satellite/archive/'
        ftp_mover = FtpMover(origin, destination)
        ftp_mover.open_connection()

        mymock.return_value.login.assert_called_once_with('auser', 'apasswd')
