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


def _get_s3_mover(origin, destination):
    from trollmoves.movers import S3Mover

    return S3Mover(origin, destination)


@patch('trollmoves.movers.S3FileSystem')
def test_s3_copy_file_to_base(S3FileSystem):
    """Test copying to base of S3 bucket."""
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/")
    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/filename.ext")


@patch('trollmoves.movers.S3FileSystem')
def test_s3_copy_file_to_sub_directory(S3FileSystem):
    """Test copying to sub directory of a S3 bucket."""
    # The target directory doesn't exist
    S3FileSystem.return_value.exists.return_value = False
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/target/directory/")
    s3_mover.copy()

    S3FileSystem.return_value.mkdirs.assert_called_once_with("data-bucket/target/directory")
    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/target/directory/filename.ext")


@patch('trollmoves.movers.S3FileSystem')
def test_s3_move(S3FileSystem):
    """Test moving a file."""
    import os
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile(delete=False) as fid:
        fname = fid.name
    s3_mover = _get_s3_mover(fname, "s3://data-bucket/target/directory")
    s3_mover.move()
    try:
        assert not os.path.exists(fname)
    except AssertionError:
        os.remove(fname)
        raise OSError("File was not deleted after transfer.")


def test_sftp_copy(tmp_path):
    """Test the sftp mover's copy functionality."""
    origin = tmp_path / "file.ext"
    destination = tmp_path / "dest.ext"
    with open(origin, mode="w") as fd:
        fd.write("trying sftp")
    from trollmoves.movers import SftpMover
    import os
    from urllib.parse import urlunparse
    dest = urlunparse(("sftp", "localhost", os.fspath(destination), None, None, None))
    SftpMover(origin, dest).copy()
    assert os.path.exists(destination)


def test_sftp_copy_custom_port(tmp_path):
    """Test the sftp mover with a custom port."""
    origin = tmp_path / "file.ext"
    destination = tmp_path / "dest.ext"
    with open(origin, mode="w") as fd:
        fd.write("trying sftp")
    from trollmoves.movers import SftpMover
    import os
    from urllib.parse import urlunparse
    dest = urlunparse(("sftp", "localhost:22", os.fspath(destination), None, None, None))
    SftpMover(origin, dest).copy()
    assert os.path.exists(destination)
