"""Test the ssh server."""

import errno
import logging
import shutil
import socket
import sys
import unittest
from tempfile import NamedTemporaryFile, mkdtemp
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import urlparse

import pytest
from paramiko import SSHException

import trollmoves

logger = logging.getLogger()

class TestSSHMovers(unittest.TestCase):
    """Tests for SSH Mover."""

    def setUp(self):
        """Create temporary directories and setup configurations."""
        self.origin_dir = mkdtemp()
        with NamedTemporaryFile("w", delete=False, dir=self.origin_dir) as temporary_file:
            self.origin = temporary_file.name

        self.dest_dir = mkdtemp()

        self.hostname = "localhost"
        self.login = "user"
        self.port = 22

        self.destination_no_login = "scp://" + self.hostname + ":" + str(self.port) + "/" + self.dest_dir
        self.destination_with_login = 'scp://' + self.login + '@' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir  # noqa
        self.destination_no_port = "scp://" + self.login + "@" + self.hostname + "/" + self.dest_dir
        self.destination_no_login_no_port = "scp://" + self.hostname + ":" + str(self.port) + "/" + self.dest_dir

        self._attrs_empty = {}
        self._attrs_connection_uptime = {"connection_uptime": 0}

    def tearDown(self):
        """Remove temporary directories."""
        try:
            shutil.rmtree(self.origin_dir, ignore_errors=True)
            shutil.rmtree(self.dest_dir, ignore_errors=True)
        except OSError:
            pass

    def test_scp(self):
        """Check ScpMover init."""
        with patch("trollmoves.movers.ScpMover") as sm:
            sm_instanse = sm.return_value
            sm_instanse.run.return_value = {u"dataObjectID": u"test1"}

            trollmoves.movers.ScpMover(self.origin, self.destination_no_login, attrs=self._attrs_empty)

            sm.assert_called_once_with(self.origin, self.destination_no_login, attrs=self._attrs_empty)

    def test_scp_open_connection(self):
        """Check scp open_connection."""
        with patch("trollmoves.movers.ScpMover") as smgc:
            smgc.return_value.open_connection.return_value = "testing"

            scp_mover = trollmoves.movers.ScpMover(self.origin, self.destination_no_login, attrs=self._attrs_empty)

            self.assertEqual(scp_mover.open_connection(), "testing")

    def test_scp_get_connection(self):
        """Check scp get_connection."""
        with patch("trollmoves.movers.ScpMover") as smgc:
            smgc.return_value.get_connection.return_value = "testing"

            scp_mover = trollmoves.movers.ScpMover(self.origin, self.destination_no_login_no_port,
                                                   attrs=self._attrs_empty)

            self.assertEqual(scp_mover.get_connection(self.hostname, self.port, self.login), "testing")

    @patch("paramiko.SSHClient", autospec=True)
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

    @patch("paramiko.SSHClient", autospec=True)
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

    @patch("paramiko.SSHClient.connect", autospec=True)
    def test_scp_open_connection_ssh_exception(self, mock_sshclient_connect):
        """Check scp get_connection failing for SSHException."""
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock(side_effect=SSHException)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)

        with pytest.raises(IOError, match="Failed to ssh connect after 3 attempts"):
            scp_mover.open_connection()

    @patch("paramiko.SSHClient.connect", autospec=True)
    def test_scp_open_connection_socket_timeout_exception(self, mock_sshclient_connect):
        """Check scp get_connection failing with socket timeout."""
        from trollmoves.movers import ScpMover
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

        mocked_client = MagicMock(side_effect=socket.timeout)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={"ssh_connection_timeout": 1})
        try:
            with self.assertLogs(logger, level=logging.INFO) as lc, self.assertRaises(IOError):
                scp_mover.open_connection()
            self.assertIn(("SSH connection timed out:"), lc.output[0])
        finally:
            logger.removeHandler(stream_handler)

    @patch("paramiko.SSHClient", autospec=True)
    def test_scp_open_connection_backup_targets(self, mock_sshclient):
        """Check scp get_connection using backup targets."""
        from trollmoves.movers import ScpMover
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

        mocked_client = MagicMock(side_effect=socket.timeout)
        mock_sshclient.return_value.connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={"ssh_connection_timeout": 1},
                             backup_targets=["backup_host1",
                                             "backup_host2"])
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

    @patch("paramiko.SSHClient.connect", autospec=True)
    def test_scp_open_connection_generic_exception(self, mock_sshclient_connect):
        """Check scp open_connection() failure when a generic exception happens."""
        from trollmoves.movers import ScpMover

        mocked_client = MagicMock(side_effect=Exception)
        mock_sshclient_connect.side_effect = mocked_client

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs=self._attrs_connection_uptime)

        with pytest.raises(IOError, match="Failed to ssh connect after 3 attempts"):
            scp_mover.open_connection()

    @patch("paramiko.SSHClient", autospec=True)
    def test_scp_open_connection_honours_configured_number_of_retries(self, mock_sshclient):
        """Check that num_ssh_retries decides how many times connecting is attempted."""
        from trollmoves.movers import ScpMover

        mock_sshclient.return_value.connect.side_effect = socket.timeout

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={"num_ssh_retries": 2, "ssh_connection_timeout": 1})

        with pytest.raises(IOError, match="Failed to ssh connect after 2 attempts"):
            scp_mover.open_connection()

        assert mock_sshclient.return_value.connect.call_count == 2

    @patch("paramiko.SSHClient", autospec=True)
    def test_scp_open_connection_number_of_retries_given_as_string(self, mock_sshclient):
        """Check that num_ssh_retries works when read from an ini config file as a string."""
        from trollmoves.movers import ScpMover

        mock_sshclient.return_value.connect.side_effect = socket.timeout

        scp_mover = ScpMover(self.origin, self.destination_no_port,
                             attrs={"num_ssh_retries": "2", "ssh_connection_timeout": 1})

        with pytest.raises(IOError, match="Failed to ssh connect after 2 attempts"):
            scp_mover.open_connection()

        assert mock_sshclient.return_value.connect.call_count == 2

    @patch("paramiko.SSHClient.connect", autospec=True)
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

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy(self, mock_scp_client, mock_sshclient):
        """Check scp copy."""
        from trollmoves.movers import ScpMover

        mocked_scp_client = MagicMock()
        mock_scp_client.return_value = mocked_scp_client

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        scp_mover.copy()

        mocked_scp_client.put.assert_called_once_with(self.origin, urlparse(self.destination_no_port).path)

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_uses_the_default_scpclient_timeout(self, mock_scp_client, mock_sshclient):
        """Check the timeout used when nothing is configured against the documented default."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        scp_mover.copy()

        assert mock_scp_client.call_args.kwargs["socket_timeout"] == 10

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_uses_the_configured_scpclient_timeout(self, mock_scp_client, mock_sshclient):
        """Check that a configured response timeout is passed on to the scp client."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"scpclient_timeout_seconds": 30})
        scp_mover.copy()

        assert mock_scp_client.call_args.kwargs["socket_timeout"] == 30

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_scpclient_timeout_given_as_string(self, mock_scp_client, mock_sshclient):
        """Check that a timeout read from an ini config file, and thus a string, is converted."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"scpclient_timeout_seconds": "30"})
        scp_mover.copy()

        assert mock_scp_client.call_args.kwargs["socket_timeout"] == 30

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_generic_exception(self, mock_scp_client, mock_sshclient):
        """Check scp copy for generic exception."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.side_effect = Exception

        with pytest.raises(Exception):
            scp_mover.copy()

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_oserror_exception(self, mock_scp_client, mock_sshclient):
        """Check scp copy for OSError."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = OSError

        with pytest.raises(OSError):
            scp_mover.copy()

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_oserror_exception_errno_2(self, mock_scp_client, mock_sshclient):
        """Check scp copy OSError errno 2."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = OSError(errno.ENOENT, "message")

        with pytest.raises(OSError):
            scp_mover.copy()

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_of_vanished_file_is_not_reported_as_success(self, mock_scp_client, mock_sshclient):
        """Check that a file deleted before the transfer started is not reported as copied."""
        mock_scp_client.return_value.put.side_effect = OSError(errno.ENOENT, "message")

        with self.assertLogs("trollmoves.movers", level=logging.ERROR) as logs, pytest.raises(OSError):
            trollmoves.movers.move_it(self.origin, self.destination_no_port, attrs=self._attrs_empty)

        assert not any("Successfully copied" in message for message in logs.output)

    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_put_exception(self, mock_scp_client):
        """Check scp client.put() raising Exception."""
        from trollmoves.movers import ScpMover

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        mock_scp_client.return_value.put.side_effect = Exception("Test message")

        with pytest.raises(Exception):
            scp_mover.copy()

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_retries_transient_failure(self, mock_scp_client, mock_sshclient):
        """Check that a transfer failing with a transient error is retried."""
        from scp import SCPException

        from trollmoves.movers import ScpMover

        mock_scp_client.return_value.put.side_effect = [SCPException("Connection lost"), None]

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"num_ssh_retries": 2})
        scp_mover.copy()

        assert mock_scp_client.return_value.put.call_count == 2

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_raises_when_every_retry_fails(self, mock_scp_client, mock_sshclient):
        """Check that a transfer failing on every attempt raises the error from the last one."""
        from scp import SCPException

        from trollmoves.movers import ScpMover

        mock_scp_client.return_value.put.side_effect = SCPException("Connection lost")

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"num_ssh_retries": 2})

        with pytest.raises(SCPException):
            scp_mover.copy()

        assert mock_scp_client.return_value.put.call_count == 2

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_missing_origin_file_is_not_retried(self, mock_scp_client, mock_sshclient):
        """Check that a missing origin file is reported at once, as retrying cannot help."""
        from trollmoves.movers import ScpMover

        mock_scp_client.return_value.put.side_effect = OSError(errno.ENOENT, "message")

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"num_ssh_retries": 2})

        with pytest.raises(OSError):
            scp_mover.copy()

        assert mock_scp_client.return_value.put.call_count == 1

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_response_timeout_suggests_a_longer_timeout(self, mock_scp_client, mock_sshclient):
        """Check that a timed-out scp response points the user at the setting that would help."""
        from scp import SCPException

        from trollmoves.movers import ScpMover

        mock_scp_client.return_value.put.side_effect = SCPException("Timeout waiting for scp response")

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"num_ssh_retries": 1})

        with self.assertLogs("trollmoves.movers", level=logging.ERROR) as logs, pytest.raises(SCPException):
            scp_mover.copy()

        assert any("scpclient_timeout_seconds" in message for message in logs.output)

    def test_scp_response_timeout_message_still_matches_the_scp_library(self):
        """Check the message the timeout hint keys on against what scp really raises.

        The hint only fires when ScpMover recognises scp's own timeout message, and scp
        is an unpinned dependency, so a reworded message there would silently disable the
        hint. Driving _recv_confirm is the cheapest way to see the real message.
        """
        from scp import SCPClient, SCPException

        from trollmoves.movers import SCP_RESPONSE_TIMEOUT_MESSAGE

        scp_client = SCPClient(MagicMock())
        scp_client.channel = MagicMock()
        scp_client.channel.recv.side_effect = socket.timeout

        with pytest.raises(SCPException) as raised:
            scp_client._recv_confirm()

        assert SCP_RESPONSE_TIMEOUT_MESSAGE in str(raised.value)

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_copy_retries_and_explains_a_stalled_transfer(self, mock_scp_client, mock_sshclient):
        """Check that a transfer stalling past the socket timeout is retried and explained.

        paramiko raises a bare socket.timeout when a channel read or write stalls, so
        unlike the timeouts scp notices itself this one does not arrive as an SCPException.
        """
        from trollmoves.movers import ScpMover

        mock_scp_client.return_value.put.side_effect = socket.timeout("timed out")

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs={"num_ssh_retries": 2})

        with self.assertLogs("trollmoves.movers", level=logging.ERROR) as logs, pytest.raises(TimeoutError):
            scp_mover.copy()

        assert mock_scp_client.return_value.put.call_count == 2
        assert any("scpclient_timeout_seconds" in message for message in logs.output)

    @patch("paramiko.SSHClient", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_scp_move(self, mock_scp_client, mock_sshclient):
        """Check scp move."""
        from trollmoves.movers import ScpMover

        mocked_scp_client = MagicMock()
        mock_scp_client.return_value = mocked_scp_client

        scp_mover = ScpMover(self.origin, self.destination_no_port, attrs=self._attrs_empty)
        scp_mover.move()

        mocked_scp_client.put.assert_called_once_with(self.origin, urlparse(self.destination_no_port).path)


    @patch("paramiko.SSHClient.connect", autospec=True)
    @patch("scp.SCPClient", autospec=True)
    def test_move_it_destination_types(self, patch_scpclient, patch_connect):
        """Test move_it handles destination string and urlparse types."""
        import os

        from trollmoves.movers import move_it

        expected_destination = urlparse("scp://hostname/path/name")
        pathname = os.path.join(self.dest_dir, "dest.ext")

        destination = "scp://hostname/path/name"
        ret_destination = move_it(pathname, destination)
        assert ret_destination == expected_destination

        urlparse_destination = urlparse("scp://hostname/path/name")
        ret_destination = move_it(pathname, urlparse_destination)
        assert ret_destination == expected_destination

if __name__ == "__main__":
    unittest.main()
