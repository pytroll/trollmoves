"""Test the movers."""

import os
from contextlib import contextmanager
from unittest.mock import patch
from urllib.parse import urlparse, urlunparse

import pytest
import yaml

ORIGIN_FILENAME = "filename.ext"

ORIGIN = "/path/to/mydata/" + ORIGIN_FILENAME
USERNAME = "username"
PASSWORD = "passwd"
ACCOUNT = None

test_yaml_s3_connection_params = """
target-s3-example1:
  host: s3://my-fancy-bucket/
  connection_parameters:
    client_kwargs:
      endpoint_url: 'https://minio-server.mydomain.se:9000'
      verify: false
    secret: "my-super-secret-key"
    key: "my-access-key"
    use_ssl: true
  aliases:
    platform_name:
      Suomi-NPP: npp
      NOAA-20: j01
      NOAA-21: j02
    variant:
      DR: directreadout

  dispatch_configs:
    - topics:
        - /atms/sdr/1
      conditions:
        - sensor: [atms, [atms]]
          format: SDR
          variant: DR
      directory: /upload/sdr
"""


@contextmanager
def _get_ftp(destination, origin=ORIGIN):
    from trollmoves.movers import FtpMover

    with patch("trollmoves.movers.FTP") as ftp:
        ftp_mover = FtpMover(origin, destination)
        connection = ftp_mover.open_connection()

        yield ftp, ftp_mover

        ftp_mover.delete_connection(connection)


@pytest.fixture
def file_to_move(tmp_path):
    """Create a file that can be moved."""
    filename = tmp_path / ORIGIN_FILENAME
    with open(filename, "w") as fd:
        fd.write("Hej")
    return filename


@patch("netrc.netrc")
def test_open_ftp_connection_with_netrc_no_netrc(netrc):
    """Check getting ftp connection when .netrc is missing."""
    netrc.side_effect = FileNotFoundError("Failed retrieve authentification details from netrc file")

    with _get_ftp("ftp://localhost.smhi.se/data/satellite/archive/") as (ftp, _):
        ftp.return_value.login.assert_called_once_with()


@patch("netrc.netrc")
def test_open_ftp_connection_with_netrc(netrc):
    """Check getting the netrc authentication for ftp connection."""
    netrc.return_value.hosts = {"localhost.smhi.se": (USERNAME, ACCOUNT, PASSWORD)}
    netrc.return_value.authenticators.return_value = (USERNAME, ACCOUNT, PASSWORD)
    netrc.side_effect = None

    with _get_ftp("ftp://localhost.smhi.se/data/satellite/archive/") as (ftp, _):
        ftp.return_value.login.assert_called_once_with(USERNAME, PASSWORD)


def test_open_ftp_connection_credentials_in_url():
    """Check getting ftp connection with credentials in the URL."""
    with _get_ftp("ftp://auser:apasswd@localhost.smhi.se/data/satellite/archive/") as (ftp, _):
        ftp.return_value.login.assert_called_once_with("auser", "apasswd")


@pytest.mark.parametrize("destination,expected_filename",
                         [("ftp://localhost.smhi.se/data/satellite/archive/somefile.ext", "STOR somefile.ext"),
                          ("ftp://localhost.smhi.se/data/satellite/archive/", "STOR " + ORIGIN_FILENAME)])
def test_ftp_mover_uses_destination_filename(file_to_move, destination, expected_filename):
    """Check ftp movers uses requested destination filename when provided, origin filename otherwise."""
    with _get_ftp(destination, file_to_move) as (ftp, ftp_mover):
        ftp_mover.copy()
        ftp.return_value.cwd.assert_called_once_with("/data/satellite/archive")
        assert ftp.return_value.storbinary.call_args[0][0] == expected_filename


def _get_s3_mover(origin, destination, **attrs):
    from trollmoves.movers import S3Mover

    return S3Mover(origin, destination, attrs=attrs)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_base(S3FileSystem):
    """Test copying to base of S3 bucket."""
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/")
    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/" + ORIGIN_FILENAME)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_attrs_are_sanitized(S3FileSystem):
    """Test that only accepted attrs are passed to S3Filesystem."""
    attrs = {"ssh_key_filename": "should_be_removed", "endpoint_url": "should_be_included"}
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/", **attrs)
    s3_mover.copy()

    expected_attrs = {"endpoint_url": "should_be_included"}
    S3FileSystem.assert_called_once_with(**expected_attrs)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_prefix_with_trailing_slash(S3FileSystem):
    """Test that when destination ends in a slash, the original file basename is added to it."""
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/upload/")
    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/upload/" + ORIGIN_FILENAME)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_prefix_no_trailing_slash(S3FileSystem):
    """Test giving destination without trailing slash to see it is used as object name."""
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/upload")
    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/upload")


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_prefix_urlparse(S3FileSystem):
    """Test that giving urlparse() result as destination works."""
    s3_mover = _get_s3_mover(ORIGIN, urlparse("s3://data-bucket/upload/my_satellite_data.h5"))
    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/upload/my_satellite_data.h5")


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_base_using_connection_parameters(S3FileSystem):
    """Test copying to base of S3 bucket."""
    # Get the connection parameters:
    config = yaml.safe_load(test_yaml_s3_connection_params)
    attrs = config["target-s3-example1"]["connection_parameters"]

    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/", **attrs)
    assert s3_mover.attrs["client_kwargs"] == {
        "endpoint_url": "https://minio-server.mydomain.se:9000", "verify": False}
    assert s3_mover.attrs["secret"] == "my-super-secret-key"
    assert s3_mover.attrs["key"] == "my-access-key"
    assert s3_mover.attrs["use_ssl"] is True

    s3_mover.copy()

    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/" + ORIGIN_FILENAME)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_file_to_sub_directory(S3FileSystem):
    """Test copying to sub directory of a S3 bucket."""
    # The target directory doesn't exist
    S3FileSystem.return_value.exists.return_value = False
    s3_mover = _get_s3_mover(ORIGIN, "s3://data-bucket/target/directory/")
    s3_mover.copy()

    S3FileSystem.return_value.mkdirs.assert_called_once_with("data-bucket/target/directory")
    S3FileSystem.return_value.put.assert_called_once_with(ORIGIN, "data-bucket/target/directory/" + ORIGIN_FILENAME)


@patch("trollmoves.movers.S3FileSystem")
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


@pytest.mark.parametrize("hostname", ["localhost", "localhost:22"])
def test_sftp_copy(tmp_file, tmp_path, monkeypatch, hostname):
    """Test the sftp mover's copy functionality."""
    patch_ssh_client_for_auto_add_policy(monkeypatch)
    origin = tmp_file
    destination = tmp_path / "dest.ext"
    from trollmoves.movers import SftpMover

    dest = urlunparse(("sftp", hostname, os.fspath(destination), None, None, None))
    SftpMover(origin, dest).copy()
    assert os.path.exists(destination)


def patch_ssh_client_for_auto_add_policy(monkeypatch):
    """Patch the `paramiko.SSHClient` to use the `AutoAddPolicy`."""
    import paramiko
    SSHClient = paramiko.SSHClient

    def new_ssh_client(*args, **kwargs):
        client = SSHClient(*args, **kwargs)
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    monkeypatch.setattr(paramiko, "SSHClient", new_ssh_client)


@pytest.fixture
def tmp_file(tmp_path):
    """Create a simple file with content."""
    path = tmp_path / "file.ext"
    with open(path, mode="w") as fd:
        fd.write("dummy file")
    return path
