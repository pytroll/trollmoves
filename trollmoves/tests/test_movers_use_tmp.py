"""Tests for the use_tmp workflow and finalize_atomic_transfer methods in movers."""

from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest

# ---------------------------------------------------------------------------
# Helpers shared across SSH-based tests
# ---------------------------------------------------------------------------

def _patch_ssh_for_auto_add_policy(monkeypatch):
    """Patch paramiko.SSHClient to accept any host key (AutoAddPolicy)."""
    import paramiko
    OrigSSHClient = paramiko.SSHClient

    def _new_ssh_client(*args, **kwargs):
        client = OrigSSHClient(*args, **kwargs)
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    monkeypatch.setattr(paramiko, "SSHClient", _new_ssh_client)


@pytest.fixture
def source_file(tmp_path):
    """A small source file used as the origin for mover tests."""
    path = tmp_path / "source" / "data.txt"
    path.parent.mkdir(parents=True)
    path.write_text("hello atomic transfer")
    return path


# ===========================================================================
# Group A – Mover.tmp_destination_for (pure static method, no I/O)
# ===========================================================================

def test_tmp_destination_for_default_prefix():
    """Default prefix '.' is prepended to the basename."""
    from trollmoves.movers import Mover
    dest = urlparse("/some/dir/file.txt")
    tmp = Mover.tmp_destination_for(dest, ".")
    assert tmp.path == "/some/dir/.file.txt"
    assert tmp.scheme == dest.scheme


def test_tmp_destination_for_custom_prefix():
    """Custom prefix is prepended to the basename."""
    from trollmoves.movers import Mover
    dest = urlparse("file:///some/dir/data.nc")
    tmp = Mover.tmp_destination_for(dest, "_tmp_")
    assert tmp.path == "/some/dir/_tmp_data.nc"


def test_tmp_destination_for_non_path_dest():
    """Fallback: if dest has no .path attribute, dest is returned unchanged."""
    from trollmoves.movers import Mover

    class _NoDotPath:
        pass

    obj = _NoDotPath()
    result = Mover.tmp_destination_for(obj, ".")
    assert result is obj


# ===========================================================================
# Group B – Mover base class finalize_atomic_transfer
# ===========================================================================

def test_base_mover_finalize_raises_for_pathless_dest():
    """Base Mover.finalize_atomic_transfer raises NotImplementedError when
    tmp_destination has no .path attribute."""
    from trollmoves.movers import Mover

    class _NoPath:
        pass

    mover = object.__new__(Mover)
    mover.destination = urlparse("/some/path")
    with pytest.raises(NotImplementedError):
        mover.finalize_atomic_transfer(_NoPath(), urlparse("/other/path"))


# ===========================================================================
# Group C – FileMover (real files on local filesystem)
# ===========================================================================

def test_file_mover_finalize_atomic_transfer_renames(tmp_path):
    """finalize_atomic_transfer renames tmp file to final, preserving content."""
    from trollmoves.movers import FileMover

    tmp_file = tmp_path / ".data.txt"
    tmp_file.write_text("content")
    final_file = tmp_path / "data.txt"

    mover = FileMover(str(tmp_file), str(tmp_file))
    tmp_dest = urlparse(str(tmp_file))
    final_dest = urlparse(str(final_file))

    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    assert final_file.read_text() == "content"
    assert not tmp_file.exists()


def test_file_mover_finalize_creates_missing_final_dir(tmp_path):
    """finalize_atomic_transfer creates the final directory if it is absent."""
    from trollmoves.movers import FileMover

    tmp_file = tmp_path / ".data.txt"
    tmp_file.write_text("data")
    final_dir = tmp_path / "subdir" / "deep"
    final_file = final_dir / "data.txt"

    mover = FileMover(str(tmp_file), str(tmp_file))
    mover.finalize_atomic_transfer(urlparse(str(tmp_file)), urlparse(str(final_file)))

    assert final_file.exists()
    assert not tmp_file.exists()


def test_file_mover_finalize_updates_destination(tmp_path):
    """finalize_atomic_transfer updates mover.destination to the final dest."""
    from trollmoves.movers import FileMover

    tmp_file = tmp_path / ".data.txt"
    tmp_file.write_text("x")
    final_file = tmp_path / "data.txt"

    mover = FileMover(str(tmp_file), str(tmp_file))
    final_dest = urlparse(str(final_file))
    mover.finalize_atomic_transfer(urlparse(str(tmp_file)), final_dest)

    assert mover.destination == final_dest


def test_move_it_use_tmp_file_scheme(tmp_path, source_file):
    """Full use_tmp round-trip via move_it() with empty (local) scheme."""
    from trollmoves.movers import move_it

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    destination = str(dest_dir / "data.txt")
    attrs = {"use_tmp_on_transfer": True}

    returned = move_it(str(source_file), destination, attrs=attrs)

    assert (dest_dir / "data.txt").exists()
    assert not (dest_dir / ".data.txt").exists(), "tmp file should be cleaned up"
    assert returned == urlparse(destination)


def test_move_it_use_tmp_custom_prefix(tmp_path, source_file):
    """Full use_tmp round-trip via move_it() with a custom tmp_prefix."""
    from trollmoves.movers import move_it

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    destination = str(dest_dir / "data.txt")
    attrs = {"use_tmp_on_transfer": True, "tmp_prefix": "incoming_"}

    move_it(str(source_file), destination, attrs=attrs)

    assert (dest_dir / "data.txt").exists()
    assert not (dest_dir / "incoming_data.txt").exists()


def test_move_it_use_tmp_cleanup_on_finalize_error(tmp_path, source_file, monkeypatch):
    """When finalize_atomic_transfer raises, move_it removes the tmp file."""
    from trollmoves.movers import FileMover, move_it

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    destination = str(dest_dir / "data.txt")
    attrs = {"use_tmp_on_transfer": True}

    def _raise(*_):
        raise NotImplementedError("simulated")

    monkeypatch.setattr(FileMover, "finalize_atomic_transfer", _raise)

    with pytest.raises(NotImplementedError):
        move_it(str(source_file), destination, attrs=attrs)

    # The tmp file must have been cleaned up
    assert not (dest_dir / ".data.txt").exists()
    # The final destination should not exist either
    assert not (dest_dir / "data.txt").exists()


def test_move_it_use_tmp_cleanup_on_oserror(tmp_path, source_file, monkeypatch):
    """Non-NotImplementedError exceptions from finalize also trigger cleanup."""
    from trollmoves.movers import FileMover, move_it

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    destination = str(dest_dir / "data.txt")
    attrs = {"use_tmp_on_transfer": True}

    def _raise_os(*_):
        raise OSError("disk full")

    monkeypatch.setattr(FileMover, "finalize_atomic_transfer", _raise_os)

    with pytest.raises(OSError, match="disk full"):
        move_it(str(source_file), destination, attrs=attrs)

    assert not (dest_dir / ".data.txt").exists()
    assert not (dest_dir / "data.txt").exists()


# ===========================================================================
# Group D – ScpMover (real localhost SSH)
# ===========================================================================

@pytest.mark.slow
def test_scp_mover_finalize_atomic_transfer(tmp_path, monkeypatch):
    """ScpMover.finalize_atomic_transfer performs a remote SFTP rename on localhost."""
    from trollmoves.movers import ScpMover

    _patch_ssh_for_auto_add_policy(monkeypatch)

    tmp_file = tmp_path / ".data.txt"
    tmp_file.write_text("scp finalize")
    final_file = tmp_path / "data.txt"

    # Origin and initial destination path don't matter for finalize; only hostname does
    mover = ScpMover(str(tmp_file), f"scp://localhost{tmp_path}/data.txt")

    tmp_dest = urlparse(f"scp://localhost{tmp_path}/.data.txt")
    final_dest = urlparse(f"scp://localhost{tmp_path}/data.txt")
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    assert final_file.read_text() == "scp finalize"
    assert not tmp_file.exists()
    assert mover.destination == final_dest


@pytest.mark.slow
def test_move_it_use_tmp_scp_scheme(tmp_path, source_file, monkeypatch):
    """Full use_tmp round-trip via move_it() with scp://localhost destination."""
    from trollmoves.movers import move_it

    _patch_ssh_for_auto_add_policy(monkeypatch)

    dest_dir = tmp_path / "dest_scp"
    dest_dir.mkdir()
    destination = f"scp://localhost{dest_dir}/data.txt"
    attrs = {"use_tmp_on_transfer": True}

    returned = move_it(str(source_file), destination, attrs=attrs)

    assert (dest_dir / "data.txt").exists()
    assert not (dest_dir / ".data.txt").exists()
    assert returned == urlparse(destination)


# ===========================================================================
# Group E – SftpMover (real localhost SSH)
# ===========================================================================

@pytest.mark.slow
def test_sftp_mover_finalize_atomic_transfer(tmp_path, monkeypatch):
    """SftpMover.finalize_atomic_transfer performs a remote SFTP rename on localhost."""
    from trollmoves.movers import SftpMover

    _patch_ssh_for_auto_add_policy(monkeypatch)

    tmp_file = tmp_path / ".data.txt"
    tmp_file.write_text("sftp finalize")
    final_file = tmp_path / "data.txt"

    mover = SftpMover(str(tmp_file), f"sftp://localhost{tmp_path}/data.txt")
    tmp_dest = urlparse(f"sftp://localhost{tmp_path}/.data.txt")
    final_dest = urlparse(f"sftp://localhost{tmp_path}/data.txt")
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    assert final_file.read_text() == "sftp finalize"
    assert not tmp_file.exists()
    assert mover.destination == final_dest


@pytest.mark.slow
def test_move_it_use_tmp_sftp_scheme(tmp_path, source_file, monkeypatch):
    """Full use_tmp round-trip via move_it() with sftp://localhost destination."""
    from trollmoves.movers import move_it

    _patch_ssh_for_auto_add_policy(monkeypatch)

    dest_dir = tmp_path / "dest_sftp"
    dest_dir.mkdir()
    destination = f"sftp://localhost{dest_dir}/data.txt"
    attrs = {"use_tmp_on_transfer": True}

    returned = move_it(str(source_file), destination, attrs=attrs)

    assert (dest_dir / "data.txt").exists()
    assert not (dest_dir / ".data.txt").exists()
    assert returned == urlparse(destination)


# ===========================================================================
# Group F – FtpMover (mocked FTP connection)
# ===========================================================================

def test_ftp_mover_finalize_atomic_transfer_rename_args():
    """FtpMover.finalize_atomic_transfer: cwd to dest dir then rename with basenames."""
    from trollmoves.movers import FtpMover

    origin = "/path/to/source.txt"
    destination = "ftp://ftphost/remote/dir/source.txt"
    mover = FtpMover(origin, destination)

    mock_connection = MagicMock()
    tmp_dest = urlparse("ftp://ftphost/remote/dir/.source.txt")
    final_dest = urlparse("ftp://ftphost/remote/dir/source.txt")

    with patch.object(mover, "get_connection", return_value=mock_connection):
        mover.finalize_atomic_transfer(tmp_dest, final_dest)

    # ensure_remote_dirs cds to the directory; rename uses basenames relative to that cwd
    mock_connection.cwd.assert_called_with("/remote/dir")
    mock_connection.rename.assert_called_once_with(".source.txt", "source.txt")


def test_ftp_mover_finalize_updates_destination():
    """FtpMover.finalize_atomic_transfer updates mover.destination to final."""
    from trollmoves.movers import FtpMover

    mover = FtpMover("/origin.txt", "ftp://ftphost/dir/file.txt")
    mock_connection = MagicMock()
    tmp_dest = urlparse("ftp://ftphost/dir/.file.txt")
    final_dest = urlparse("ftp://ftphost/dir/file.txt")

    with patch.object(mover, "get_connection", return_value=mock_connection):
        mover.finalize_atomic_transfer(tmp_dest, final_dest)

    assert mover.destination == final_dest


# ===========================================================================
# Group G – S3Mover (mocked S3 backend)
# ===========================================================================

@patch("trollmoves.movers.S3FileSystem")
def test_s3_mover_finalize_copy_mode(mock_s3fs):
    """s3_use_copy=True: finalize uses final_destination directly (not tmp-stripping).

    tmp is in a staging prefix; final is in a completely different path to prove
    the implementation reads final_destination rather than reverse-engineering it
    from the tmp path.
    """
    from trollmoves.movers import S3Mover

    mock_s3 = MagicMock()
    mock_s3fs.return_value = mock_s3

    mover = S3Mover("/local/file.txt", "s3://mybucket/staging/.file.txt",
                    attrs={"s3_use_copy": True, "tmp_prefix": "."})

    tmp_dest = urlparse("s3://mybucket/staging/.file.txt")
    final_dest = urlparse("s3://mybucket/archive/2024/file.txt")  # different dir
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    mock_s3.copy.assert_called_once_with("mybucket/staging/.file.txt", "mybucket/archive/2024/file.txt")
    mock_s3.rm.assert_called_once_with("mybucket/staging/.file.txt")
    assert mover.destination == urlparse("s3://mybucket/archive/2024/file.txt")


@patch("trollmoves.movers.boto3")
def test_s3_mover_finalize_multipart_mode(mock_boto3):
    """s3_use_multipart=True: finalize only updates destination, no S3 I/O."""
    from trollmoves.movers import S3Mover

    mover = S3Mover("/local/file.txt", "s3://mybucket/dir/.file.txt",
                    attrs={"s3_use_multipart": True, "tmp_prefix": "."})

    tmp_dest = urlparse("s3://mybucket/dir/.file.txt")
    final_dest = urlparse("s3://mybucket/dir/file.txt")
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    # No boto3 client calls – the multipart copy() already wrote the final key
    mock_boto3.client.assert_not_called()
    assert mover.destination == urlparse("s3://mybucket/dir/file.txt")


@patch("trollmoves.movers.S3FileSystem", None)
@patch("trollmoves.movers.boto3", None)
def test_s3_mover_finalize_raises_if_unconfigured():
    """finalize raises NotImplementedError when neither multipart nor copy is set."""
    from trollmoves.movers import S3Mover

    mover = S3Mover("/local/file.txt", "s3://mybucket/dir/.file.txt", attrs={})
    tmp_dest = urlparse("s3://mybucket/dir/.file.txt")
    final_dest = urlparse("s3://mybucket/dir/file.txt")

    with pytest.raises(NotImplementedError):
        mover.finalize_atomic_transfer(tmp_dest, final_dest)


# ===========================================================================
# Group H – move_it() FTP end-to-end with use_tmp (mocked)
# ===========================================================================

@patch("trollmoves.movers.FTP")
def test_move_it_use_tmp_ftp_scheme(mock_ftp_class, tmp_path):
    """Full use_tmp round-trip via move_it() with ftp:// destination (mocked FTP)."""
    from trollmoves.movers import move_it

    source = tmp_path / "upload.txt"
    source.write_text("ftp content")

    mock_ftp = MagicMock()
    mock_ftp_class.return_value.__enter__ = lambda s: mock_ftp
    mock_ftp_class.return_value.__exit__ = MagicMock(return_value=False)
    mock_ftp_class.return_value = mock_ftp

    destination = "ftp://ftphost/remote/dir/upload.txt"
    attrs = {"use_tmp_on_transfer": True}

    move_it(str(source), destination, attrs=attrs)

    # copy step: storbinary was called once (for the tmp file)
    assert mock_ftp.storbinary.call_count == 1
    store_call_args = mock_ftp.storbinary.call_args[0]
    assert store_call_args[0] == "STOR .upload.txt"

    # finalize step: rename was called once from tmp to final
    mock_ftp.rename.assert_called_once_with(".upload.txt", "upload.txt")


# ===========================================================================
# Group I – supports_atomic classmethod on each mover class
# ===========================================================================

def test_supports_atomic_file_mover():
    """FileMover always supports atomic transfers."""
    from trollmoves.movers import FileMover
    assert FileMover.supports_atomic() is True
    assert FileMover.supports_atomic(attrs={}) is True


def test_supports_atomic_ftp_mover():
    """FtpMover always supports atomic transfers."""
    from trollmoves.movers import FtpMover
    assert FtpMover.supports_atomic() is True


def test_supports_atomic_scp_mover():
    """ScpMover always supports atomic transfers."""
    from trollmoves.movers import ScpMover
    assert ScpMover.supports_atomic() is True


def test_supports_atomic_sftp_mover():
    """SftpMover always supports atomic transfers."""
    from trollmoves.movers import SftpMover
    assert SftpMover.supports_atomic() is True


def test_supports_atomic_base_mover_returns_false():
    """The base Mover class returns False as a safe default."""
    from trollmoves.movers import Mover
    assert Mover.supports_atomic() is False
    assert Mover.supports_atomic(attrs={"use_tmp_on_transfer": True}) is False


# ===========================================================================
# Group J – move_it() falls back when supports_atomic returns False
# ===========================================================================

def test_move_it_falls_back_when_atomic_not_supported(tmp_path, monkeypatch, caplog):
    """When use_tmp_on_transfer=True but supports_atomic returns False, move_it
    falls back to a direct transfer and logs an error."""
    import logging

    from trollmoves.movers import FileMover, move_it

    # Make FileMover report it does not support atomic so we can test the fallback
    # without needing a custom protocol.
    monkeypatch.setattr(FileMover, "supports_atomic", classmethod(lambda cls, attrs=None: False))

    source = tmp_path / "source" / "data.txt"
    source.parent.mkdir()
    source.write_text("fallback content")
    dest = tmp_path / "dest" / "data.txt"

    with caplog.at_level(logging.ERROR, logger="trollmoves.movers"):
        move_it(str(source), str(dest), attrs={"use_tmp_on_transfer": True})

    # Final file must exist (transfer succeeded via direct path)
    assert dest.exists()
    assert dest.read_text() == "fallback content"
    # Tmp file must NOT exist
    assert not (tmp_path / "dest" / ".data.txt").exists()
    # An error must have been logged about the fallback
    assert any("does not support atomic" in record.message for record in caplog.records)
