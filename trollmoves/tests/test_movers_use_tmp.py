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

    monkeypatch.setattr(FileMover, "finalize_atomic_transfer",
                        lambda *_: (_ for _ in ()).throw(NotImplementedError("simulated")))

    with pytest.raises(NotImplementedError):
        move_it(str(source_file), destination, attrs=attrs)

    # The tmp file must have been cleaned up
    assert not (dest_dir / ".data.txt").exists()
    # The final destination should not exist either
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
    """FtpMover.finalize_atomic_transfer calls rename with correct basenames."""
    from trollmoves.movers import FtpMover

    origin = "/path/to/source.txt"
    destination = "ftp://ftphost/remote/dir/source.txt"
    mover = FtpMover(origin, destination)

    mock_connection = MagicMock()
    tmp_dest = urlparse("ftp://ftphost/remote/dir/.source.txt")
    final_dest = urlparse("ftp://ftphost/remote/dir/source.txt")

    with patch.object(mover, "get_connection", return_value=mock_connection):
        with patch("trollmoves.movers.ensure_remote_dirs"):
            mover.finalize_atomic_transfer(tmp_dest, final_dest)

    mock_connection.rename.assert_called_once_with(".source.txt", "source.txt")


def test_ftp_mover_finalize_updates_destination():
    """FtpMover.finalize_atomic_transfer updates mover.destination to final."""
    from trollmoves.movers import FtpMover

    mover = FtpMover("/origin.txt", "ftp://ftphost/dir/file.txt")
    mock_connection = MagicMock()
    tmp_dest = urlparse("ftp://ftphost/dir/.file.txt")
    final_dest = urlparse("ftp://ftphost/dir/file.txt")

    with patch.object(mover, "get_connection", return_value=mock_connection):
        with patch("trollmoves.movers.ensure_remote_dirs"):
            mover.finalize_atomic_transfer(tmp_dest, final_dest)

    assert mover.destination == final_dest


# ===========================================================================
# Group G – S3Mover (mocked S3 backend)
# ===========================================================================

@patch("trollmoves.movers.S3FileSystem")
def test_s3_mover_finalize_copy_mode(mock_s3fs):
    """s3_use_copy=True: finalize calls s3.copy and s3.rm with correct keys."""
    from trollmoves.movers import S3Mover

    mock_s3 = MagicMock()
    mock_s3fs.return_value = mock_s3

    mover = S3Mover("/local/file.txt", "s3://mybucket/dir/file.txt",
                    attrs={"s3_use_copy": True, "tmp_prefix": "."})

    tmp_dest = urlparse("s3://mybucket/dir/.file.txt")
    final_dest = urlparse("s3://mybucket/dir/file.txt")
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    mock_s3.copy.assert_called_once_with("mybucket/dir/.file.txt", "mybucket/dir/file.txt")
    mock_s3.rm.assert_called_once_with("mybucket/dir/.file.txt")
    assert mover.destination == urlparse("s3://mybucket/dir/file.txt")


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
