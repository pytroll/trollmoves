"""Helper utilities for movers to reduce duplication.

Contains functions to ensure remote directories exist. Designed to be small
and dependency-free; supports FTP-like objects (ftplib.FTP) and SFTP-like
(paramiko SFTPClient) objects by duck-typing.
"""

import ftplib
import os


def _ensure_remote_dirs_ftp(connection, parts):
    """Handle FTP-like directory creation (internal helper).

    Implements fast path optimization followed by fallback loop.
    For FTP connections: try single cwd(full_path) first; if fails,
    iterate through parts creating missing directories as needed.

    The connection's working directory is restored to its original value
    before returning, so callers must not rely on the CWD side-effect.
    """
    try:
        original_cwd = connection.pwd()
    except (ftplib.Error, OSError):
        original_cwd = None

    path = "/" + "/".join(parts)

    try:
        # Fast path: the full path already exists.
        if _ftp_dir_exists(connection, path):
            return

        # Build path from root, creating any missing segments.
        current = ""
        for part in parts:
            current = current + "/" + part
            if not _ftp_dir_exists(connection, current):
                _ftp_mkd(connection, current, part)
                _ftp_cwd_into(connection, current, part)
    finally:
        _restore_cwd(connection, original_cwd)


def _ftp_dir_exists(connection, path):
    """Return True if *path* exists as a directory on the FTP server.

    Uses cwd as a probe; does not permanently change the working directory
    (the caller is responsible for save/restore via _restore_cwd).
    """
    try:
        connection.cwd(path)
        return True
    except (ftplib.Error, OSError):
        return False


def _ftp_mkd(connection, full_path, part):
    """Create an FTP directory, trying *full_path* first then the relative *part*.

    Some servers reject absolute paths in MKD, hence the second attempt. If both
    fail the error is raised: a directory that cannot be created would otherwise
    surface much later as a confusing STOR failure.
    """
    try:
        connection.mkd(full_path)
    except (ftplib.Error, OSError):
        connection.mkd(part)


def _ftp_cwd_into(connection, full_path, part):
    """Change into an FTP directory, trying *full_path* first then the relative *part*."""
    try:
        connection.cwd(full_path)
    except (ftplib.Error, OSError):
        connection.cwd(part)


def _restore_cwd(connection, original_cwd):
    if original_cwd is None:
        return
    try:
        connection.cwd(original_cwd)
    except (ftplib.Error, OSError):
        pass


def _ensure_remote_dirs_sftp(connection, parts):
    """Handle SFTP-like directory creation (internal helper).

    Iterates through the path parts, stats each segment and creates it with mkdir
    if it is missing. A segment that cannot be created raises, rather than leaving
    the caller to fail later with a less informative error.
    """
    current = ""
    for part in parts:
        current = current + "/" + part
        try:
            connection.stat(current)
        except OSError:
            connection.mkdir(current)


def ensure_remote_dirs(connection, path):
    """Ensure directories exist on a remote connection.

    Supports FTP-like objects (with cwd() and mkd()) and SFTP-like objects
    (with stat() and mkdir()). The function is iterative (no recursion).

    Behavior mirrors previous recursive helper: try a single cwd(path) first; if
    that succeeds, return with only one cwd call. If it fails, create missing
    directories and cd into the final path as needed.
    """
    if not path or path == "/":
        return
    parts = [p for p in path.split("/") if p]
    if not parts:
        return

    # FTP-like API
    if hasattr(connection, "cwd") and hasattr(connection, "mkd"):
        _ensure_remote_dirs_ftp(connection, parts)
        return

    # SFTP-like API (paramiko.SFTPClient)
    if hasattr(connection, "stat") and hasattr(connection, "mkdir"):
        _ensure_remote_dirs_sftp(connection, parts)
        return

    raise TypeError("Unsupported connection type for ensure_remote_dirs")


def ensure_final_directory_for_rename(sftp_connection, final_destination_path):
    """Ensure the directory of *final_destination_path* exists on an SFTP connection.

    Used by finalize_atomic_transfer to make sure the target directory exists
    before renaming a temporary file to its final location.
    """
    ensure_remote_dirs(sftp_connection, os.path.dirname(final_destination_path))
