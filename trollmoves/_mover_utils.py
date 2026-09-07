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
    """Create an FTP directory, trying *full_path* first then the relative *part*."""
    try:
        connection.mkd(full_path)
    except (ftplib.Error, OSError):
        try:
            connection.mkd(part)
        except (ftplib.Error, OSError):
            pass


def _ftp_cwd_into(connection, full_path, part):
    """Change into an FTP directory, trying *full_path* first then the relative *part*."""
    try:
        connection.cwd(full_path)
    except (ftplib.Error, OSError):
        try:
            connection.cwd(part)
        except (ftplib.Error, OSError):
            pass


def _restore_cwd(connection, original_cwd):
    if original_cwd is None:
        return
    try:
        connection.cwd(original_cwd)
    except (ftplib.Error, OSError):
        pass


def _ensure_remote_dirs_sftp(connection, parts):
    """Handle SFTP-like directory creation (internal helper).

    For SFTP connections: iterate through path parts, stat each path segment
    and create with mkdir if it doesn't exist. Silently ignore all errors.
    """
    current = ""
    for part in parts:
        current = current + "/" + part
        try:
            connection.stat(current)
        except OSError:
            try:
                connection.mkdir(current)
            except OSError:
                try:
                    connection.mkdir(part)
                except OSError:
                    pass


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
    """Ensure final directory exists for a rename operation on SFTP.

    Used in finalize_atomic_transfer operations to ensure the target directory
    exists before renaming a temporary file to its final location. Attempts to
    stat each path segment; creates with mkdir if it doesn't exist. Silently
    ignores all errors to match existing SFTP behavior in movers.
    """
    final_dir = os.path.dirname(final_destination_path)
    if not final_dir:
        return

    parts = final_dir.split("/")
    path = ""
    for p in parts:
        if not p:
            continue
        path = path + "/" + p
        try:
            sftp_connection.stat(path)
        except OSError:
            try:
                sftp_connection.mkdir(path)
            except OSError:
                pass
