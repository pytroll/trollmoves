"""Helper utilities for movers to reduce duplication.

Contains functions to ensure local and remote directories exist. Designed to be small
and dependency-free; supports FTP-like objects (ftplib.FTP) and SFTP-like
(paramiko SFTPClient) objects by duck-typing.
"""

import os


def ensure_local_dir(path):
    """Ensure local directory exists for given path.

    If path is a file path, its directory is created. If path is a directory,
    it is created. No exception is raised if it already exists.
    """
    if not path:
        return
    dirname = path
    if os.path.isfile(path) or os.path.splitext(path)[1]:
        dirname = os.path.dirname(path) or "."
    os.makedirs(dirname, exist_ok=True)


def _ensure_remote_dirs_ftp(connection, parts):
    """Handle FTP-like directory creation (internal helper).

    Implements fast path optimization followed by fallback loop.
    For FTP connections: try single cwd(full_path) first; if fails,
    iterate through parts creating missing directories as needed.
    """
    path = "/" + "/".join(parts)

    # Fast path: if the full path already exists, a single cwd is sufficient
    try:
        connection.cwd(path)
        return
    except Exception:
        pass

    # Build path from root, creating missing segments and only cd'ing when needed
    current = ""
    for part in parts:
        current = current + "/" + part
        try:
            connection.cwd(current)
        except Exception:
            # try to create the directory; accept failures silently and proceed
            try:
                connection.mkd(current)
            except Exception:
                try:
                    connection.mkd(part)
                except Exception:
                    pass
            # after creating, change into it
            try:
                connection.cwd(current)
            except Exception:
                try:
                    connection.cwd(part)
                except Exception:
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
        except Exception:
            try:
                connection.mkdir(current)
            except Exception:
                try:
                    connection.mkdir(part)
                except Exception:
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
            except Exception:
                pass
