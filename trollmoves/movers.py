"""Movers for the move_it scripts."""

import logging
import netrc
import os
import shutil
import socket
import sys
import time
import traceback
from ftplib import FTP, all_errors
from threading import Event, Lock, Thread, current_thread
from urllib.parse import urlparse

try:
    from s3fs import S3FileSystem
except ImportError:
    S3FileSystem = None
try:
    import boto3
except ImportError:
    boto3 = None

from trollmoves.utils import clean_url

from ._mover_utils import ensure_final_directory_for_rename, ensure_remote_dirs

S3_ALLOWED_SETTINGS = ["anon", "endpoint_url", "key", "secret",
                       "token", "use_ssl", "s3_additional_kwargs", "client_kwargs",
                       "requester_pays", "default_block_size", "default_fill_cache",
                       "default_cache_type", "version_aware", "cache_regions",
                       "asynchronous", "config_kwargs", "kwargs", "session",
                       "max_concurrency", "fixed_upload_size",
                       # allow our atomic-transfer and multipart options to pass through sanitize
                       "s3_use_multipart", "s3_use_copy", "tmp_prefix", "s3_multipart_chunksize"]

# Keys consumed by S3Mover logic; must not be forwarded to S3FileSystem or boto3 client
_S3_MOVER_INTERNAL_KEYS = frozenset({"s3_use_multipart", "s3_use_copy", "tmp_prefix", "s3_multipart_chunksize"})

LOGGER = logging.getLogger(__name__)


def move_it(pathname, destination, attrs=None, hook=None, rel_path=None, backup_targets=None):
    """Check if the file pointed by *pathname* is in the filelist, and move it if it is.

    The *destination* provided is used, and if *rel_path* is provided, it will
    be appended to the destination path.

    """
    try:
        dest_url = urlparse(destination)
    except AttributeError:
        dest_url = destination
    if rel_path is not None:
        new_path = os.path.join(dest_url.path, rel_path)
    else:
        new_path = dest_url.path
    new_dest = dest_url._replace(path=new_path)
    fake_dest = clean_url(new_dest)

    LOGGER.debug("Copying to: %s", fake_dest)
    try:
        LOGGER.debug("Scheme = %s", str(dest_url.scheme))
        mover_cls = MOVERS[dest_url.scheme]
    except KeyError:
        LOGGER.error("Unsupported protocol '" + str(dest_url.scheme) +
                     "'. Could not copy " + pathname + " to " + str(destination))
        raise

    try:
        tmp_dest = _get_tmp_destination(mover_cls, new_dest, attrs)
        mover = _create_mover(mover_cls, pathname, new_dest, attrs, backup_targets, tmp_dest)
        _copy(mover, new_dest, tmp_dest)
        if hook:
            hook(pathname, new_dest)
    except Exception as err:
        # Intentionally broad: logs and re-raises any failure from copy/finalize across all protocols.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.error("Something went wrong during copy of %s to %s: %s",
                     pathname, str(fake_dest), str(err))
        LOGGER.debug("".join(traceback.format_tb(exc_traceback)))
        raise err
    else:
        LOGGER.info("Successfully copied %s to %s", pathname, str(fake_dest))
    return mover.destination


def _create_mover(mover_cls, pathname, new_dest, attrs, backup_targets=None, tmp_dest=None):
    if tmp_dest:
        return mover_cls(pathname, tmp_dest, attrs=attrs, backup_targets=backup_targets)
    return mover_cls(pathname, new_dest, attrs=attrs, backup_targets=backup_targets)


def _get_tmp_destination(mover_cls, new_dest, attrs):
    use_tmp = bool(attrs and attrs.get("use_tmp_on_transfer"))
    if use_tmp:
        tmp_prefix = attrs.get("tmp_prefix", ".")
        tmp_dest = mover_cls.tmp_destination_for(new_dest, tmp_prefix)
        return tmp_dest
    return None


def _copy(mover, new_dest, tmp_dest=None):
    mover.copy()
    if tmp_dest:
        # finalize: default finalizer works for local schemes; subclasses should override
        try:
            mover.finalize_atomic_transfer(tmp_dest, new_dest)
        except Exception:
            # Intentionally broad: must clean up local tmp regardless of protocol error.
            # Re-raises so the caller sees the original failure.
            try:
                if hasattr(tmp_dest, "path") and os.path.exists(tmp_dest.path):
                    os.remove(tmp_dest.path)
            finally:
                raise


class Mover:
    """Base mover object. Doesn't do anything as it has to be subclassed."""

    def __init__(self, origin, destination, attrs=None, backup_targets=None):
        """Initialize the Mover."""
        LOGGER.debug("destination = %s", str(destination))
        try:
            self.destination = urlparse(destination)
        except AttributeError:
            self.destination = destination

        self._dest_username = self.destination.username
        self._dest_password = self.destination.password

        LOGGER.debug("Destination: %s", str(destination))
        self.origin = origin
        self.attrs = attrs or {}
        self.backup_targets = backup_targets

    def copy(self):
        """Copy the file."""
        raise NotImplementedError("Copy for scheme " + self.destination.scheme +
                                  " not implemented (yet).")

    def move(self):
        """Move the file."""
        raise NotImplementedError("Move for scheme " + self.destination.scheme +
                                  " not implemented (yet).")

    def supports_atomic(self):
        """Return True if this mover supports the default atomic finalize method.

        Subclasses should override if they can perform remote atomic rename.
        """
        try:
            scheme = self.destination.scheme
        except AttributeError:
            scheme = ""
        return scheme in ("", "file")

    @staticmethod
    def tmp_destination_for(dest, tmp_prefix="."):
        """Return a copy of dest with the basename prefixed by tmp_prefix."""
        try:
            path = dest.path
        except AttributeError:
            return dest
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        tmp_name = tmp_prefix + basename
        return dest._replace(path=os.path.join(dirname, tmp_name))

    def finalize_atomic_transfer(self, tmp_destination, final_destination):
        """Finalize atomic transfer by renaming tmp to final.

        Default implementation works for local filesystems (empty or 'file' scheme).
        Subclasses handling remote schemes must override this method.
        """
        try:
            tmp_path = tmp_destination.path
            final_path = final_destination.path
        except AttributeError:
            raise NotImplementedError("Finalize atomic transfer not implemented for remote schemes")

        final_dir = os.path.dirname(final_path)
        if final_dir:
            os.makedirs(final_dir, exist_ok=True)
        # Use os.replace for atomic rename where possible
        os.replace(tmp_path, final_path)
        # Update mover's destination to final
        self.destination = final_destination

    def get_connection(self, hostname, port, username=None):
        """Get the connection."""
        with self.active_connection_lock:
            LOGGER.debug("Destination username and passwd: %s %s", self._dest_username, self._dest_password)
            LOGGER.debug("Getting connection to %s@%s:%s", username, hostname, port)
            try:
                connection, timer = self.active_connections[(hostname, port, username)]
                if not self.is_connected(connection):
                    del self.active_connections[(hostname, port, username)]
                    LOGGER.debug("Resetting connection")
                    connection = self.open_connection()
                timer.cancel()
            except KeyError:
                connection = self.open_connection()

            timer = CTimer(int(self.attrs.get("connection_uptime", 30)), self.delete_connection, (connection,))
            timer.start()
            self.active_connections[(self.destination.hostname, port, username)] = connection, timer

            return connection

    def delete_connection(self, connection):
        """Delete active connection *connection*."""
        with self.active_connection_lock:
            LOGGER.debug("Closing connection to %s@%s:%s",
                         self._dest_username, self.destination.hostname, self.destination.port)
            try:
                if current_thread().finished.is_set():
                    return
            except AttributeError:
                pass
            try:
                self.close_connection(connection)
            finally:
                for key, (current_connection, current_timer) in self.active_connections.items():
                    if current_connection == connection:
                        del self.active_connections[key]
                        current_timer.cancel()
                        break


class FileMover(Mover):
    """Move files in the filesystem."""

    def copy(self):
        """Copy the file."""
        dirname = os.path.dirname(self.destination.path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        try:
            os.link(self.origin, self.destination.path)
        except OSError:
            shutil.copy(self.origin, self.destination.path)

    def move(self):
        """Move the file."""
        shutil.move(self.origin, self.destination.path)


class CTimer(Thread):
    """Call a function after a specified number of seconds.

    ::

        t = CTimer(30.0, f, args=(), kwargs={})
        t.start()
        t.cancel() # stop the timer's action if it's still waiting

    """

    def __init__(self, interval, function, args=(), kwargs=None):
        """Initialize the timer."""
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs or {}
        self.finished = Event()

    def cancel(self):
        """Stop the timer if it hasn't finished yet."""
        self.finished.set()

    def run(self):
        """Run the timer."""
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()


class FtpMover(Mover):
    """Move files over ftp."""

    active_connections = dict()
    active_connection_lock = Lock()

    def _get_netrc_authentication(self):
        """Get login authentications from netrc file if available."""
        try:
            secrets = netrc.netrc()
        except (netrc.NetrcParseError, FileNotFoundError) as e__:
            LOGGER.warning("Failed retrieve authentification details from netrc file! Exception: %s", str(e__))
            return

        LOGGER.debug("Destination hostname: %s", self.destination.hostname)
        LOGGER.debug("hosts: %s", str(list(secrets.hosts.keys())))
        LOGGER.debug("Check if hostname matches any listed in the netrc file")
        if self.destination.hostname in list(secrets.hosts.keys()):
            self._dest_username, account, self._dest_password = secrets.authenticators(self.destination.hostname)
            LOGGER.debug("Got username and password from netrc file!")

    def open_connection(self):
        """Open the connection and login."""
        connection = FTP(timeout=10)
        LOGGER.debug("Connect...")
        connection.connect(self.destination.hostname,
                           self.destination.port or 21)

        if not self._dest_username or not self._dest_password:
            # Check if usernams, password is stored in the $(HOME)/.netrc file:
            self._get_netrc_authentication()
            LOGGER.debug("Authentication retrieved from netrc file!")

        if self._dest_username and self._dest_password:
            connection.login(self._dest_username, self._dest_password)
        else:
            connection.login()

        return connection

    @staticmethod
    def is_connected(connection):
        """Check if the connection *connection* is active."""
        try:
            connection.voidcmd("NOOP")
            return True
        except all_errors:
            return False
        except IOError:
            return False

    @staticmethod
    def close_connection(connection):
        """Close connection *connection*."""
        try:
            connection.quit()
        except all_errors:
            connection.close()

    def move(self):
        """Upload the file and delete afterwards."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Upload the file."""
        connection = self.get_connection(self.destination.hostname, self.destination.port, self._dest_username)

        LOGGER.debug("cd to %s", os.path.dirname(self.destination.path))
        destination_dirname, destination_filename = os.path.split(self.destination.path)
        ensure_remote_dirs(connection, destination_dirname)
        if not destination_filename:
            destination_filename = os.path.basename(self.origin)
        with open(self.origin, "rb") as file_obj:
            connection.storbinary("STOR " + destination_filename,
                                  file_obj)

    def finalize_atomic_transfer(self, tmp_destination, final_destination):
        """Finalize atomic transfer by renaming tmp -> final on FTP server."""
        connection = self.get_connection(self.destination.hostname, self.destination.port, self._dest_username)

        dest_dirname = os.path.dirname(tmp_destination.path)
        tmp_basename = os.path.basename(tmp_destination.path)
        final_basename = os.path.basename(final_destination.path)
        ensure_remote_dirs(connection, dest_dirname)
        try:
            connection.rename(tmp_basename, final_basename)
        except all_errors as err:
            LOGGER.exception("Failed to finalize FTP atomic transfer: %s", str(err))
            raise
        self.destination = final_destination


class ScpMover(Mover):
    """Move files over ssh with scp."""

    active_connections = dict()
    active_connection_lock = Lock()

    def open_connection(self):
        """Open a connection."""
        import copy

        from paramiko import SSHClient, SSHException
        retries = 3
        ssh_key_filename = self.attrs.get("ssh_key_filename", None)
        try:
            timeout = float(self.attrs.get("ssh_connection_timeout", None))
        except TypeError:
            timeout = None
        backup_targets = copy.deepcopy(self.backup_targets)
        backup_targets_message = ""
        try:
            num_backup_targets = len(backup_targets)
        except TypeError:
            num_backup_targets = None
        while retries > 0:
            retries -= 1
            try:
                ssh_connection = SSHClient()
                ssh_connection.load_system_host_keys()
                ssh_connection.connect(self.destination.hostname,
                                       username=self._dest_username,
                                       port=self.destination.port or 22,
                                       key_filename=ssh_key_filename,
                                       timeout=timeout)
                LOGGER.debug("Successfully connected to %s:%s as %s",
                             self.destination.hostname,
                             self.destination.port or 22,
                             self._dest_username)
            except SSHException as sshe:
                LOGGER.exception("Failed to init SSHClient: %s", str(sshe))
            except socket.timeout as sto:
                LOGGER.exception("SSH connection timed out: %s", str(sto))
            except Exception as err:
                # Intentionally broad: SSHClient.connect() may raise unexpected exceptions
                # (e.g. from underlying transport or third-party SSH agents).
                LOGGER.exception("Unknown exception at init SSHClient: %s", str(err))
            else:
                return ssh_connection

            ssh_connection.close()
            time.sleep(2)
            LOGGER.debug("Retrying ssh connect ...")
            if retries == 0 and backup_targets:
                backup_target = backup_targets.pop(0)
                self.destination = self.destination._replace(netloc=f"{self.destination.username}@{backup_target}")
                LOGGER.info("Changing destination to backup target: %s", self.destination.hostname)
                retries = 3
                backup_targets_message = f" to primary and {num_backup_targets} backup host(s)"
        raise IOError(f"Failed to ssh connect after 3 attempts{backup_targets_message}.")

    @staticmethod
    def is_connected(connection):
        """Check if the connection *connection* is active."""
        LOGGER.debug("checking ssh connection")
        try:
            is_active = connection.get_transport().is_active()
            if is_active:
                LOGGER.debug("SSH connection is active.")
            return is_active
        except AttributeError:
            return False

    @staticmethod
    def close_connection(connection):
        """Close connection *connection*."""
        if isinstance(connection, tuple):
            connection[0].close()
        else:
            connection.close()

    def move(self):
        """Upload the file and delete it afterwards."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Upload the file."""
        from paramiko import SSHException as _SSHException
        from scp import SCPClient, SCPException

        ssh_connection = self.get_connection(self.destination.hostname,
                                             self.destination.port or 22,
                                             self._dest_username)
        try:
            scp = SCPClient(ssh_connection.get_transport())
        except (TypeError, _SSHException, OSError) as err:
            LOGGER.error("Failed to initiate SCPClient: %s", str(err))
            ssh_connection.close()
            raise

        try:
            scp.put(self.origin, self.destination.path)
        except OSError as osex:
            if osex.errno == 2:
                LOGGER.error("No such file or directory. File not transfered: "
                             "%s. Original error message: %s",
                             self.origin, str(osex))
                return
            else:
                LOGGER.error("OSError in scp.put: %s", str(osex))
                raise
        except (SCPException, _SSHException) as err:
            LOGGER.error("Something went wrong with scp: %s", str(err))
            LOGGER.error("Exception name %s", type(err).__name__)
            LOGGER.error("Exception args %s", str(err.args))
            raise
        finally:
            scp.close()

    def finalize_atomic_transfer(self, tmp_destination, final_destination):
        """Finalize atomic transfer for SCP by performing remote rename via SFTP."""
        from paramiko import SSHException as _SSHException
        ssh_connection = self.get_connection(self.destination.hostname,
                                             self.destination.port or 22,
                                             self._dest_username)
        sftp = None
        try:
            sftp = ssh_connection.open_sftp()
            ensure_final_directory_for_rename(sftp, final_destination.path)
            sftp.rename(tmp_destination.path, final_destination.path)
        finally:
            if sftp is not None:
                try:
                    sftp.close()
                except (_SSHException, OSError):
                    pass
        self.destination = final_destination


class SftpMover(Mover):
    """Move files over sftp."""

    def move(self):
        """Push the file."""
        self.copy()
        os.remove(self.origin)

    def copy(self):
        """Copy files.

        Uses high level paramiko functions.
        """
        import paramiko
        with paramiko.SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.connect(self.destination.hostname,
                        port=self.destination.port or 22,
                        username=self._dest_username,
                        allow_agent=True,
                        key_filename=self.attrs.get("ssh_private_key_file"))
            with ssh.open_sftp() as sftp:
                sftp.put(self.origin, self.destination.path)

    def finalize_atomic_transfer(self, tmp_destination, final_destination):
        """Finalize atomic transfer for SFTP by renaming tmp -> final on remote host."""
        import paramiko
        with paramiko.SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.connect(self.destination.hostname,
                        port=self.destination.port or 22,
                        username=self._dest_username,
                        allow_agent=True,
                        key_filename=self.attrs.get("ssh_private_key_file"))
            with ssh.open_sftp() as sftp:
                ensure_final_directory_for_rename(sftp, final_destination.path)
                sftp.rename(tmp_destination.path, final_destination.path)
        self.destination = final_destination


class S3Mover(Mover):
    """Move files to S3 cloud storage.

    The transfer is initiated by Trollmoves Client by having destination that starts with "s3://".

    All the connection configurations and such may be done using the `fsspec` configuration system:

    https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration

    An example configuration could be for example placed in `~/.config/fsspec/s3.json`::

        {
            "s3": {
                "client_kwargs": {"endpoint_url": "https://s3.server.foo.com"},
                "secret": "VERYBIGSECRET",
                "key": "ACCESSKEY"
            }
        }

    However, using the this procedure may not be useful if having several
    endpoints/buckets with their own access/secret keys. Instead one can use
    aws profiles (placed in `.aws/config`) to for instance set the
    access/secret keys for various endpoints and then keep the actual url of
    the endpoints in the yaml configuration (see examples/dispatch.yaml).

    See documentation on profiles here:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file


    NB! Special behaviour on destination filepath:

    If the destination prefix (~filepath) has a trailing slash ('/') the
    original filename will be appended (analogous to moving a file from one
    directory to another keeping the same filename).

    If the destination prefix does not have a trailing slash the operation will
    be analogous to moving a file from one directory to a new destination
    changing the filename. The new destination filename will be the last part
    of the provided destination following the last slash ('/').

    In the Trollmoves Server config, which is in .ini format, the connection parameters
    and other dictionary-like items can be defined with douple underscore format::

        connection_parameters__secret = secret
        connection_parameters__client_kwargs__endpoint_url = https://endpoint.url
        connection_parameters__client_kwargs__verify = false

    will result in a nested dictionary item::

        {
            'connection_parameters': {
                'secret': 'secret',
                'client_kwargs': {
                    'endpoint_url': 'https://endpoint.url',
                    'verify': False
                }
            }
        }

    Note that boolean values are converted. Numeric values are handled where they are used.

    """

    def __init__(self, origin, destination, attrs=None, backup_targets=None):
        """Initialize the S3Mover."""
        super().__init__(origin, destination, attrs, backup_targets)
        self._sanitize_attrs()

    def copy(self):
        """Copy the file to a bucket."""
        if S3FileSystem is None and boto3 is None:
            raise ImportError("S3Mover requires 's3fs' or 'boto3' to be installed.")

        destination_file_path = self._get_destination()
        LOGGER.debug("destination_file_path = %s", destination_file_path)

        if bool(self.attrs.get("s3_use_multipart", False)) and boto3 is not None:
            self._multipart_upload(destination_file_path)
            return

        # Fallback: use s3fs put to destination_file_path (tmp or final)
        if S3FileSystem is None:
            raise ImportError("S3Mover requires 's3fs' to be installed for non-multipart operations.")
        s3_attrs = {k: v for k, v in self.attrs.items() if k not in _S3_MOVER_INTERNAL_KEYS}
        s3 = S3FileSystem(**s3_attrs)
        LOGGER.debug("Before call to put: destination_file_path = %s", destination_file_path)
        LOGGER.debug("self.origin = %s", self.origin)
        _create_s3_destination_path(s3, destination_file_path)
        s3.put(self.origin, destination_file_path)

    def _build_boto3_client(self):
        """Build and return a boto3 S3 client from attrs.

        Reads client_kwargs, key, secret, and token from self.attrs.
        Falls back to boto3 default credential chain when key/secret are absent.
        """
        client_kwargs = self.attrs.get("client_kwargs", {})
        boto_kwargs = dict(client_kwargs) if isinstance(client_kwargs, dict) else {}
        if self.attrs.get("key") and self.attrs.get("secret"):
            return boto3.client(
                "s3",
                aws_access_key_id=self.attrs["key"],
                aws_secret_access_key=self.attrs["secret"],
                aws_session_token=self.attrs.get("token"),
                **boto_kwargs,
            )
        return boto3.client("s3", **boto_kwargs)

    def _do_multipart_upload(self, client, bucket, final_key):
        """Perform a multipart upload of self.origin to bucket/final_key.

        Uploads in chunks of s3_multipart_chunksize bytes (default 8 MB).
        On failure, aborts the multipart upload (best-effort) and re-raises.
        """
        from botocore.exceptions import BotoCoreError
        from botocore.exceptions import ClientError as BotoCoreClientError

        chunk_size = int(self.attrs.get("s3_multipart_chunksize", 8 * 1024 * 1024))
        upload_id = None
        try:
            mp = client.create_multipart_upload(Bucket=bucket, Key=final_key)
            upload_id = mp["UploadId"]
            upload_parts = []
            part_number = 1
            with open(self.origin, "rb") as f:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    resp = client.upload_part(
                        Bucket=bucket, Key=final_key, PartNumber=part_number,
                        UploadId=upload_id, Body=data,
                    )
                    upload_parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                    part_number += 1
            client.complete_multipart_upload(
                Bucket=bucket, Key=final_key, UploadId=upload_id,
                MultipartUpload={"Parts": upload_parts},
            )
        except (BotoCoreClientError, BotoCoreError, OSError) as e:
            LOGGER.exception("Multipart upload failed: %s", str(e))
            if upload_id is not None:
                try:
                    client.abort_multipart_upload(Bucket=bucket, Key=final_key, UploadId=upload_id)
                except (BotoCoreClientError, BotoCoreError):
                    pass
            raise

    def _multipart_upload(self, destination_file_path):
        """Orchestrate a boto3 multipart upload.

        Parses destination_file_path into bucket and key, strips the tmp_prefix from the
        basename if present to derive the final key, then uploads and updates self.destination.
        """
        tmp_prefix = self.attrs.get("tmp_prefix", ".")
        path_parts = destination_file_path.split("/")
        bucket = path_parts[0]
        key = "/".join(path_parts[1:]) if len(path_parts) > 1 else ""

        basename = key.split("/")[-1] if key else ""
        if basename.startswith(tmp_prefix):
            final_basename = basename[len(tmp_prefix):]
            final_key = key.rsplit("/", 1)[0] + "/" + final_basename if "/" in key else final_basename
        else:
            final_key = key

        client = self._build_boto3_client()
        self._do_multipart_upload(client, bucket, final_key)
        self.destination = urlparse("s3://" + bucket + "/" + final_key)


    def _sanitize_attrs(self):
        keys = list(self.attrs.keys())
        for key in keys:
            if key not in S3_ALLOWED_SETTINGS:
                del self.attrs[key]

    def _get_destination(self):
        bucket_parts = []
        bucket_parts.append(self.destination.netloc)

        if self.destination.path != "/":
            bucket_parts.append(self.destination.path.strip("/"))
        if self.destination.path.endswith("/"):
            bucket_parts.append(os.path.basename(self.origin))

        return "/".join(bucket_parts)

    def move(self):
        """Move the file."""
        self.copy()
        os.remove(self.origin)

    def finalize_atomic_transfer(self, tmp_destination, final_destination):
        """Finalize atomic transfer for S3.

        If multipart upload was used, copy() already wrote to the final key — just update
        self.destination. Otherwise perform a server-side copy+delete to move the tmp key
        to the final key (requires s3_use_copy=True).
        """
        use_multipart = bool(self.attrs.get("s3_use_multipart", False))
        use_copy = bool(self.attrs.get("s3_use_copy", False))

        # Derive source (tmp) S3 path from tmp_destination
        if tmp_destination:
            tmp_bucket = tmp_destination.netloc
            tmp_key = tmp_destination.path.lstrip("/")
            tmp_path = (tmp_bucket + "/" + tmp_key) if tmp_key else tmp_bucket
        else:
            tmp_path = self._get_destination()
            tmp_parts = tmp_path.split("/")
            tmp_bucket = tmp_parts[0]
            tmp_key = "/".join(tmp_parts[1:]) if len(tmp_parts) > 1 else ""

        # Derive destination (final) S3 path from final_destination
        final_bucket = final_destination.netloc
        final_key = final_destination.path.lstrip("/")
        final_path = (final_bucket + "/" + final_key) if final_key else final_bucket

        # If multipart upload was used, copy() already wrote to the final key
        if use_multipart and boto3 is not None:
            self.destination = final_destination
            return

        if not use_copy:
            raise NotImplementedError("S3 atomic finalize requires either multipart uploads or copy+delete fallback")

        s3_attrs = {k: v for k, v in self.attrs.items() if k not in _S3_MOVER_INTERNAL_KEYS}

        # use s3fs or boto3 to copy and delete tmp key
        if S3FileSystem is not None:
            s3 = S3FileSystem(**s3_attrs)
            s3.copy(tmp_path, final_path)
            s3.rm(tmp_path)
            self.destination = final_destination
            return

        if boto3 is None:
            raise ImportError("No S3 backend available for copy+delete finalize")
        # boto3 copy_object and delete_object
        client = self._build_boto3_client()
        copy_source = {"Bucket": tmp_bucket, "Key": tmp_key}
        client.copy_object(CopySource=copy_source, Bucket=final_bucket, Key=final_key)
        client.delete_object(Bucket=tmp_bucket, Key=tmp_key)
        self.destination = final_destination



def _create_s3_destination_path(s3, destination_file_path):
    destination_path = os.path.dirname(destination_file_path)
    if not s3.exists(destination_path):
        s3.mkdirs(destination_path)


MOVERS = {"ftp": FtpMover,
          "file": FileMover,
          "": FileMover,
          "scp": ScpMover,
          "sftp": SftpMover,
          "s3": S3Mover,
          }
