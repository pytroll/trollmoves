# Trollmoves

Trollmoves is a package providing software for file transfers.

The common nominator is the use of Posttroll messaging to make requests, and to
publish the completed transfers. These messages can be used to trigger further
processing.

The required libraries are listed for each of the parts below.

## Server/Client

Setup where one or more Server processes announce new files, and one or two
Client processes make transfer requests for the configured files.

### Trollmoves Server

Trollmoves Server is a process that follows a directory for new files, and publishes
a message when a matching file appears. If a Client makes a request for a file,
the file is transferred using one of the built-in movers (see below) based on the
destination given in the request.

Required libraries:
- ``netifaces``
- ``posttroll``
- ``pyzmq``
- ``trollsift``
- ``watchdog``

In addition, the required packages for the transfer protocol(s) to be used need to be
installed. See the mover documentation below for more details.

#### Running the server without a client
In some situations, it might be difficult to use the server/client architecture,
and thus there is a possibility to run the server in stand alone mode. To do
this, the only thing to do is omit the `request_port` configuration item in the
server configuration. From that point on, the server will send full uris in
the messages it publishes, along with a json representation of a `fsspec`
filesystem. From there, processes accepting these (eg `trollflow2`) will be able
to use `fsspec` to read and process the remote files.

### Trollmoves Client

Trollmoves Client is configured to subscribe to a specific topic, and to make requests
for matching files published by a Server. The destination of the file is given in
the request message. The Server handles the actual transfer.

Client can be configured to listen to multiple sources for the same files. The request
is made to the Server where the first announcement were received from.

Two clients can be configured to handle requests for a given data. This makes it possible
to make updates without outages, and in general add redundancy. One of the Client
processes is considered the primary, and the secondary will process the leftover
messages after a small (for example 0.2 s - 1 s) delay. The Clients communicate
which files are already handled, so duplicate transfers should not happen.

Required libraries:
- ``netifaces``
- ``posttroll``
- ``pyzmq``
- ``trollsift``

### Trollmoves Mirror

Trollmoves Mirror is a setup of back-to-back Server and Client that is used for
example to handle transfers from internal network to external Client processes. The
Mirror receives announcements from the internal network, publishes the file on
external network, and upon receiving a request handles the transfer from internal
Server to temporary directory and further on to the external destination.

Required libraries:
- ``netifaces``
- ``posttroll``
- ``pyzmq``
- ``trollsift``
- ``watchdog``

In addition, the required packages for the transfer protocol(s) to be used. See the
mover documentation below for more details.

## Trollmoves Dispatcher

Trollmoves Dispatcher can push files from local file system to any destination supported
by the built-in movers. The dispatching is triggered by Posttroll messages published
by a process creating the files, or otherwise following the arrival/creation of
files.

Required libraries:
- ``netifaces``
- ``posttroll``
- ``pyinotify``
- ``pyzmq``
- ``trollsift``

In addition, the required packages for the transfer protocol(s) to be used. See the
mover documentation below for more details.

## Individual movers

The individual movers can be used via the above listed processes, or used directly
in other applications. The movers can be imported from the ``trollmoves.movers``
module.

### ``FileMover``

``FileMover`` copies or moves a file between local filesystems.

Additional required packages: none.

### ``FtpMover``

``FtpMover`` transfers a local file to a FTP server.

Additional required packages: none.

### ``ScpMover``

``ScpMover`` uses SSH to transfer a local file to another (or the same) server.

Additional required packages:
- ``scp``
- ``paramiko``

### ``SftpMover``

``SftpMover`` uses SFTP protocol to transfer a local file to an SFTP server.

Additional required packages:
- ``paramiko``

### ``S3Mover``

``S3Mover`` uploads a file to an S3 object storage.

Additional required packages:
- ``s3fs``

Special behaviour on destination filepath when using the S3Mover class:

If the destination prefix (~filepath) has a trailing slash ('/') the original
filename will be appended (analogous to moving a file from one directory to
another keeping the same filename).

If the destination prefix does not have a trailing slash the operation will be
analogous to moving a file from one directory to a new destination changing the
filename. The new destination filename will be the last part of the provided
destination following the last slash ('/').

New atomic-transfer options (opt-in)

To avoid exposing partially-uploaded files, movers can be configured to upload
first to a temporary name and be renamed/activated only after the transfer
completes. These options are passed via the mover's connection_parameters or
attrs dictionary.

- use_tmp_on_transfer: boolean (default: False)
    If true, movers will upload to a temporary destination (see tmp_prefix)
    and finalize the transfer by renaming/moving the tmp object to the final
    name after successful transfer.
- tmp_prefix: string (default: '.')
    Prefix to use for temporary filenames (e.g. ".filename").

S3-specific options
- s3_use_multipart: boolean (default: True)
    When True and boto3 is available, S3Mover will perform a multipart upload
    directly to the final key and CompleteMultipartUpload to make the object
    visible atomically.
- s3_use_copy: boolean (default: False)
    If multipart uploads are not used, enabling this will finalize an upload
    performed to a tmp key by performing a server-side copy (CopyObject) to
    the final key and deleting the temporary key. This is compatible with
    s3fs or boto3 backends but requires additional permissions.
- s3_multipart_chunksize: integer (default: 8388608)
    Chunk size (bytes) used for multipart uploads when boto3 multipart is used.

Behavior notes
- Multipart uploads (preferred) avoid the extra server-side copy step but
  require boto3 and appropriate permissions. Copy+delete is provided as a
  fallback for S3-compatible endpoints that do not support multipart.
- The tmp_prefix and use_tmp_on_transfer options are intentionally opt-in to
  preserve existing behavior by default.


## s3downloader

This module is able to download files from a s3 endpoint.

The s3downloader module need to get posttroll messages from eg. s3stalker from the pytroll-collectors module which announces new available files in a configured s3 bucket. The s3downloader then downloads these files.

Example config for the module is given in `examples/s3downloader-config.yaml`. If you use a `nameserver` with muliticast (nameserver default) you don't need to include the nameserver option nor the service name in the config. Opposite if you use nameserver without multicast.

Additional required packages:
- ``boto3``
- ``botocore``


# License

Copyright 2012 Trollmoves developers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
