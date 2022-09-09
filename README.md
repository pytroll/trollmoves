# Trollmoves

Trollmoves is a package providing software for file transfers.

The common nominator is the use of Posttroll messaging to make requests, and to
publish the completed transfers. These messages can be used to trigger further
processing.

## Server/Client

Setup where one or more Server processes announce new files, and one or more
Client processes make transfer requests for the configured files.

### Trollmoves Server

Trollmoves Server is a process that follows a directory for new files, and publishes
a message when a matching file appears. If a Client makes a request for a file,
the file is transferred using one of the built-in movers (see below) based on the
destination given in the request.

### Trollmoves Client

Trollmoves Client is configured to subscribe to a specific topic, and to make requests
for matching files published by a Server. The destination of the file is given in
the request message. The Server handles the actual transfer.

Client can be configured to listen to multiple sources for the same files. The request
is made to the Server where the first announcement were received from.

### Trollmoves Mirror

Trollmoves Mirror is a setup of back-to-back Server and Client that is used for
example to handle transfers from internal network to external Client processes. The
Mirror receives announcements from the internal network, publishes the file on
external network, and upon receiving a request handles the transfer from internal
Server to temporary directory and further on to the external destination.

## Trollmoves Dispatcher

Trollmoves Dispatcher can push files from local file system to any destination supported
by the built-in movers. The dispatching is triggered by Posttroll messages published
by a process creating the files, or otherwise following the arrival/creation of
files.

## Individual movers

The individual movers can be used via the above listed processes, or used directly
in other applications. The movers can be imported from the ``trollmoves.movers``
module.

### ``FileMover``

``FileMover`` copies or moves a file between local filesystems.

### ``FtpMover``

``FtpMover`` transfers a local file to a FTP server.

### ``ScpMover``

``ScpMover`` uses SSH to transfer a local file to another (or the same) server.

### ``SftpMover``

``SftpMover`` uses SFTP protocol to transfer a local file to an SFTP server.

### ``S3Mover``

``S3Mover`` uploads a file to an S3 object storage.
