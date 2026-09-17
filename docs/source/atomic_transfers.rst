Transfers using temporary filenames
====================================

Overview
--------

By default, movers write files directly to their final destination. This means a consumer
watching the destination directory may pick up a partially-transferred file before the
transfer completes.

To avoid this, Trollmoves movers can be configured to upload to a **temporary filename**
first and rename the file to its final name only after the transfer completes successfully.
From the consumer's perspective the file appears atomically — it either does not exist yet
or is fully present.

This behaviour is controlled by the ``use_tmp_on_transfer`` option (off by default).


Configuration options
---------------------

The following options are passed via a ``connection_parameters`` block (Server/Client,
Dispatcher) or directly as an ``attrs`` dictionary when using the Python API.

.. list-table::
   :widths: 30 15 55
   :header-rows: 1

   * - Option
     - Default
     - Description
   * - ``use_tmp_on_transfer``
     - ``False``
     - Upload to a temporary filename first, then rename to the final name on success.
   * - ``tmp_prefix``
     - ``"."``
     - String prepended to the filename to form the temporary name.
       For example, ``data.txt`` becomes ``.data.txt`` with the default prefix.
   * - ``s3_use_multipart``
     - ``False``
     - *S3 only.* Use boto3 multipart upload so the object becomes visible
       atomically when ``CompleteMultipartUpload`` is called.
       Requires ``boto3`` to be installed.
   * - ``s3_use_copy``
     - ``False``
     - *S3 only.* If multipart uploads are not used, finalize by performing a
       server-side ``CopyObject`` to the final key followed by deleting the
       temporary key. Compatible with both ``s3fs`` and ``boto3`` backends.
   * - ``s3_multipart_chunksize``
     - ``8388608``
     - *S3 only.* Chunk size in bytes for boto3 multipart uploads (default: 8 MiB).


Protocol support
----------------

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Mover
     - Supported
     - How the rename is performed
   * - ``FileMover`` (``file://``, local)
     - Yes
     - ``os.replace()`` — atomic rename on the local filesystem.
   * - ``FtpMover`` (``ftp://``)
     - Yes
     - FTP ``RNFR``/``RNTO`` command pair.
   * - ``ScpMover`` (``scp://``)
     - Yes
     - SFTP rename over the same SSH connection.
   * - ``SftpMover`` (``sftp://``)
     - Yes
     - SFTP rename.
   * - ``S3Mover`` (``s3://``)
     - Conditional
     - Requires either ``s3_use_multipart`` (preferred) or ``s3_use_copy``.
       See :ref:`s3-notes` below.

The base ``Mover`` class returns ``False`` for ``supports_atomic``. If
``use_tmp_on_transfer`` is set but the mover does not support atomic transfers, a warning
is logged and the transfer falls back to writing directly to the final destination.


Via Trollmoves Server / Client
--------------------------------

In the Server INI configuration, connection parameters are specified with the
``connection_parameters__`` prefix under ``[DEFAULT]`` (applies to all sections) or
under a specific section.

.. code-block:: ini

   [DEFAULT]
   # Transfer to a temporary name first, then rename to final
   connection_parameters__use_tmp_on_transfer = True
   connection_parameters__tmp_prefix = .

   [eumetcast-hrit-0deg]
   origin = /data/received/MSGHRIT/H-000-*
   request_port = 9094
   topic = /1b/hrit-segment/0deg

The Client does not need any additional configuration — it requests the transfer from the
Server and the Server applies the ``connection_parameters`` when calling the mover.

For **S3 destinations**, also configure the S3-specific options:

.. code-block:: ini

   [DEFAULT]
   connection_parameters__use_tmp_on_transfer = True
   connection_parameters__s3_use_multipart = True
   # connection_parameters__s3_use_copy = False
   # connection_parameters__s3_multipart_chunksize = 8388608


Via Trollmoves Dispatcher
--------------------------

In the Dispatcher YAML configuration, the options are nested under
``connection_parameters`` for each target:

.. code-block:: yaml

   target-local:
     host: file:///output/data/
     connection_parameters:
       use_tmp_on_transfer: True
       tmp_prefix: "."

   target-s3:
     host: s3://my-bucket/
     connection_parameters:
       client_kwargs:
         endpoint_url: "https://s3.example.com"
       key: "ACCESS_KEY"
       secret: "SECRET_KEY"
       # Atomic transfer options
       use_tmp_on_transfer: True
       s3_use_multipart: True
       # s3_multipart_chunksize: 8388608


Via ``movers.move_it()``
------------------------

Pass the options in the ``attrs`` dictionary:

.. code-block:: python

   from trollmoves.movers import move_it

   move_it(
       "/local/data/myfile.txt",
       "scp://user@remote.host/data/myfile.txt",
       attrs={
           "use_tmp_on_transfer": True,
           "tmp_prefix": ".",
       },
   )

``move_it()`` selects the appropriate mover based on the URL scheme, creates it with
the given ``attrs``, and calls ``mover.copy()``. The mover handles the temporary name
and rename internally.


Via mover classes directly
---------------------------

Instantiate a mover with ``attrs`` containing the transfer options and call ``copy()``:

.. code-block:: python

   from urllib.parse import urlparse
   from trollmoves.movers import FileMover

   mover = FileMover(
       "/local/data/myfile.txt",
       urlparse("file:///output/data/myfile.txt"),
       attrs={
           "use_tmp_on_transfer": True,
           "tmp_prefix": ".",
       },
   )
   mover.copy()
   # mover.destination now points to the final path

The temporary name and finalization are handled entirely inside ``mover.copy()``.
If an error occurs during transfer or during the rename, the mover removes the
temporary file -- locally or on the remote side, depending on the protocol --
before the exception is re-raised. A cleanup that fails is logged as a warning
and does not replace the original transfer error.


.. _s3-notes:

S3-specific notes
-----------------

S3 does not support in-place rename of objects, so two strategies are available:

**Multipart upload** (``s3_use_multipart = True``, requires ``boto3``)
   The file is uploaded in chunks and the object only becomes visible when
   ``CompleteMultipartUpload`` is called. No extra copy step is needed.
   This is the preferred option and avoids additional API calls and permissions.

**Copy + delete** (``s3_use_copy = True``)
   The file is first uploaded to the temporary key, then ``CopyObject`` is used to
   duplicate it to the final key, followed by deleting the temporary key.
   Works with both ``s3fs`` and ``boto3``. Requires ``s3:CopyObject`` and
   ``s3:DeleteObject`` permissions in addition to the standard upload permissions.

If neither ``s3_use_multipart`` nor ``s3_use_copy`` is set, ``S3Mover.supports_atomic``
returns ``False`` and ``use_tmp_on_transfer`` will have no effect (a warning is logged).
