import os
import tempfile
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest

from trollmoves.movers import S3Mover, S3FileSystem, boto3


def test_s3_multipart_upload():
    # Create a temporary file with some content
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(b"hello world")
        tmpname = f.name

    # Setup mock boto3 client
    mock_client = MagicMock()
    mock_client.create_multipart_upload.return_value = {"UploadId": "upload123"}
    mock_client.upload_part.return_value = {"ETag": "etag-1"}
    mock_client.complete_multipart_upload.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    mock_boto_mod = MagicMock()
    mock_boto_mod.client = MagicMock(return_value=mock_client)

    with patch("trollmoves.movers.boto3", new=mock_boto_mod):
        # Destination: s3://mybucket/some/path/file
        dest = "s3://mybucket/some/path/file"
        attrs = {"s3_use_multipart": True, "client_kwargs": {}}

        mover = S3Mover(tmpname, dest, attrs=attrs)
        # Should use boto3 multipart upload and complete it
        mover.copy()

        # Assertions: multipart calls were made
        mock_client.create_multipart_upload.assert_called()
        assert mock_client.upload_part.called
        mock_client.complete_multipart_upload.assert_called()

        # Destination should be updated to final key
        assert mover.destination.scheme == "s3"
        assert "mybucket" in mover.destination.netloc or "mybucket" in mover.destination.path

    # Cleanup
    os.remove(tmpname)


@patch("trollmoves.movers.S3FileSystem")
def test_s3_copy_delete_fallback(mock_s3fs):
    # Create a temporary file with some content
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(b"fallback test")
        tmpname = f.name

    # Setup mock s3fs instance
    mock_s3 = MagicMock()
    mock_s3.exists.return_value = True
    mock_s3.copy.return_value = None
    mock_s3.rm.return_value = None
    mock_s3.put.return_value = None
    mock_s3fs.return_value = mock_s3

    # Destination path that will be used for tmp key
    dest = "s3://bucketname/dir/.tmpfile"
    attrs = {"s3_use_multipart": False, "s3_use_copy": True, "tmp_prefix": "."}

    mover = S3Mover(tmpname, dest, attrs=attrs)
    # copy should call s3.put
    mover.copy()
    mock_s3.put.assert_called_once()

    # Now finalize: simulate moving tmp key to final
    tmp_dest = urlparse("s3://bucketname/dir/.tmpfile")
    final_dest = urlparse("s3://bucketname/dir/file")
    mover.attrs = attrs
    mover.finalize_atomic_transfer(tmp_dest, final_dest)

    # copy+delete should be invoked
    mock_s3.copy.assert_called_once()
    mock_s3.rm.assert_called_once()

    # Cleanup
    os.remove(tmpname)
