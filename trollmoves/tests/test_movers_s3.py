import os
import tempfile
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest

from trollmoves.movers import S3Mover

ALLOWED_S3_SETTINGS = ["anon", "endpoint_url", "key", "secret",
                       "token", "use_ssl", "s3_additional_kwargs", "client_kwargs",
                       "requester_pays", "default_block_size", "default_fill_cache",
                       "default_cache_type", "version_aware", "cache_regions",
                       "asynchronous", "config_kwargs", "kwargs", "session",
                       "max_concurrency", "fixed_upload_size", "profile",
                       # allow our atomic-transfer and multipart options to pass through sanitize
                       "s3_use_multipart", "s3_use_copy", "tmp_prefix", "s3_multipart_chunksize"]


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


def test_build_boto3_client_default_credentials():
    """_build_boto3_client uses boto3 default chain when no key/secret present."""
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        tmpname = f.name

    mock_boto_mod = MagicMock()
    mock_boto_mod.client = MagicMock(return_value=MagicMock())

    with patch("trollmoves.movers.boto3", new=mock_boto_mod):
        mover = S3Mover(tmpname, "s3://bucket/key", attrs={"client_kwargs": {"endpoint_url": "http://minio"}})
        mover._build_boto3_client()
        mock_boto_mod.client.assert_called_once_with("s3", endpoint_url="http://minio")

    os.remove(tmpname)


def test_build_boto3_client_explicit_credentials():
    """_build_boto3_client passes key, secret, and token when present."""
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        tmpname = f.name

    mock_boto_mod = MagicMock()
    mock_boto_mod.client = MagicMock(return_value=MagicMock())

    with patch("trollmoves.movers.boto3", new=mock_boto_mod):
        attrs = {"key": "AKID", "secret": "SECRET", "token": "MYTOKEN"}
        mover = S3Mover(tmpname, "s3://bucket/key", attrs=attrs)
        mover._build_boto3_client()
        mock_boto_mod.client.assert_called_once_with(
            "s3",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
            aws_session_token="MYTOKEN",
        )

    os.remove(tmpname)


def test_do_multipart_upload_aborts_on_error():
    """_do_multipart_upload aborts the upload when an error occurs mid-upload."""
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(b"data")
        tmpname = f.name

    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_client.create_multipart_upload.return_value = {"UploadId": "uid-abort"}
    mock_client.upload_part.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "fail"}}, "UploadPart"
    )

    mock_boto_mod = MagicMock()
    with patch("trollmoves.movers.boto3", new=mock_boto_mod):
        mover = S3Mover(tmpname, "s3://bucket/key", attrs={})
        with pytest.raises(ClientError):
            mover._do_multipart_upload(mock_client, "bucket", "key")

    mock_client.abort_multipart_upload.assert_called_once_with(
        Bucket="bucket", Key="key", UploadId="uid-abort"
    )
    os.remove(tmpname)


def test_do_multipart_upload_no_abort_when_create_fails():
    """_do_multipart_upload does not call abort if create_multipart_upload itself fails."""
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        tmpname = f.name

    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_client.create_multipart_upload.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "CreateMultipartUpload"
    )

    mock_boto_mod = MagicMock()
    with patch("trollmoves.movers.boto3", new=mock_boto_mod):
        mover = S3Mover(tmpname, "s3://bucket/key", attrs={})
        with pytest.raises(ClientError):
            mover._do_multipart_upload(mock_client, "bucket", "key")

    mock_client.abort_multipart_upload.assert_not_called()
    os.remove(tmpname)


def test_allowed_s3_settings():
    """Check that all expected S3 backend settings are allowed."""
    from trollmoves.movers import S3_ALLOWED_SETTINGS

    for setting in ALLOWED_S3_SETTINGS:
        assert setting in S3_ALLOWED_SETTINGS, f"{setting} should be in S3_ALLOWED_SETTINGS"


# ===========================================================================
# supports_atomic property for S3Mover
# ===========================================================================

def _s3mover_with_attrs(attrs):
    """Create a minimal S3Mover instance without triggering __init__ (no files needed)."""
    mover = S3Mover.__new__(S3Mover)
    mover.attrs = attrs
    mover._final_dest = None
    mover._tmp_dest = None
    return mover


@pytest.mark.parametrize("attrs,expected", [
    pytest.param({}, False, id="empty_attrs"),
    pytest.param({"s3_use_multipart": True}, True, id="multipart"),
    pytest.param({"s3_use_copy": True}, True, id="copy"),
    pytest.param({"s3_use_multipart": True, "s3_use_copy": True}, True, id="both"),
    pytest.param({"s3_use_multipart": False, "s3_use_copy": False}, False, id="both_false"),
])
def test_s3_mover_supports_atomic(attrs, expected):
    """S3Mover.supports_atomic reflects s3_use_multipart / s3_use_copy from attrs."""
    assert _s3mover_with_attrs(attrs).supports_atomic is expected


def test_s3_mover_does_not_mutate_caller_attrs():
    """S3Mover must not delete keys from the caller's shared connection_parameters dict.

    Trollmoves Server passes the same long-lived ``connection_parameters`` dict for
    every transfer, so in-place sanitizing would disable options after the first file.
    """
    connection_parameters = {"use_tmp_on_transfer": True, "s3_use_copy": True,
                             "endpoint_url": "http://endpoint", "not_an_s3_setting": "x"}
    expected = dict(connection_parameters)

    S3Mover("/local/file.txt", "s3://bucket/dir/file.txt", attrs=connection_parameters)

    assert connection_parameters == expected


def test_s3_mover_use_tmp_survives_repeated_transfers():
    """``use_tmp_on_transfer`` must still take effect on the second and later transfers."""
    connection_parameters = {"use_tmp_on_transfer": True, "s3_use_copy": True}

    first = S3Mover("/local/f.nc", "s3://bucket/dir/f.nc", attrs=connection_parameters)
    second = S3Mover("/local/g.nc", "s3://bucket/dir/g.nc", attrs=connection_parameters)

    assert first._tmp_dest.path == "/dir/.f.nc"
    assert second._tmp_dest.path == "/dir/.g.nc"


def test_use_tmp_on_transfer_is_an_allowed_s3_setting():
    """``use_tmp_on_transfer`` must survive attribute sanitizing."""
    from trollmoves.movers import S3_ALLOWED_SETTINGS

    assert "use_tmp_on_transfer" in S3_ALLOWED_SETTINGS


def test_s3_internal_keys_are_not_forwarded_to_backend():
    """Mover-internal options must not be passed on to S3FileSystem/boto3."""
    from trollmoves.movers import _S3_MOVER_INTERNAL_KEYS

    assert "use_tmp_on_transfer" in _S3_MOVER_INTERNAL_KEYS
