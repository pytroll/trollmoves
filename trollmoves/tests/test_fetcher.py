"""Tests for the fetcher."""

import logging

import yaml
from posttroll.message import Message
from posttroll.testing import patched_publisher, patched_subscriber_recv

from trollmoves.fetcher import fetch_from_message, fetch_from_subscriber


def test_fetch_message_with_filesystem(tmp_path):
    """Test fetch_message can use filesystem."""
    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"

    msg = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(tmp_path)}/sdr/{uid}", "path": "{str(tmp_path)}/sdr/{uid}", '
           '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    sdr = tmp_path / "sdr"
    sdr.mkdir()
    with open(sdr / uid, "w") as fd:
        fd.write("viirs data")

    dest_path1 = tmp_path / "dest1"
    dest_path1.mkdir()

    fetch_from_message(Message(rawstr=msg), dest_path1)

    assert (dest_path1 / uid).exists()


def test_fetch_message_with_uri(tmp_path):
    """Test fetch_message can use uri."""
    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)
    msg = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(sdr_file)}"' '}')

    dest_path2 = tmp_path / "dest2"
    dest_path2.mkdir()

    fetch_from_message(Message(rawstr=msg), dest_path2)
    assert (dest_path2 / uid).exists()


def test_fetch_message_logs(tmp_path, caplog):
    """Test fetch_message logs."""
    caplog.set_level("DEBUG")

    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)
    msg = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "{str(sdr_file)}"' '}')

    dest_path2 = tmp_path / "dest2"
    dest_path2.mkdir()

    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    publisher_settings = dict(nameservers=False, port=1979)

    with patched_publisher() as messages:
        with patched_subscriber_recv([Message(rawstr=msg)]):
            fetch_from_subscriber(dest_path2, subscriber_settings, publisher_settings)

    assert str(msg) in caplog.text
    assert str(dest_path2 / uid) in caplog.text
    assert f"Sending {messages[0]}" in caplog.text


def test_subscribe_and_fetch(tmp_path):
    """Test subscribe and fetch."""
    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
           '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    dest_path2 = tmp_path / "dest2"
    dest_path2.mkdir()

    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    publisher_settings = dict(nameservers=False, port=1979)

    with patched_publisher() as messages:
        with patched_subscriber_recv([Message(rawstr=msg)]):
            fetch_from_subscriber(dest_path2, subscriber_settings, publisher_settings)

    assert (dest_path2 / uid).exists()
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    expected_uri = f"{str(dest_path2)}/{uid}"
    assert "path" not in message.data
    assert "filesystem" not in message.data
    assert message.data["uri"] == expected_uri


def test_fetcher_cli(tmp_path):
    """Test the fetcher command-line interface."""
    config_file = tmp_path / "config.yaml"
    destination = tmp_path / "destination"
    create_config_file(destination, config_file)

    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg = ('pytroll://segment/viirs/l1b/ file a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
           '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    with patched_publisher() as messages:
        with patched_subscriber_recv([Message(rawstr=msg)]):
            from trollmoves.fetcher import cli
            cli([str(config_file)])

    assert (destination / uid).exists()
    assert len(messages) == 1
    message = Message(rawstr=messages[0])
    expected_uri = f"{str(destination)}/{uid}"
    assert "path" not in message.data
    assert "filesystem" not in message.data
    assert message.data["uri"] == expected_uri


def test_fetcher_does_not_try_to_fetch_non_file_messages(tmp_path):
    """Test fetcher does not try to fetch non-file messages."""
    config_file = tmp_path / "config.yaml"
    destination = tmp_path / "destination"
    create_config_file(destination, config_file)

    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg = ('pytroll://segment/viirs/l1b/ info a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
           '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    with patched_publisher() as messages:
        with patched_subscriber_recv([Message(rawstr=msg)]):

            from trollmoves.fetcher import cli
            cli([str(config_file)])

    assert not (destination / uid).exists()
    assert len(messages) == 0


def test_fetcher_uses_log_config(tmp_path):
    """Test fetcher uses log config."""
    config_file = tmp_path / "config.yaml"
    destination = tmp_path / "destination"
    create_config_file(destination, config_file)

    uid = "IVCDB_j02_d20240419_t1114110_e1115356_b07465_c20240419113435035578_cspp_dev.h5"
    sdr_file = tmp_path / "sdr" / uid
    create_data_file(sdr_file)

    msg = ('pytroll://segment/viirs/l1b/ info a001673@c22519.ad.smhi.se 2024-04-19T11:35:00.487388 v1.01 '
           'application/json {"sensor": "viirs", '
           f'"uid": "{uid}", "uri": "file://{str(sdr_file)}", "path": "{str(sdr_file)}", '
           '"filesystem": {"cls": "fsspec.implementations.local.LocalFileSystem", "protocol": "file", "args": []}}')

    log_config_file = tmp_path / "log_config.yaml"
    handler_name = "console123"
    create_log_config(log_config_file, handler_name)

    with patched_publisher():
        with patched_subscriber_recv([Message(rawstr=msg)]):

            from trollmoves.fetcher import cli
            cli([str(config_file), "-c", str(log_config_file)])

    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert root.handlers[0].name == handler_name


def create_config_file(destination, config_file):
    """Create a configuration file."""
    destination.mkdir()
    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    publisher_settings = dict(nameservers=False, port=1979)
    config = dict(destination=str(destination),
                  subscriber_config=subscriber_settings,
                  publisher_config=publisher_settings)
    with open(config_file, "w") as fd:
        fd.write(yaml.dump(config))

    return config


def create_data_file(path):
    """Create a data file."""
    path.parent.mkdir()

    with open(path, "w") as fd:
        fd.write("data")


def create_log_config(log_config_file, handler_name):
    """Create a log config file."""
    log_config = {
        "version": 1,
        "handlers": {
            handler_name: {
                "class": "logging.StreamHandler",
                "level": "INFO",
            },
        },
        "loggers": {
            "": {
                "level": "INFO",
                "handlers": [handler_name],
            },
        },
    }
    with open(log_config_file, "w") as fd:
        fd.write(yaml.dump(log_config))
