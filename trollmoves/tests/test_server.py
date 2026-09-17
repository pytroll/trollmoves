"""Test Trollmoves server."""

import datetime as dt
import logging
import os
import shutil
import time
import unittest
from collections import deque
from tempfile import NamedTemporaryFile, gettempdir
from unittest.mock import MagicMock, patch

import pytest
from trollsift import globify

from trollmoves.server import MoveItServer, parse_args

tmp_dir = gettempdir()


def test_file_detected_with_inotify_is_published(tmp_path):
    """Test that a file detected with inotify is published."""
    from threading import Thread

    from posttroll.testing import patched_publisher

    test_file_path = tmp_path / "my_file.hdf"

    config_file = f"""
        [eumetcast-hrit-0deg]
        origin={str(test_file_path)}
        publisher_port=9010
        topic=/some/hdf/file
        delete=False
    """
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as fd:
        fd.write(config_file)

    cmd_args = parse_args([str(config_path)])

    with patched_publisher() as message_list:
        server = MoveItServer(cmd_args)
        server.reload_cfg_file(cmd_args.config_file)
        thr = Thread(target=server.run)
        thr.start()

        # Wait a bit so that the watcher is properly up and running
        time.sleep(.2)
        with open(test_file_path, "w") as fd:
            fd.write("hello!")

        time.sleep(.2)
        try:
            assert len(message_list) == 1
            assert str(test_file_path) in message_list[0]
        finally:
            server.chains_stop()
            thr.join()


def test_create_watchdog_notifier(tmp_path):
    """Test creating a polling notifier."""
    from trollmoves.server import create_watchdog_polling_notifier

    fname = "20200428_1000_foo.tif"
    file_path = tmp_path / fname

    fname_pattern = tmp_path / "{start_time:%Y%m%d_%H%M}_{product}.tif"
    pattern_path = tmp_path / fname_pattern

    function_to_run = MagicMock()
    observer = create_watchdog_polling_notifier(globify(str(pattern_path)), function_to_run, timeout=.1)
    observer.start()

    with open(os.path.join(file_path), "w") as fid:
        fid.write("")

    # Wait for a while for the watchdog to register the event
    time.sleep(.2)

    observer.stop()
    observer.join()

    function_to_run.assert_called_with(str(file_path))


@pytest.mark.parametrize("config,expected_timeout",
                         [({"origin": tmp_dir}, 1.0),
                          ({"origin": tmp_dir, "watchdog_timeout": 2.0}, 2.0),
                          ({"origin": tmp_dir, "watchdog_timeout": "3.0"}, 3.0),
                          ])
@patch("trollmoves.server.PollingObserver")
def test_create_watchdog_notifier_timeout_default(PollingObserver, config, expected_timeout):
    """Test creating a watchdog notifier with default settings."""
    from trollmoves.server import Chain
    chain = Chain("some_chain", config)
    function_to_run = MagicMock()
    chain.create_notifier(notifier_builder=None, use_polling=True, function_to_run_on_matching_files=function_to_run)
    PollingObserver.assert_called_with(timeout=expected_timeout)


def test_create_posttroll_notifier():
    """Test creating a posttroll notifier."""
    from trollmoves.server import Chain
    config = {"listen": "some_topic"}
    chain = Chain("some_chain", config)
    function_to_run = MagicMock()
    # assert no crash
    from posttroll.testing import patched_subscriber_recv
    with patched_subscriber_recv(["hello"]):
        chain.create_notifier(notifier_builder=None,
                              use_polling=True,
                              function_to_run_on_matching_files=function_to_run)
        chain.start()
        chain.stop()


def test_handler_does_not_dispatch_files_not_matching_pattern():
    """Test that the handle does not dispatch files that are not matching the pattern."""
    from trollmoves.server import WatchdogCreationHandler

    function_to_run = MagicMock()

    handler = WatchdogCreationHandler(function_to_run, pattern="bar")
    event = MagicMock()
    event.dest_path = "foo"
    event.is_directory = False
    assert handler.dispatch(event) is None


def _run_process_notify(process_notify, publisher, tmpdir):
    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

    matching_pattern = os.path.join(tmpdir, fname_pattern)
    pathname = os.path.join(tmpdir, fname)
    kwargs = {"origin": matching_pattern,
              "request_address": "localhost",
              "request_port": "9001",
              "topic": "/topic"}

    with open(os.path.join(pathname), "w") as fid:
        fid.write("foo")

    process_notify(pathname, publisher, kwargs)

    return pathname, fname, kwargs


def _run_process_notify_with_message(process_notify, publisher, tmpdir):
    """Run process notification with a message."""
    from posttroll.message import Message
    fname = "20200428_1000_foo.tif"
    fname_pattern = "{start_time:%Y%m%d_%H%M}_{product}.tif"

    matching_pattern = os.path.join(tmpdir, fname_pattern)
    pathname = os.path.join(tmpdir, fname)
    kwargs = {"origin": matching_pattern,
              "request_address": "localhost",
              "request_port": "9001",
              "topic": "/topic"}

    with open(os.path.join(pathname), "w") as fid:
        fid.write("foo")

    # Make message from filepath:
    msg_string = ('pytroll://topic file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                  '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": '
                  f'"{os.path.basename(pathname)}", "uri": '
                  f'"{pathname}"'
                  '}')
    message = Message(rawstr=msg_string)
    process_notify(message, publisher, kwargs)

    return pathname, fname, kwargs


@patch("trollmoves.server.file_cache", new_callable=deque)
def test_process_notify_matching_file_as_message(file_cache, tmp_path):
    """Test process_notify() with a file matching the configured pattern."""
    from trollmoves.server import process_notification
    publisher = MagicMock()
    pathname, fname, kwargs = _run_process_notify_with_message(process_notification, publisher, tmp_path)


@patch("trollmoves.server.file_cache", new_callable=deque)
def test_process_notify_matching_file(file_cache, tmp_path):
    """Test process_notify() with a file matching the configured pattern."""
    from posttroll.message import Message

    from trollmoves.server import process_notification

    publisher = MagicMock()

    pathname, fname, kwargs = _run_process_notify(process_notification, publisher, tmp_path)

    # Check that the message was formed correctly
    message_info = {"start_time": dt.datetime(2020, 4, 28, 10, 0),
                    "product": "foo",
                    "uri": pathname,
                    "uid": fname,
                    "request_address": "localhost:9001"}

    message = Message(rawstr=publisher.send.mock_calls[0][1][0])
    assert message.subject == kwargs["topic"]
    assert message.type == "file"
    assert message.data == message_info

    assert "/topic/20200428_1000_foo.tif" in file_cache
    assert len(file_cache) == 1


class TestDeleter(unittest.TestCase):
    """Test the deleter."""

    def test_empty_init_arguments_does_not_crash_add(self):
        """Test that empty init arguments still work."""
        from trollmoves.server import Deleter
        Deleter(dict()).add("bla")


CONFIG_INI = b"""
[eumetcast-hrit-0deg]
origin = /local_disk/tellicast/received/MSGHRIT/H-000-{nominal_time:%Y%m%d%H%M}-{compressed:_<2s}
request_port = 9094
publisher_port = 9010
info = sensor=seviri;variant=0DEG
topic = /1b/hrit-segment/0deg
delete = False
# Everything below this should end up in connection_parameters dict
connection_uptime = 30
ssh_key_filename = id_rsa.pub
ssh_private_key_file = id_rsa
ssh_connection_timeout = 30
connection_parameters__secret = secret
connection_parameters__client_kwargs__endpoint_url = https://endpoint.url
connection_parameters__client_kwargs__verify = false
"""


def test_read_config_ini_with_dicts():
    """Test reading a config in ini format when dictionary values should be created."""
    from trollmoves.server import read_config

    with NamedTemporaryFile(suffix=".ini") as config_file:
        config_file.write(CONFIG_INI)
        config_file.flush()
        with pytest.warns(UserWarning, match="Consider using connection_parameters__"):
            config = read_config(config_file.name)
        eumetcast = config["eumetcast-hrit-0deg"]
        assert "origin" in eumetcast
        assert "request_port" in eumetcast
        assert "publisher_port" in eumetcast
        assert "info" in eumetcast
        assert "topic" in eumetcast
        assert "delete" in eumetcast
        expected_conn_params = {
            "secret": "secret",
            "client_kwargs": {
                "endpoint_url": "https://endpoint.url",
                "verify": False,
            },
            "connection_uptime": "30",
            "ssh_key_filename": "id_rsa.pub",
            "ssh_private_key_file": "id_rsa",
            "ssh_connection_timeout": "30",
        }
        assert eumetcast["connection_parameters"] == expected_conn_params


class TestMoveItServer:
    """Test the move it server."""

    def test_reloads_config_crashes_when_config_file_does_not_exist(self):
        """Test that reloading a non existing config file crashes."""
        cmd_args = parse_args(["--port", "9999", "somefile99999.cfg"])
        server = MoveItServer(cmd_args)
        with pytest.raises(FileNotFoundError):
            server.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    def test_reloads_config_on_example_config(self, fake_publisher):
        """Test that config can be reloaded with basic example."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(CONFIG_INI)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            server = MoveItServer(cmd_args)
            server.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.server.MoveItServer.reload_config")
    def test_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(CONFIG_INI)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            server = MoveItServer(cmd_args)
            server.reload_cfg_file(cmd_args.config_file)
            mock_reload_config.assert_called_once()

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.server.MoveItServer.reload_config")
    def test_signal_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded through signal."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(CONFIG_INI)
            config_filename = temporary_config_file.name
            cmd_args = parse_args([config_filename])
            client = MoveItServer(cmd_args)
            client.signal_reload_cfg_file()
            mock_reload_config.assert_called_once()


@patch("trollmoves.server.set_up_server_socket")
@patch("trollmoves.server.Poller.poll")
@patch("trollmoves.server.RequestManager._set_station")
@patch("trollmoves.server.RequestManager._set_out_socket")
@patch("trollmoves.server.RequestManager._get_address_and_payload")
@patch("trollmoves.server.RequestManager._validate_file_pattern")
@patch("trollmoves.server.RequestManager._process_request")
def test_requestmanager_run_valid_pytroll_message(patch_process_request,
                                                  patch_validate_file_pattern,
                                                  patch_get_address_and_payload,
                                                  patch_set_out_socket,
                                                  patch_set_station,
                                                  patch_poller,
                                                  patch_set_up_server_socket):
    """Test request manager run with valid address and payload."""
    from posttroll.message import _MAGICK
    from zmq import POLLIN

    from trollmoves.server import RequestManager
    payload = (_MAGICK +
               r"/test/1/2/3 info ras@hawaii 2008-04-11T22:13:22.123000 v1.01" +
               r' text/ascii "what' + r"'" + r's up doc"')
    address = b"tcp://192.168.10.8:37325"
    patch_set_up_server_socket.return_value = MagicMock(), MagicMock(), MagicMock()
    patch_get_address_and_payload.return_value = address, payload
    port = 9876
    patch_poller.return_value = {"POLLIN": POLLIN}
    req_man = RequestManager(port)
    req_man.out_socket = "POLLIN"
    req_man._run_loop()
    patch_process_request.assert_called_once()


@patch("trollmoves.server.set_up_server_socket")
@patch("trollmoves.server.Poller.poll")
@patch("trollmoves.server.RequestManager._set_station")
@patch("trollmoves.server.RequestManager._set_out_socket")
@patch("trollmoves.server.RequestManager._get_address_and_payload")
@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_run_MessageError_exception(patch_validate_file_pattern,
                                                   patch_get_address_and_payload,
                                                   patch_set_out_socket,
                                                   patch_set_station,
                                                   patch_poller,
                                                   patch_set_up_server_socket,
                                                   caplog):
    """Test request manager run with invalid payload causing a MessageError exception."""
    import logging

    from zmq import POLLIN

    from trollmoves.server import RequestManager
    patch_set_up_server_socket.return_value = MagicMock(), MagicMock(), MagicMock()
    patch_get_address_and_payload.return_value = "address", "fake_payload"
    port = 9876
    patch_poller.return_value = {"POLLIN": POLLIN}
    req_man = RequestManager(port)
    req_man.out_socket = "POLLIN"
    with caplog.at_level(logging.DEBUG):
        req_man._run_loop()
    assert "Failed to create message from payload: fake_payload with address address" in caplog.text


@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_is_delete_set(patch_validate_file_pattern):
    """Test delete default config."""
    from trollmoves.server import RequestManager
    port = 9876
    req_man = RequestManager(port, attrs={})
    assert req_man._is_delete_set() is False


@patch("trollmoves.server.RequestManager._validate_file_pattern")
def test_requestmanager_is_delete_set_True(patch_validate_file_pattern):
    """Test setting delete to True."""
    from trollmoves.server import RequestManager
    port = 9876
    req_man = RequestManager(port, attrs={"delete": True})
    assert req_man._is_delete_set() is True


def test_unpack_with_delete(tmp_path):
    """Test unpacking with deletion."""
    import bz2
    zipped_file = tmp_path / "my_file.txt.bz2"
    with open(zipped_file, "wb") as fd_:
        fd_.write(bz2.compress(b"hello world", 5))

    from trollmoves.server import unpack

    res = unpack(zipped_file, delete=True, working_directory=tmp_path, compression="bzip")
    assert not os.path.exists(zipped_file)
    assert res == os.path.splitext(zipped_file)[0]


def _create_chain(directory, function_to_run=None, **extra_config):
    """Create a started chain watching *directory* with a polling notifier."""
    from trollmoves.server import Chain

    config = {"origin": os.path.join(str(directory), "{product}.tif"),
              "topic": "/topic",
              "watchdog_timeout": 0.1}
    config.update(extra_config)
    chain = Chain("some_chain", config)
    chain.create_notifier(notifier_builder=None,
                          use_polling=True,
                          function_to_run_on_matching_files=function_to_run or MagicMock())
    return chain


def _break_the_watch(directory):
    """Make the watchdog watch on *directory* die, and put the directory back in place."""
    shutil.rmtree(directory)
    # Wait for the watchdog emitter to notice that the directory is gone
    time.sleep(.5)
    directory.mkdir()


def _create_server(tmp_path, directory, *extra_args):
    """Create a server with a single chain watching *directory*."""
    config_file = "[test_chain]\n"
    config_file += "origin=" + os.path.join(str(directory), "{product}.tif") + "\n"
    config_file += "topic=/topic\n"
    config_file += "watchdog_timeout=0.1\n"
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as fd:
        fd.write(config_file)

    cmd_args = parse_args([str(config_path), "--watchdog", *extra_args])
    server = MoveItServer(cmd_args)
    server.reload_cfg_file(cmd_args.config_file)
    return server


def test_process_path_skips_a_file_that_has_disappeared(tmp_path, caplog):
    """Test that a file that is gone when it is handled is skipped instead of raising."""
    from trollmoves.server import process_path

    publisher = MagicMock()
    config = {"origin": str(tmp_path / "{product}.tif"), "topic": "/topic", "request_port": "9001"}

    with caplog.at_level(logging.WARNING):
        process_path(config, str(tmp_path / "gone.tif"), publisher)

    publisher.send.assert_not_called()
    assert "gone.tif" in caplog.text


def test_process_old_files_survives_a_disappearing_file(tmp_path):
    """Test that the backlog handling survives a file that is removed while it is processed."""
    from trollmoves.server import process_old_files

    (tmp_path / "foo.tif").write_text("hello")

    def function_to_run(fname):
        raise FileNotFoundError(2, "No such file or directory", fname)

    process_old_files(str(tmp_path / "*.tif"), function_to_run)


def test_delete_file_that_is_already_gone(tmp_path):
    """Test that removing a file that does not exist is not an error."""
    from trollmoves.server import delete_file

    delete_file(str(tmp_path / "not_here.txt"))


def test_chain_health_check_passes_for_a_working_chain(tmp_path):
    """Test that a working chain is reported as healthy."""
    chain = _create_chain(tmp_path)
    chain.start()
    try:
        assert chain.check_health() is None
    finally:
        chain.stop()


def test_chain_health_check_detects_a_missing_directory(tmp_path):
    """Test that a chain watching a directory that is gone is reported as broken."""
    watched = tmp_path / "watched"
    watched.mkdir()
    chain = _create_chain(watched)
    chain.start()
    try:
        shutil.rmtree(watched)
        problem = chain.check_health()
        assert problem is not None
        assert str(watched) in problem
    finally:
        chain.stop()


def test_chain_health_check_detects_a_dead_watch_thread(tmp_path):
    """Test that a chain is reported as broken when the thread watching the files has died."""
    watched = tmp_path / "watched"
    watched.mkdir()
    chain = _create_chain(watched)
    chain.start()
    try:
        _break_the_watch(watched)
        assert chain.check_health() == "a filesystem watch thread is not running"
    finally:
        chain.stop()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_chain_health_check_detects_a_crashed_notifier_thread(tmp_path):
    """Test that a chain is reported as broken when its notifier thread has crashed."""
    watched = tmp_path / "watched"
    watched.mkdir()
    function_to_run = MagicMock(side_effect=FileNotFoundError(2, "No such file or directory"))
    chain = _create_chain(watched, function_to_run=function_to_run)
    chain.start()
    try:
        (watched / "foo.tif").write_text("hello")
        # Wait for the watchdog to notice the file and for the handler to blow up
        time.sleep(.5)
        assert chain.check_health() == "the notifier thread is not running"
    finally:
        chain.stop()


def test_restarting_a_chain_handles_the_files_that_arrived_meanwhile(tmp_path):
    """Test that restarting a chain makes it work again and picks up the files that were missed."""
    watched = tmp_path / "watched"
    watched.mkdir()
    function_to_run = MagicMock()
    chain = _create_chain(watched, function_to_run=function_to_run)
    chain.start()
    try:
        _break_the_watch(watched)
        missed_file = watched / "foo.tif"
        missed_file.write_text("hello")

        chain.restart_notifier()
        chain.process_backlog()

        assert chain.check_health() is None
        function_to_run.assert_called_once_with(str(missed_file), chain_config=chain.config)
    finally:
        chain.stop()


def test_server_restarts_a_chain_that_stopped_working(tmp_path):
    """Test that the server notices a broken chain and gets it working again."""
    from posttroll.testing import patched_publisher

    watched = tmp_path / "watched"
    watched.mkdir()
    with patched_publisher():
        server = _create_server(tmp_path, watched, "--chain-check-interval", "0")
        try:
            _break_the_watch(watched)
            chain = server.chains["test_chain"]
            assert chain.check_health() is not None

            server.check_chains()

            assert chain.check_health() is None
            assert chain.restarts == 1
            # The counter is only reset once the chain has been seen working again
            server.check_chains()
            assert chain.restarts == 0
        finally:
            server.chains_stop()


def test_server_keeps_a_chain_whose_directory_is_missing_at_startup(tmp_path):
    """Test that a chain is kept and started later when its directory shows up."""
    from posttroll.testing import patched_publisher

    watched = tmp_path / "watched"
    with patched_publisher():
        server = _create_server(tmp_path, watched, "--chain-check-interval", "0")
        try:
            assert "test_chain" in server.chains

            watched.mkdir()
            server.check_chains()

            assert server.chains["test_chain"].check_health() is None
        finally:
            server.chains_stop()


def test_server_gives_up_after_too_many_failed_restarts(tmp_path):
    """Test that the server raises an error when a chain can not be brought back to life."""
    from posttroll.testing import patched_publisher

    from trollmoves.server import ChainRecoveryError

    watched = tmp_path / "watched"
    watched.mkdir()
    with patched_publisher():
        server = _create_server(tmp_path, watched,
                                "--chain-check-interval", "0",
                                "--max-notifier-restarts", "3")
        try:
            shutil.rmtree(watched)
            with pytest.raises(ChainRecoveryError):
                for _ in range(3):
                    server.check_chains()
        finally:
            server.chains_stop()


def test_server_never_gives_up_when_restarts_are_unlimited(tmp_path):
    """Test that the server keeps trying when the maximum number of restarts is disabled."""
    from posttroll.testing import patched_publisher

    watched = tmp_path / "watched"
    watched.mkdir()
    with patched_publisher():
        server = _create_server(tmp_path, watched,
                                "--chain-check-interval", "0",
                                "--max-notifier-restarts", "0")
        try:
            shutil.rmtree(watched)
            for _ in range(5):
                server.check_chains()
            assert server.chains["test_chain"].restarts == 5
        finally:
            server.chains_stop()


def test_server_run_exits_when_a_chain_can_not_be_recovered(tmp_path):
    """Test that the server stops with an error so that a process manager can restart it."""
    from posttroll.testing import patched_publisher

    from trollmoves.server import ChainRecoveryError

    with patched_publisher():
        server = _create_server(tmp_path, tmp_path / "watched",
                                "--chain-check-interval", "0",
                                "--max-notifier-restarts", "1")
        try:
            with pytest.raises(ChainRecoveryError):
                server.run()
        finally:
            server.chains_stop()
