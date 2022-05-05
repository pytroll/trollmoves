"""Acceptance tests for move it server/client."""

from pytest_bdd import scenario, given, when, then
import pytest
from trollmoves.server import MoveItServer
from trollmoves.server import parse_args as parse_args_server
from trollmoves.client import MoveItClient
from trollmoves.client import parse_args as parse_args_client
from pathlib import Path
from datetime import datetime
import time
import socket
from threading import Thread
from posttroll.subscriber import Subscriber

import os


@pytest.fixture
def free_port():
    """Get a free port.

    From https://gist.github.com/bertjwregeer/0be94ced48383a42e70c3d9fff1f4ad0

    Returns a factory that finds the next free port that is available on the OS
    This is a bit of a hack, it does this by creating a new socket, and calling
    bind with the 0 port. The operating system will assign a brand new port,
    which we can find out using getsockname(). Once we have the new port
    information we close the socket thereby returning it to the free pool.
    This means it is technically possible for this function to return the same
    port twice (for example if run in very quick succession), however operating
    systems return a random port number in the default range (1024 - 65535),
    and it is highly unlikely for two processes to get the same port number.
    In other words, it is possible to flake, but incredibly unlikely.
    """
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", 0))
    portnum = s.getsockname()[1]
    s.close()

    return portnum


@scenario('move_it_server_client.feature', 'Simple file transfer')
def test_simple_transfer():
    """Stub for this scenario."""


@given("We have a source directory", target_fixture="source_dir")
def create_source_directory(tmp_path):
    """Create a source directory."""
    dirname = tmp_path / "source"
    os.mkdir(dirname)
    return dirname


@given("We have a separate destination directory", target_fixture="target_dir")
def create_target_directory(tmp_path):
    """Create a target directory."""
    dirname = tmp_path / "target"
    os.mkdir(dirname)
    return dirname


@pytest.fixture
def server(source_dir, tmp_path, reraise):
    """Create a move it server instance."""
    server_config = f"""
    [eumetcast-hrit-0deg]
    origin = {str(source_dir)}/H-000-{{nominal_time:%Y%m%d%H%M}}-__
    request_port = 9094
    publisher_port = 9010
    info = sensor=seviri;variant=0DEG
    topic = /1b/hrit-segment/0deg
    delete = False
    """
    server_config_filename = tmp_path / "server_config.cfg"
    with open(server_config_filename, "wb") as fp:
        fp.write(server_config.encode())
    cmd_args = parse_args_server(["--port", "9010", "-v", "-l", str(tmp_path / "move_it_server.log"),
                                  str(server_config_filename)])
    server = MoveItServer(cmd_args)
    server.reload_cfg_file(cmd_args.config_file)
    thread = Thread(target=reraise.wrap(server.run))
    thread.start()
    yield server
    server.chains_stop()
    thread.join()


@given("Move it server is started")
def start_move_it_server(server):
    """Start a move_it_server instance."""
    return server


@pytest.fixture
def client(target_dir, tmp_path, reraise):
    """Create a move it client."""
    client_config = f"""
    [eumetcast_hrit_0deg_ftp]
    providers = localhost:9010
    destination = file://{str(target_dir)}
    topic = /1b/hrit-segment/0deg
    publish_port = 0
    heartbeat_alarm_scale = 10
    """
    client_config_filename = tmp_path / "client_config.cfg"
    with open(client_config_filename, "wb") as fp:
        fp.write(client_config.encode())
    cmd_args = parse_args_client(["-v", "-l", str(tmp_path / "move_it_client.log"), str(client_config_filename)])
    client = MoveItClient(cmd_args)
    client.reload_cfg_file(cmd_args.config_file)
    thread = Thread(target=reraise.wrap(client.run))
    thread.start()
    yield client
    client.chains_stop()
    thread.join()


@given("Move it client is started")
def start_move_it_client(client):
    """Start a move_it_client instance."""
    return client


@when("A new file arrives matching the pattern", target_fixture="moved_filename")
def create_new_file(source_dir):
    """Create a new file in source_dir."""
    pattern = "H-000-%Y%m%d%H%M-__"
    filename = datetime.utcnow().strftime(pattern)
    path = Path(source_dir / filename)
    path.write_bytes(b"Very Important Satellite Data")
    return filename


@then("The file should be moved to the destination directory")
def file_moved(target_dir, moved_filename):
    """Check that files is moved."""
    path = Path(target_dir / moved_filename)
    time.sleep(1)
    assert path.exists()


@scenario('move_it_server_client.feature', 'Simple file publishing')
def test_simple_publishing():
    """Stub for this scenario."""


@pytest.fixture
def server_without_request_port(source_dir, tmp_path, free_port, reraise):
    """Create a move it server instance."""
    server_config = f"""
    [eumetcast-hrit-0deg]
    origin = {str(source_dir)}/H-000-{{nominal_time:%Y%m%d%H%M}}-__
    info = sensor=seviri;variant=0DEG
    topic = /1b/hrit-segment/0deg
    delete = False
    """
    server_config_filename = tmp_path / "server_config.cfg"
    with open(server_config_filename, "wb") as fp:
        fp.write(server_config.encode())
    cmd_args = parse_args_server(["--port", str(free_port), "-v", "-l", str(tmp_path / "move_it_server.log"),
                                  str(server_config_filename)])
    server = MoveItServer(cmd_args)
    server.reload_cfg_file(cmd_args.config_file)
    thread = Thread(target=reraise.wrap(server.run))
    thread.start()
    yield server
    server.chains_stop()
    thread.join()


@given("Move it server with no request port is started")
def start_move_it_server_without_request_port(server_without_request_port):
    """Start a move_it_server instance without a request port."""
    return server_without_request_port


@pytest.fixture
def subscriber(free_port):
    """Create a subscriber."""
    sub = Subscriber([f"tcp://localhost:{free_port}"], "")
    yield sub(timeout=2)
    sub.close()


@given("A posttroll subscriber is started")
def start_subscriber(subscriber):
    """Start a subscriber."""
    return subscriber


@then("A posttroll message with filesystem information should be issued by the server")
def check_message_for_filesystem_info(subscriber, tmp_path, source_dir, moved_filename):
    """Check the posttroll message for filesystem info."""
    msg = next(subscriber)
    host = socket.gethostname()
    expected_filesystem = {"cls": "fsspec.implementations.sftp.SFTPFileSystem", "protocol": "ssh", "args": [],
                           "host": host}
    expected_uri = f'ssh://{host}{source_dir}/{moved_filename}'

    assert msg.data["filesystem"] == expected_filesystem
    assert msg.data["uri"] == expected_uri
    assert msg.data["sensor"] == "seviri"
    assert msg.data["variant"] == "0DEG"


@scenario('move_it_server_client.feature', 'Simple file publishing with untarring')
def test_simple_publishing_with_untarring():
    """Stub for this scenario."""


@pytest.fixture
def server_without_request_port_and_untarring(source_dir, tmp_path, free_port, reraise):
    """Create a move it server instance."""
    server_config = f"""
    [eumetcast-hrit-0deg]
    origin = {source_dir}/H-000-{{nominal_time:%Y%m%d%H%M}}-__.tar
    info = sensor=seviri;variant=0DEG
    topic = /1b/hrit-segment/0deg
    delete = False
    unpack = tar
    """
    server_config_filename = tmp_path / "server_config.cfg"
    with open(server_config_filename, "wb") as fp:
        fp.write(server_config.encode())
    cmd_args = parse_args_server(["--port", str(free_port), "-v", "-l", str(tmp_path / "move_it_server.log"),
                                  str(server_config_filename)])
    server = MoveItServer(cmd_args)
    server.reload_cfg_file(cmd_args.config_file)
    thread = Thread(target=reraise.wrap(server.run))
    thread.start()
    yield server
    server.chains_stop()
    thread.join()


@given("Move it server with no request port is started with untarring option activated")
def start_move_it_server_with_untarring(server_without_request_port_and_untarring):
    """Start a move_it_server instance without a request port."""
    return server_without_request_port


@when("A new tar file arrives matching the pattern", target_fixture="moved_filename")
def create_new_tar_file(source_dir):
    """Create a new file in source_dir."""
    pattern = "H-000-%Y%m%d%H%M-__"
    filename = datetime.utcnow().strftime(pattern)
    path = source_dir / filename
    path.write_bytes(b"Very Important Satellite Data")
    tarfilename = source_dir / (filename + ".tar")
    from tarfile import TarFile
    with TarFile(tarfilename, mode="w") as tarfile:
        tarfile.add(path)
    return tarfilename


@then("A posttroll message with filesystem information and untarred file collection should be issued by the server")
def check_message_for_filesystem_info_and_untarring(subscriber, tmp_path, moved_filename):
    """Check the posttroll message for filesystem info and untarring."""
    msg = next(subscriber)
    host = socket.gethostname()
    expected_filesystem = {"cls": "fsspec.implementations.tar.TarFileSystem",
                           "protocol": "tar",
                           "args": [],
                           "target_options": {"host": host, "protocol": "ssh"},
                           "target_protocol": "ssh",
                           "fo": os.fspath(moved_filename)}

    expected_uri = f'tar:/{str(moved_filename)[:-4]}::ssh://{host}{moved_filename}'

    assert len(msg.data["dataset"]) == 1
    assert msg.data["dataset"][0]["filesystem"] == expected_filesystem
    assert msg.data["dataset"][0]["uri"] == expected_uri
    assert msg.data["sensor"] == "seviri"
    assert msg.data["variant"] == "0DEG"
