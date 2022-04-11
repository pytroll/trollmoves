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
from threading import Thread

import os


@scenario('move_it_server_client.feature', 'Simple file transfer')
def test_simple_transfer():
    """Stub for this scenario."""
    pass


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
def server(source_dir, tmp_path):
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
    thread = Thread(target=server.run)
    thread.start()
    yield server
    server.chains_stop()
    thread.join()


@given("Move it server is started")
def start_move_it_server(server):
    """Start a move_it_server instance."""
    return server


@pytest.fixture
def client(target_dir, tmp_path):
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
    thread = Thread(target=client.run)
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
