"""Tests for move_it."""

import logging
import os
import time

from posttroll.testing import patched_publisher

from trollmoves.move_it import MoveItSimple
from trollmoves.server import parse_args

move_it_config_template = """[eumetcast_hrit]
delete=False
topic=/HRIT/L0/dev
info=sensors=seviri;stream=eumetcast
"""


def test_move_it_moves_files(tmp_path):
    """Test that move it moves a file."""
    input_dir, output_dir, config_file = create_config_file(tmp_path)

    cmd_args = parse_args([str(config_file)], default_port=None)
    move_it_thread = MoveItSimple(cmd_args)

    move_it_thread.reload_cfg_file(cmd_args.config_file)

    from threading import Thread
    thr = Thread(target=move_it_thread.run)
    thr.start()

    with open(input_dir / "bla1.txt", "w"):
        pass
    time.sleep(.1)
    try:
        assert move_it_thread.publisher is None
        assert os.path.exists(output_dir / "bla1.txt")
    finally:
        move_it_thread.chains_stop()


def create_config_file(tmp_path):
    """Create a move_it config file."""
    input_dir = tmp_path / "in"
    output_dir = tmp_path / "out"
    os.mkdir(input_dir)
    os.mkdir(output_dir)
    origin = "origin=" + str(input_dir / "bla{number:1s}.txt")
    destinations = "destinations=" + str(output_dir)
    local_move_it_config = "\n".join([move_it_config_template, origin, destinations])
    config_file = tmp_path / "move_it.cfg"
    with open(config_file, "w") as fd:
        fd.write(local_move_it_config)
    return input_dir, output_dir, config_file


def test_move_it_published_a_message(tmp_path):
    """Test that move it is publishing messages when provided a port."""
    from posttroll.message import Message
    input_dir, output_dir, config_file = create_config_file(tmp_path)

    with patched_publisher() as message_list:
        cmd_args = parse_args([str(config_file), "-p", "2022"])
        move_it_thread = MoveItSimple(cmd_args)

        move_it_thread.reload_cfg_file(cmd_args.config_file)

        from threading import Thread
        thr = Thread(target=move_it_thread.run)
        thr.start()

        with open(input_dir / "bla1.txt", "w"):
            pass
        time.sleep(.1)
        try:
            assert os.path.exists(output_dir / "bla1.txt")
        finally:
            move_it_thread.chains_stop()

    assert len(message_list) == 1
    message = Message(rawstr=message_list[0])
    assert message.type == "file"
    assert message.data == {"sensors": "seviri", "stream": "eumetcast", "number": "1",
                            "uri": str(output_dir / "bla1.txt"), "uid": "bla1.txt"}


def test_move_it_does_not_copy_a_file_that_could_not_be_decompressed(tmp_path, caplog):
    """Test that the compressed file is not copied in place of the decompressed one."""
    from trollmoves.move_it import process_notify

    compressed_file = tmp_path / "bla1.txt.bz2"
    compressed_file.write_bytes(b"this is not bzip2 data")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (tmp_path / "work").mkdir()
    chain_config = {"origin": str(tmp_path / "bla{number:1s}.txt.bz2"),
                    "compression": "bzip",
                    "working_directory": str(tmp_path / "work"),
                    "decompression_retry_delay": "0",
                    "destinations": str(output_dir),
                    "delete": True,
                    "delete_hook": None}

    with caplog.at_level(logging.ERROR):
        process_notify(str(compressed_file), None, chain_config)

    assert os.listdir(output_dir) == []
    assert compressed_file.exists()
    assert "bla1.txt.bz2" in caplog.text
