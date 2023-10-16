"""Tests for move_it."""

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


def test_move_it_published_a_message(tmp_path):
    """Test that move it is publishing messages when provided a port."""
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
    message = message_list[0]
    assert message.type == "file"
    assert message.data == {"sensors": "seviri", "stream": "eumetcast", "number": "1",
                            "uri": str(output_dir / "bla1.txt"), "uid": "bla1.txt"}
