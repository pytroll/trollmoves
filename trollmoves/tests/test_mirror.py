#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 Trollmoves developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test Trollmoves mirror."""

import unittest
from unittest.mock import patch, DEFAULT
from trollmoves.mirror import parse_args, MoveItMirror
from tempfile import NamedTemporaryFile
import pytest


class TestMirrorDeleter(unittest.TestCase):
    """Test the deleter."""

    def test_empty_init_arguments_does_not_crash(self):
        """Test that no arguments to init works."""
        from trollmoves.mirror import MirrorDeleter
        MirrorDeleter()  # noqa

    def test_calling_delete_with_just_filename_does_not_crash(self):
        """Test that calling delete with just the filename does not crash."""
        from trollmoves.mirror import MirrorDeleter
        deleter = MirrorDeleter()
        deleter.delete("some_filename")


class TestMirrorRequestManager(unittest.TestCase):
    """Test the MRM."""

    def test_deleter_gets_attrs(self):
        """Test that the deleter gets the right info on init."""
        from trollmoves.mirror import MirrorRequestManager
        attrs = {'origin': 'here'}

        with patch("trollmoves.mirror.MirrorDeleter", autospec=True) as md:
            with patch.multiple("trollmoves.server", Poller=DEFAULT, get_context=DEFAULT):
                MirrorRequestManager("some_port", attrs)  # noqa
                assert md.call_args[0][0] == attrs


config_file = b"""
[eumetcast-hrit-0deg]
origin = /local_disk/tellicast/received/MSGHRIT/H-000-{nominal_time:%Y%m%d%H%M}-{compressed:_<2s}
request_port = 9094
publisher_port = 9010
info = sensor=seviri;variant=0DEG
topic = /1b/hrit-segment/0deg
delete = False
"""


class TestMoveItMirror:
    """Test the move it mirror."""

    def test_reloads_config_crashes_when_config_file_does_not_exist(self):
        """Test that reloading a non existing config file crashes."""
        cmd_args = parse_args(["--port", "9999", "somefile99999.cfg"])
        mirror = MoveItMirror(cmd_args)
        with pytest.raises(FileNotFoundError):
            mirror.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    def test_reloads_config_on_example_config(self, fake_publisher):
        """Test that config can be reloaded with basic example."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            mirror = MoveItMirror(cmd_args)
            mirror.reload_cfg_file(cmd_args.config_file)

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.mirror.MoveItMirror.reload_config")
    def test_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args(["--port", "9999", config_filename])
            mirror = MoveItMirror(cmd_args)
            mirror.reload_cfg_file(cmd_args.config_file)
            mock_reload_config.assert_called_once()

    @patch("trollmoves.move_it_base.Publisher")
    @patch("trollmoves.mirror.MoveItMirror.reload_config")
    def test_signal_reloads_config_calls_reload_config(self, mock_reload_config, mock_publisher):
        """Test that config file can be reloaded through signal."""
        with NamedTemporaryFile() as temporary_config_file:
            temporary_config_file.write(config_file)
            config_filename = temporary_config_file.name
            cmd_args = parse_args([config_filename])
            client = MoveItMirror(cmd_args)
            client.signal_reload_cfg_file()
            mock_reload_config.assert_called_once()
