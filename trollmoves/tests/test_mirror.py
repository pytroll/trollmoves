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
