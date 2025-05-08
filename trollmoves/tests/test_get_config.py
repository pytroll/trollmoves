#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2025 Pytroll Developers

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unittesting the reading and extraction of clean configurations."""


from configparser import RawConfigParser


def test_read_config_params(minimal_config_file):
    """Test reading configuration from a config file."""
    conf = RawConfigParser()
    conf.read(minimal_config_file)

    info = dict(conf.items('DEFAULT'))
    assert info == {'mailhost': 'localhost', 'to': 'some_users@xxx.yy', 'subject': 'Cleanup Error on {hostname}'}

    info = dict(conf.items('mytest_files1'))
    assert info == {'mailhost': 'localhost', 'to': 'some_users@xxx.yy', 'subject': 'Cleanup Error on {hostname}', 'base_dir': '/san1', 'templates': 'polar_in/sentinel3/olci/lvl1/*/*,polar_in/sentinel3/olci/lvl1/*', 'hours': '3'}  # noqa
