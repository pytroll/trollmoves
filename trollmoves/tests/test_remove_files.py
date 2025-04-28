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

"""Unittests for the utilities used to removing files from the remove_it script."""


import pytest

from trollmoves.filescleaner import FilesCleaner


class FakePublisher():
    """Implements a Fake Publisher for testing."""

    def __init__(self):
        """Initialize the class."""
        pass

    def send(self, msg):
        """Fake send message."""
        print(msg)


def test_remove_files_default(file_structure_with_some_old_files):
    """Test remove files - using default st_ctime as criteria."""
    pub = FakePublisher()

    dir_base, sub_dir1, sub_dir2 = file_structure_with_some_old_files

    basedir = str(dir_base)
    subdir1 = sub_dir1.name
    subdir2 = sub_dir2.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'templates': f'{subdir1}/*,{subdir2}/*.png',
            'hours': '3'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=False)
    size, num_files = fcleaner.clean_section()

    assert (dir_base / "a.txt").exists()
    assert (dir_base / "b.txt").exists()
    assert (dir_base / "c.csv").exists()
    assert (sub_dir1 / "a.nc").exists()
    assert (sub_dir1 / "b.nc").exists()
    assert (sub_dir1 / "c.h5").exists()
    assert (sub_dir2 / "a.png").exists()
    assert (sub_dir2 / "b.png").exists()
    assert (sub_dir2 / "c.tif").exists()


@pytest.mark.parametrize("hours, expected_files_removed",
                         [(3,
                           ("b.nc", "b.png")),
                          (9,
                           ())
                          ]
                         )
def test_remove_files_access_time(file_structure_with_some_old_files, hours, expected_files_removed):
    """Test remove files."""
    pub = FakePublisher()
    dir_base, sub_dir1, sub_dir2 = file_structure_with_some_old_files

    basedir = str(dir_base)
    subdir1 = sub_dir1.name
    subdir2 = sub_dir2.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'st_time': 'st_atime',
            'templates': f'{subdir1}/*,{subdir2}/*.png',
            'hours': f'{hours}'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=False)
    size, num_files = fcleaner.clean_section()

    filepaths = [(sub_dir1 / "a.nc"),
                 (sub_dir1 / "b.nc"),
                 (sub_dir1 / "c.h5"),
                 (sub_dir2 / "a.png"),
                 (sub_dir2 / "b.png"),
                 (sub_dir2 / "c.tif")]

    fpaths_copy = filepaths.copy()
    for fpath in fpaths_copy:
        for fname in expected_files_removed:
            if fname == fpath.name:
                assert not fpath.exists()
                filepaths.remove(fpath)

    for fpath in filepaths:
        assert fpath.exists()
