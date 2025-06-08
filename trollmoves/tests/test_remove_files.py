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


import logging

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
            'stat_time_method': 'st_atime',
            'templates': f'{subdir1}/*,{subdir2}/*.png',
            'hours': f'{hours}'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=False)
    size, num_files = fcleaner.clean_section()

    if hours < 6:
        assert num_files == 2
        assert size == 0
    else:
        assert num_files == 0
        assert size == 0

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


def test_remove_files_access_time_dryrun(file_structure_with_some_old_files, caplog):
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
            'stat_time_method': 'st_atime',
            'templates': f'{subdir1}/*,{subdir2}/*.png',
            'hours': '3'}

    with caplog.at_level(logging.DEBUG):
        fcleaner = FilesCleaner(pub, section, info, dry_run=True)
        size, num_files = fcleaner.clean_section()

    assert num_files == 0

    log_output1 = f'Would remove {(sub_dir1 / "b.nc")}'
    log_output2 = f'Would remove {(sub_dir2 / "b.png")}'
    assert log_output1 in caplog.text
    assert log_output2 in caplog.text


def test_remove_files_path_missing(file_structure_with_some_old_files, caplog):
    """Test remove files in file structure with an empty directory."""
    pub = FakePublisher()
    _, sub_dir1, sub_dir2 = file_structure_with_some_old_files

    basedir = '/non/existing/directory'
    subdir1 = sub_dir1.name
    subdir2 = sub_dir2.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': basedir,
            'stat_time_method': 'st_atime',
            'templates': f'{subdir1}/*.png,{subdir2}/*',
            'hours': '6'}

    with caplog.at_level(logging.WARNING):
        fcleaner = FilesCleaner(pub, section, info, dry_run=True)
        size, num_files = fcleaner.clean_section()

    assert size == 0
    assert num_files == 0

    log_output = f'Path {basedir} missing, skipping section mytest_files1'
    assert log_output in caplog.text


def test_remove_files_empty_dir_mtime(file_structure_with_some_old_files_and_empty_dir, caplog):
    """Test remove files."""
    pub = FakePublisher()
    dir_base, sub_dir1, sub_dir2 = file_structure_with_some_old_files_and_empty_dir

    basedir = str(dir_base)
    subdir1 = sub_dir1.name
    subdir2 = sub_dir2.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'stat_time_method': 'st_mtime',
            'templates': f'{subdir1}/*.png,{subdir2}/*,{basedir}/*',
            'hours': '3'}

    with caplog.at_level(logging.DEBUG):
        fcleaner = FilesCleaner(pub, section, info, dry_run=False)
        size, num_files = fcleaner.clean_section()

    log_output1 = f'Removed {(sub_dir1 / "b.png")}'
    assert log_output1 in caplog.text
    assert not (sub_dir1 / "b.png").exists()
    assert num_files == 2
    assert not sub_dir2.exists()


def test_remove_files_empty_dir_atime(file_structure_with_some_old_files_and_empty_dir, caplog):
    """Test remove files."""
    pub = FakePublisher()
    dir_base, sub_dir1, sub_dir2 = file_structure_with_some_old_files_and_empty_dir

    basedir = str(dir_base)
    subdir1 = sub_dir1.name
    subdir2 = sub_dir2.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'stat_time_method': 'st_atime',
            'templates': f'{subdir1}/*.png,{subdir2}/*,{basedir}/*',
            'hours': '3'}

    with caplog.at_level(logging.DEBUG):
        fcleaner = FilesCleaner(pub, section, info, dry_run=False)
        size, num_files = fcleaner.clean_section()

    log_output1 = f'Removed {(sub_dir1 / "b.png")}'
    assert log_output1 in caplog.text
    assert not (sub_dir1 / "b.png").exists()
    assert num_files == 1
    assert sub_dir2.exists()
