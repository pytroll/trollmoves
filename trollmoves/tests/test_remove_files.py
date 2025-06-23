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

"""Testing the functions for cleaning files in and below a directory structure."""

import datetime as dt
import logging
import os

import pytest

from trollmoves.filescleaner import FilesCleaner

DUMMY_CONTENT = "some dummy content"

OLD_FILES_TIME = dt.datetime(2023, 5, 25, 12, 0, tzinfo=dt.timezone.utc)


class FakePublisher():
    """Implements a Fake Publisher for testing."""

    def __init__(self):
        """Initialize the class."""
        pass

    def __enter__(self):
        """Enter method."""
        return self

    def __exit__(self, etype, value, traceback):
        """Exit."""
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


@pytest.fixture(params=[OLD_FILES_TIME])
def dummy_tree_of_some_files(request, tmp_path_factory) -> list[str]:
    """Create a directory tree of dummy (empty) files."""
    filepaths = []
    basedir = tmp_path_factory.mktemp("data")
    fn = basedir / "dummy1.txt"
    fn.write_text(DUMMY_CONTENT)
    filepaths.append(fn)

    fn = basedir / "dummy2.txt"
    fn.write_text(DUMMY_CONTENT)
    filepaths.append(fn)

    fn = basedir / "another_subdir"
    fn.mkdir()
    fn = fn / "dummy3.txt"
    fn.write_text(DUMMY_CONTENT)

    # Alter the times of the last file and it's sub directory
    dtobj = request.param
    atime, mtime = (dtobj.timestamp(), dtobj.timestamp())
    os.utime(fn, times=(atime, mtime))
    os.utime(fn.parent, times=(atime, mtime))
    filepaths.append(fn)

    # Make some additional content below the last sub-directory:
    fn = basedir / "another_subdir" / "subsubdir1"
    fn.mkdir()
    fn = fn / "dummy4.txt"
    fn.write_text(DUMMY_CONTENT)
    os.utime(fn, times=(atime, mtime))
    os.utime(fn.parent, times=(atime, mtime))
    filepaths.append(fn)
    fn = basedir / "another_subdir" / "subsubdir2"
    fn.mkdir()
    fn = fn / "dummy5.dat"
    fn.write_text(DUMMY_CONTENT)
    os.utime(fn, times=(atime, mtime))
    os.utime(fn.parent, times=(atime, mtime))
    filepaths.append(fn)

    yield filepaths


def test_clean_dir_non_recursive(dummy_tree_of_some_files, tmp_path, caplog):
    """Test cleaning directory for files of a certain age."""
    pub = FakePublisher()

    list_of_files_to_clean = dummy_tree_of_some_files

    basedir = list_of_files_to_clean[0].parent
    subdir1 = list_of_files_to_clean[1].parent.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'stat_time_method': 'st_ctime',
            'recursive': False,
            'templates': f'{subdir1}/*.txt',
            'hours': '1'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=False)

    with FakePublisher() as pub, caplog.at_level(logging.INFO):
        _ = fcleaner.clean_section()

    assert f"Cleaning in {basedir}" in caplog.text

    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()
    assert list_of_files_to_clean[2].exists()


def test_clean_dir_recursive_mtime_real(dummy_tree_of_some_files, caplog):
    """Test cleaning a directory tree for files which were created before the given time.

    Here we test using the modification time (st_mtime) to determine when the file has been 'created'.
    """
    pub = FakePublisher()

    list_of_files_to_clean = dummy_tree_of_some_files

    basedir = list_of_files_to_clean[0].parent
    subdir1 = list_of_files_to_clean[2].parent.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'stat_time_method': 'st_mtime',
            'recursive': True,
            'templates': f'*.txt,{subdir1}/*.txt',
            'hours': '1'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=False)

    with FakePublisher() as pub, caplog.at_level(logging.DEBUG):
        res = fcleaner.clean_section()

    section_size, section_files = res

    assert section_size == 36
    assert section_files == 2

    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()
    assert not list_of_files_to_clean[2].exists()
    assert not list_of_files_to_clean[3].exists()
    assert list_of_files_to_clean[4].exists()

    removed_file = list_of_files_to_clean[2]
    assert f"Removed {removed_file}" in caplog.text
    assert not removed_file.exists()


def test_clean_dir_recursive_mtime_dryrun(dummy_tree_of_some_files, tmp_path, caplog):
    """Test cleaning a directory tree for files of a certain age.

    Here we test using the modification time to determine when the file has been 'created'.
    """
    pub = FakePublisher()

    list_of_files_to_clean = dummy_tree_of_some_files

    basedir = list_of_files_to_clean[0].parent
    subdir1 = list_of_files_to_clean[1].parent.name
    subdir2 = list_of_files_to_clean[2].parent.name

    section = 'mytest_files1'
    info = {'mailhost': 'localhost',
            'to': 'some_users@xxx.yy',
            'subject': 'Cleanup Error on {hostname}',
            'base_dir': f'{basedir}',
            'templates': f'{subdir1}/*.txt,{subdir2}/*.txt',
            'stat_time_method': 'st_mtime',
            'recursive': True,
            'hours': '1'}

    fcleaner = FilesCleaner(pub, section, info, dry_run=True)

    with FakePublisher() as pub, caplog.at_level(logging.INFO):
        res = fcleaner.clean_section()

    section_size, section_files = res

    assert section_size == 0
    assert section_files == 0
    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()

    removed_file = list_of_files_to_clean[2]
    assert f"Would remove {removed_file}" in caplog.text
    assert removed_file.exists()
