#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Pytroll Developers


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

import pytest
from datetime import datetime, timedelta
import os
import logging

from trollmoves.filescleaner import clean_dir

DUMMY_CONTENT = "some dummy content"

OLD_FILES_TIME = datetime(2023, 5, 25, 12, 0)


class FakePublisher():
    """Fake Publisher class to be used for testing only."""

    def __enter__(self):
        """Enter method."""
        return self

    def __exit__(self, etype, value, traceback):
        """Exit."""
        pass

    def send(self, msg):
        """Fake a send method."""
        pass


@pytest.fixture(params=[OLD_FILES_TIME])
def fake_tree_of_some_files(request, tmp_path_factory) -> list[str]:
    """Create a directory tree of dummy (empty) files."""
    filepaths = []
    fn = tmp_path_factory.mktemp("data") / "dummy1.txt"
    fn.write_text(DUMMY_CONTENT)
    filepaths.append(fn)

    fn = tmp_path_factory.mktemp("data") / "dummy2.txt"
    fn.write_text(DUMMY_CONTENT)
    filepaths.append(fn)

    fn = tmp_path_factory.mktemp("data") / "another_subdir"
    fn.mkdir()
    fn = fn / "dummy3.txt"
    fn.write_text(DUMMY_CONTENT)

    # Alter the times of the last file and it's sub directory
    dtobj = request.param
    atime, mtime = (dtobj.timestamp(), dtobj.timestamp())
    os.utime(fn, times=(atime, mtime))
    os.utime(fn.parent, times=(atime, mtime))
    filepaths.append(fn)

    yield filepaths


def test_clean_dir_non_recursive(fake_tree_of_some_files, tmp_path, caplog):
    """Test cleaning a directory for files of a certain age."""
    list_of_files_to_clean = fake_tree_of_some_files
    ref_time = OLD_FILES_TIME + timedelta(hours=1)
    kws = {'filetime_checker_type': 'ctime'}
    pathname = str(tmp_path.parent / '*')

    with FakePublisher() as pub, caplog.at_level(logging.INFO):
        _ = clean_dir(pub, ref_time, pathname, False, **kws)

    assert f"Cleaning under {pathname}" in caplog.text

    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()
    assert list_of_files_to_clean[2].exists()


def test_clean_dir_recursive_mtime_real(fake_tree_of_some_files, tmp_path, caplog):
    """Test cleaning a directory tree for files of a certain age.

    Here we test using the modification time to determine when the file has been 'created'.
    """
    list_of_files_to_clean = fake_tree_of_some_files
    ref_time = OLD_FILES_TIME + timedelta(hours=1)
    kws = {'filetime_checker_type': 'mtime',
           'recursive': True}
    pathname = str(tmp_path.parent)

    with FakePublisher() as pub, caplog.at_level(logging.DEBUG):
        res = clean_dir(pub, ref_time, pathname, False, **kws)

    section_size, section_files = res

    assert section_size == 36
    assert section_files == 2

    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()

    removed_file = list_of_files_to_clean[2]
    assert f"Removed {removed_file}" in caplog.text
    assert not removed_file.exists()


def test_clean_dir_recursive_mtime_dryrun(fake_tree_of_some_files, tmp_path, caplog):
    """Test cleaning a directory tree for files of a certain age.

    Here we test using the modification time to determine when the file has been 'created'.
    """
    list_of_files_to_clean = fake_tree_of_some_files
    ref_time = OLD_FILES_TIME + timedelta(hours=1)
    kws = {'filetime_checker_type': 'mtime',
           'recursive': True}
    pathname = str(tmp_path.parent)

    with FakePublisher() as pub, caplog.at_level(logging.INFO):
        res = clean_dir(pub, ref_time, pathname, True, **kws)

    section_size, section_files = res

    assert section_size == 0
    assert section_files == 0
    assert list_of_files_to_clean[0].exists()
    assert list_of_files_to_clean[1].exists()

    removed_file = list_of_files_to_clean[2]
    assert f"Would remove {removed_file}" in caplog.text
    assert removed_file.exists()
