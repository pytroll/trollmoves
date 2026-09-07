"""The conftest module to set up pytest."""

import datetime as dt
import os

import pytest

TEST_BASIC_FILESCLEANER_CONFIG_FILENAME = "remove_it.cfg"
TEST_BASIC_FILESCLEANER_CONFIG = """
[DEFAULT]
mailhost=localhost
to=some_users@xxx.yy
subject=Cleanup Error on {hostname}


[mytest_files1]
base_dir=/san1
templates=polar_in/sentinel3/olci/lvl1/*/*,polar_in/sentinel3/olci/lvl1/*
hours=3
"""


def pytest_collection_modifyitems(items):
    """Modifiy test items in place to ensure test modules run in a given order."""
    MODULE_ORDER = ["test_fetcher", "test_logging", "test_s3downloader"]
    module_mapping = {item: item.module.__name__ for item in items}

    sorted_items = items.copy()
    # Iteratively move tests of each module to the end of the test queue
    for module in MODULE_ORDER:
        sorted_items = [it for it in sorted_items if module_mapping[it] != module] + [
            it for it in sorted_items if module_mapping[it] == module
        ]
    items[:] = sorted_items


@pytest.fixture
def minimal_config_file(tmp_path):
    """Create a fake configuration file."""
    file_path = tmp_path / TEST_BASIC_FILESCLEANER_CONFIG_FILENAME
    with open(file_path, "w") as fpt:
        fpt.write(TEST_BASIC_FILESCLEANER_CONFIG)

    return file_path


@pytest.fixture
def file_structure_with_some_old_files(tmp_path):
    """Create some empty files in a given directory structure."""
    data_dir = tmp_path / "mydata" / "geo_out"
    data_dir.mkdir(parents=True)

    # Create some empty files
    files = ["a.txt", "b.txt", "c.csv"]
    for fname in files:
        (data_dir / fname).touch()

    data_subdir1 = data_dir / "level2"
    data_subdir1.mkdir(parents=True)
    data_subdir2 = data_dir / "imagery"
    data_subdir2.mkdir(parents=True)

    files = ["a.nc", "b.nc", "c.h5"]
    for fname in files:
        (data_subdir1 / fname).touch()

    six_hours_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=6)

    oldfile = (data_subdir1 / "b.nc")
    os.utime(oldfile, (six_hours_ago.timestamp(), six_hours_ago.timestamp()))

    files = ["a.png", "b.png", "c.tif"]
    for fname in files:
        (data_subdir2 / fname).touch()

    eight_hours_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=8)
    oldfile = (data_subdir2 / "b.png")
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))

    return data_dir, data_subdir1, data_subdir2


@pytest.fixture
def file_structure_with_some_old_files_and_empty_dir(tmp_path):
    """Create some empty files in a directory structure, also with an empty subdir."""
    data_dir = tmp_path / "mydata" / "geo_out"
    data_dir.mkdir(parents=True)

    subdir1_old_png_file = data_dir / "imagery-1"
    subdir1_old_png_file.mkdir(parents=True)
    subdir2_old_tif_file = data_dir / "imagery-2"
    subdir2_old_tif_file.mkdir(parents=True)
    subdir3_empty = data_dir / "empty_dir"
    subdir3_empty.mkdir(parents=True)

    files = ["a.png", "b.png", "c.tif"]
    for fname in files:
        (subdir1_old_png_file / fname).touch()
        (subdir2_old_tif_file / fname).touch()

    eight_hours_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=8)
    oldfile = (subdir1_old_png_file / "b.png")
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))
    oldfile = (subdir2_old_tif_file / "c.tif")
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))
    # Force the directory to be old as well:
    os.utime(subdir3_empty, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))

    return data_dir, subdir1_old_png_file, subdir2_old_tif_file, subdir3_empty


@pytest.fixture
def flat_file_structure_with_no_files(tmp_path):
    """Create a flat directory structure with an old empty basedir."""
    base_dir = tmp_path / "mydata"
    base_dir.mkdir(parents=True)

    files = ["old1.png", "old2.tiff"]
    for fname in files:
        (base_dir / fname).touch()

    eight_hours_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=8)
    oldfile = (base_dir / "old1.png")
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))
    oldfile = (base_dir / "old2.tiff")
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))
    # Force the directory to be old as well:
    os.utime(base_dir, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))

    return base_dir
