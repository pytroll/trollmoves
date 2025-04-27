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
def fake_config_file(tmp_path):
    """Create a fake configuration file."""
    file_path = tmp_path / TEST_BASIC_FILESCLEANER_CONFIG_FILENAME
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_BASIC_FILESCLEANER_CONFIG)

    yield file_path


@pytest.fixture
def fake_file_structure(tmp_path):
    """Create some fake files in a fake directory structure."""
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

    six_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=6)

    oldfile = (data_subdir1 / 'b.nc')
    os.utime(oldfile, (six_hours_ago.timestamp(), six_hours_ago.timestamp()))

    files = ["a.png", "b.png", "c.tif"]
    for fname in files:
        (data_subdir2 / fname).touch()

    eight_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=8)
    oldfile = (data_subdir2 / 'b.png')
    os.utime(oldfile, (eight_hours_ago.timestamp(), eight_hours_ago.timestamp()))

    return data_dir, data_subdir1, data_subdir2
