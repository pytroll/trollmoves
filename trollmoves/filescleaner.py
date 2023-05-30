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

"""Utility functions for cleaning files and directories."""

from datetime import datetime, timedelta
from glob import glob
import os
import logging
from posttroll.message import Message


def remove_file(filename, pub):
    """Remove a file given its filename, and publish when removed.

    Removing an empty directory is not published.
    """
    LOGGER = logging.getLogger(__name__)
    try:
        if os.path.isdir(filename):
            if not os.listdir(filename):
                os.rmdir(filename)
            else:
                LOGGER.info("%s not empty.", filename)
        else:
            os.remove(filename)
            msg = Message("deletion", "del", {"uri": filename})
            pub.send(str(msg))
            LOGGER.debug("Removed %s", filename)
    except OSError as err:
        LOGGER.warning("Can't remove %s: %s", filename,
                       str(err))
        return False
    return True


def clean_dir(pub, ref_time, pathname, is_dry_run, **kwargs):
    """Clean directory of files given a path name and a time threshold.

    Only files older than a given time threshold are removed/cleaned.
    """
    LOGGER = logging.getLogger(__name__)

    filetime_checker_type = kwargs.get('filetime_checker_type')
    stat_time_checker = {'ctime': 'st_ctime',
                         'mtime': 'st_mtime'}.get(filetime_checker_type)
    recursive = kwargs.get("recursive")

    LOGGER.info("Cleaning %s", pathname)

    if not recursive:
        filepaths = glob(pathname)
        return clean_files_and_dirs(pub, filepaths, ref_time, stat_time_checker, is_dry_run)

    section_files = 0
    section_size = 0
    for dirpath, _dirnames, filenames in os.walk(pathname):
        filepaths = [os.path.join(dirpath, fname) for fname in filenames]

        s_size, s_files = clean_files_and_dirs(pub, filepaths, ref_time, stat_time_checker, is_dry_run)
        section_files += s_files
        section_size = section_size + s_size

    return (section_size, section_files)


def clean_files_and_dirs(pub, filepaths, ref_time, stat_time_checker, is_dry_run):
    """From a list of file paths and a reference time clean files and directories."""
    LOGGER = logging.getLogger(__name__)

    section_files = 0
    section_size = 0
    for filepath in filepaths:
        if not os.path.exists(filepath):
            continue
        try:
            stat = os.lstat(filepath)
        except OSError:
            LOGGER.warning("Couldn't lstat path=%s", str(filepath))
            continue

        if datetime.fromtimestamp(getattr(stat, stat_time_checker)) < ref_time:
            was_removed = False
            if not is_dry_run:
                was_removed = remove_file(filepath, pub)
            else:
                LOGGER.info("Would remove %s" % filepath)

            if was_removed:
                section_files += 1
                section_size += stat.st_size

    return (section_size, section_files)


def clean_section(pub, section, conf, is_dry_run=True):
    """Do the files cleaning given a list of directory paths and time thresholds.

    This calls the clean_dir function in this module.
    """
    LOGGER = logging.getLogger(__name__)
    section_files = 0
    section_size = 0
    info = dict(conf.items(section))

    base_dir = info.get("base_dir", "")
    if not os.path.exists(base_dir):
        LOGGER.warning("Path %s missing, skipping section %s", base_dir, section)
        return (section_size, section_files)
    LOGGER.info("Cleaning in %s", base_dir)
    templates = (item.strip() for item in info["templates"].split(","))
    kws = {}
    for key in ["days", "hours", "minutes", "seconds"]:
        try:
            kws[key] = int(info[key])
        except KeyError:
            pass

    ref_time = datetime.utcnow() - timedelta(**kws)
    for template in templates:
        pathname = os.path.join(base_dir, template)
        size, num_files = clean_dir(pub, ref_time, pathname, is_dry_run,
                                    filetime_checker_type=info.get('filetime_checker_type', 'ctime'))
        section_files += num_files
        section_size += size

    return (section_size, section_files)


def get_config_items(args, conf):
    """Get items from ini configuration."""
    LOGGER = logging.getLogger(__name__)
    config_items = []

    if args.config_item:
        for config_item in args.config_item:
            if config_item not in conf.sections():
                LOGGER.error("No section named %s in %s",
                             config_item, args.configuration_file)
            else:
                config_items.append(config_item)
    else:
        config_items = conf.sections()

    return config_items
