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

"""Utility functions for cleaning files and directories."""


import datetime as dt
import logging
import os
from glob import glob

from posttroll.message import Message

LOGGER = logging.getLogger(__name__)


def get_config_items(args, conf):
    """Get items from ini configuration."""
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


class FilesCleaner():
    """Files cleaner class to accomodate cleaning of files acording to configured rules."""

    def __init__(self, publisher, section, info, dry_run=True):
        """Initialize the class."""
        self.pub = publisher
        self.section = section
        self.info = info
        self.dry_run = dry_run
        self.stat_time_method = self.info.get('stat_time_method', 'st_ctime')

    def clean_dir(self, ref_time, pathname_template, is_dry_run, **kwargs):
        """Clean directory of files given a path name and a time threshold.

        Only files older than a given time threshold are removed/cleaned.
        """
        filetime_checker_type = kwargs.get('filetime_checker_type')
        stat_time_checker = {'ctime': 'st_ctime',
                             'mtime': 'st_mtime'}.get(filetime_checker_type)
        recursive = kwargs.get("recursive")

        LOGGER.info("Cleaning under %s", pathname_template)

        if not recursive:
            filepaths = glob(pathname_template)
            return self.clean_files_and_dirs(filepaths, ref_time, stat_time_checker, is_dry_run)

        section_files = 0
        section_size = 0
        for pathname in glob(pathname_template):
            for dirpath, _dirnames, filenames in os.walk(pathname):
                files_in_dir = glob(os.path.join(dirpath, '*'))
                if len(files_in_dir) == 0:
                    if is_dry_run:
                        LOGGER.info("Would remove empty directory: %s", dirpath)
                    else:
                        try:
                            os.rmdir(dirpath)
                        except OSError:
                            LOGGER.debug("Was trying to remove empty directory, but failed. Should not have come here!")

                filepaths = [os.path.join(dirpath, fname) for fname in filenames]

                s_size, s_files = self.clean_files_and_dirs(filepaths, ref_time, stat_time_checker, is_dry_run)
                section_files += s_files
                section_size = section_size + s_size

        return (section_size, section_files)

    def clean_files_and_dirs(self, filepaths, ref_time, stat_time_checker, is_dry_run):
        """From a list of file paths and a reference time clean files and directories."""
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

            if dt.datetime.fromtimestamp(getattr(stat, stat_time_checker)) < ref_time:
                was_removed = False
                if not is_dry_run:
                    was_removed = self.remove_file(filepath)
                else:
                    LOGGER.info(f'Would remove {str(filepath)}')

                if was_removed:
                    section_files += 1
                    section_size += stat.st_size

        return (section_size, section_files)

    def clean_section(self, section, conf, is_dry_run=True):
        """Do the files cleaning given a list of directory paths and time thresholds.

        This calls the clean_dir function in this module.
        """
        section_files = 0
        section_size = 0
        info = dict(conf.items(section))
        recursive = info.get('recursive')
        if recursive and recursive == 'true':
            recursive = True
        else:
            recursive = False

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

        ref_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(**kws)
        for template in templates:
            pathname = os.path.join(base_dir, template)
            size, num_files = self.clean_dir(ref_time, pathname, is_dry_run,
                                             filetime_checker_type=info.get('filetime_checker_type', 'ctime'),
                                             recursive=recursive)
            section_files += num_files
            section_size += size

        return (section_size, section_files)

    def remove_file(self, filename):
        """Remove a file given its filename, and publish when removed.

        Removing an empty directory is not published.
        """
        try:
            if os.path.isdir(filename):
                if not os.listdir(filename):
                    os.rmdir(filename)
                else:
                    LOGGER.info("%s not empty.", filename)
            else:
                os.remove(filename)
                msg = Message("/deletion", "del", {"uri": filename})
                self.pub.send(str(msg))
                LOGGER.debug("Removed %s", filename)
        except FileNotFoundError:
            LOGGER.debug("File already removed.")
        except OSError as err:
            LOGGER.warning("Can't remove %s: %s", filename, str(err))
            return False
        return True
