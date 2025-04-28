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

LOGGER = logging.getLogger("__name__")


class FilesCleaner():
    """Files cleaner class to accomodate cleaning of files acording to configured rules."""

    def __init__(self, publisher, section, info, dry_run=True):
        """Initialize the class."""
        self.pub = publisher
        self.section = section
        self.info = info
        self.dry_run = dry_run
        self.st_time = self.info.get('st_time', 'st_ctime')

    def clean_dir(self, ref_time, pathname):
        """Clean up a directory."""
        section_files = 0
        section_size = 0
        LOGGER.info("Cleaning %s", pathname)
        flist = glob(pathname)

        for filename in flist:
            if not os.path.exists(filename):
                continue
            try:
                stat = os.lstat(filename)
            except OSError:
                LOGGER.warning("Couldn't lstat path=%s", str(filename))
                continue

            if dt.datetime.fromtimestamp(stat.__getattribute__(self.st_time), dt.timezone.utc) < ref_time:
                was_removed = False
                if not self.dry_run:
                    was_removed = self.remove_file(filename)
                else:
                    LOGGER.debug("Would remove %s", filename)
                if was_removed:
                    section_files += 1
                    section_size += stat.st_size

        return (section_size, section_files)

    def clean_section(self):
        """Clean up according to the configuration section."""
        section_files = 0
        section_size = 0
        base_dir = self.info.get("base_dir", "")
        if not os.path.exists(base_dir):
            LOGGER.warning("Path %s missing, skipping section %s",
                           base_dir, self.section)
            return (section_size, section_files)
        LOGGER.info("Cleaning in %s", base_dir)
        templates = (item.strip() for item in self.info["templates"].split(","))
        kws = {}
        for key in ["days", "hours", "minutes", "seconds"]:
            try:
                kws[key] = int(self.info[key])
            except KeyError:
                pass
        ref_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(**kws)

        for template in templates:
            pathname = os.path.join(base_dir, template)
            size, num_files = self.clean_dir(ref_time, pathname)
            section_files += num_files
            section_size += size

        return (section_size, section_files)

    def remove_file(self, filename):
        """Remove one file or directory."""
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
            LOGGER.warning("Can't remove %s: %s", filename,
                           str(err))
            return False
        return True
