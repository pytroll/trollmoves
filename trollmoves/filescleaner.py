"""Utility functions for cleaning files and directories."""


import datetime as dt
import logging
import os
from glob import glob
from pathlib import Path

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
        self.recursive = self.info.get("recursive", False)
        self.stat_time_method = self.info.get("stat_time_method", "st_ctime")

    def clean_dir(self, ref_time, pathname_template, **kwargs):
        """Clean directory of files given a path name and a time threshold.

        Only files older than a given time threshold are removed/cleaned.
        """
        LOGGER.info("Cleaning under %s", pathname_template)

        if not self.recursive:
            filepaths = glob(pathname_template)
            return self.clean_files_and_dirs(filepaths, ref_time)

        section_files = 0
        section_size = 0
        for pathname in glob(pathname_template):
            for dirpath, _dirnames, _ in os.walk(Path(pathname).parent):
                files_in_dir = glob(os.path.join(dirpath, Path(pathname_template).name))

                if len(files_in_dir) == 0:
                    self._remove_empty_directory(dirpath)

                s_size, s_files = self.clean_files_and_dirs(files_in_dir, ref_time)
                section_files += s_files
                section_size += s_size

        return (section_size, section_files)

    def clean_files_and_dirs(self, filepaths, ref_time):
        """Clean files and directories defined by a list of file paths and a reference time."""
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

            if dt.datetime.fromtimestamp(getattr(stat, self.stat_time_method), tz=dt.timezone.utc) < ref_time:
                if not self.dry_run:
                    _ = self.remove_file(filepath)
                    section_files += 1
                    section_size += stat.st_size
                else:
                    LOGGER.info(f"Would remove {str(filepath)}")

        return (section_size, section_files)

    def clean_section(self):
        """Do the files cleaning given a list of directory paths and time thresholds.

        This calls the clean_dir function in this module.
        """
        section_files = 0
        section_size = 0
        base_dir = self.info.get("base_dir", "")
        if not os.path.exists(base_dir):
            LOGGER.warning("Path %s missing, skipping section %s", base_dir, self.section)
            return (section_size, section_files)
        LOGGER.info("Cleaning in %s", base_dir)

        templates = (item.strip() for item in self.info["templates"].split(","))

        ref_time = self._get_reference_time()

        for template in templates:
            pathname = os.path.join(base_dir, template)
            size, num_files = self.clean_dir(ref_time, pathname)
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

    def _remove_empty_directory(self, dirpath):
        """Remove empty directory."""
        if self.dry_run:
            LOGGER.info("Would remove empty directory: %s", dirpath)
        else:
            try:
                os.rmdir(dirpath)
            except OSError:
                LOGGER.warning("Was trying to remove empty directory, but failed. Should not have come here!")

    def _get_reference_time(self):
        """Get the reference time from the configuration parameters."""
        kws = {}
        for key in ["days", "hours", "minutes", "seconds"]:
            try:
                kws[key] = int(self.info[key])
            except KeyError:
                pass

        return dt.datetime.now(dt.timezone.utc) - dt.timedelta(**kws)
