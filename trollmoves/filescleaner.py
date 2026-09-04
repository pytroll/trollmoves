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
        self.include_hidden = self.info.get("include_hidden", False)
        self.stat_time_method = self.info.get("stat_time_method", "st_ctime")


    def clean_dir(self, ref_time, pathname_template, **kwargs):
        """Clean directory of files given a path name and a time threshold.

        Only files older than a given time threshold are removed/cleaned.
        """
        LOGGER.info("Cleaning under %s", pathname_template)

        if not self.recursive:
            filepaths = glob(pathname_template, include_hidden=self.include_hidden)
            return self.clean_files_and_dirs(filepaths, ref_time)

        section_files = 0
        section_size = 0
        removed = []

        base_template = str(Path(pathname_template).parent)
        file_pattern = Path(pathname_template).name

        for base_dir in glob(base_template, include_hidden=self.include_hidden):
            if not os.path.isdir(base_dir):
                continue

            s_size, s_files, removed_files = self._clean_recursive_base_dir(base_dir, file_pattern, ref_time)

            section_files += s_files
            section_size += s_size
            removed.extend(removed_files)

        return section_size, section_files, removed


    def _clean_recursive_base_dir(self, base_dir, file_pattern, ref_time):
        """Clean matching files recursively below one base directory.

        Empty subdirectories may be removed, but the base directory itself is
        never removed.
        """
        section_files = 0
        section_size = 0
        removed = []

        base_dir = Path(base_dir)

        for dirpath, _dirnames, _filenames in os.walk(base_dir, topdown=False, followlinks=True):
            dirpath = Path(dirpath)
            files_in_dir = glob(str(dirpath / file_pattern), include_hidden=self.include_hidden)

            s_size, s_files, removed_files = self.clean_files_and_dirs(files_in_dir, ref_time)

            section_files += s_files
            section_size += s_size
            removed.extend(removed_files)

            if dirpath != base_dir and self._is_empty_dir(dirpath):
                self._remove_empty_directory(dirpath)

        return section_size, section_files, removed


    @staticmethod
    def _is_empty_dir(path):
        """Return True if path is an empty directory.

        Using iterdir instead of glob will make sure dot files are also counted.
        """
        try:
            return path.is_dir() and not any(path.iterdir())
        except OSError:
            return False


    def clean_files_and_dirs(self, filepaths, ref_time):
        """Clean files and directories defined by a list of file paths and a reference time."""
        section_files = 0
        section_size = 0
        removed = []
        for filepath in filepaths:
            if not os.path.exists(filepath):
                continue
            try:
                stat = os.stat(filepath)
            except OSError:
                LOGGER.warning("Couldn't stat path=%s", str(filepath))
                continue
            if filepath.endswith(".keep"):
                continue

            if dt.datetime.fromtimestamp(getattr(stat, self.stat_time_method), tz=dt.timezone.utc) < ref_time:
                if not self.dry_run:
                    removed_file = self.remove_file(filepath)
                    if removed_file:
                        section_files += 1
                        section_size += stat.st_size
                        removed.append(removed_file)
                else:
                    removed.append(filepath)
                    LOGGER.info(f"Would remove {str(filepath)}")

        return (section_size, section_files, removed)


    def clean_section(self):
        """Do the files cleaning given a list of directory paths and time thresholds.

        This calls the clean_dir function in this module.
        """
        section_files = 0
        section_size = 0
        base_dir = self.info.get("base_dir", "")
        if not os.path.exists(base_dir):
            LOGGER.warning("Path %s missing, skipping section %s", base_dir, self.section)
            return (section_size, section_files, [])
        LOGGER.info("Cleaning in %s", base_dir)

        templates = (item.strip() for item in self.info["templates"].split(","))

        ref_time = self._get_reference_time()
        removed = []

        for template in templates:
            pathname = os.path.join(base_dir, template)
            size, num_files, removed_files = self.clean_dir(ref_time, pathname)
            section_files += num_files
            section_size += size
            removed.extend(removed_files)


        return (section_size, section_files, removed)

    def remove_file(self, filename):
        """Remove a file given its filename, and publish when removed.

        Removal of an empty directory is not published.

        The return value of this function is the removed filename, or None if nothing was removed.
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
            return
        except OSError as err:
            LOGGER.warning("Can't remove %s: %s", filename, str(err))
            return
        return filename

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
