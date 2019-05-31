#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

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
import netifaces
import bz2
import logging
import logging.handlers
import os
import shutil
import subprocess
import traceback
from urlparse import urlparse, urlunparse


LOGGER = logging.getLogger(__name__)

def get_local_ips():
    """Get the ips of the current machine."""
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def gen_dict_extract(var, key):
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if hasattr(v, 'items'):
                for result in gen_dict_extract(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(d, key):
                        yield result


def gen_dict_contains(var, key):
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield var
            if hasattr(v, 'items'):
                for result in gen_dict_contains(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_contains(d, key):
                        yield result


def translate_dict_value(var, key, callback):
    newvar = var.copy()
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                newvar[key] = callback(k, v)
            elif hasattr(v, 'items'):
                newvar[k] = translate_dict_value(v, key, callback)
            elif isinstance(v, list):
                newvar[k] = [translate_dict_value(d, key, callback) for d in v]
        return newvar
    else:
        return var


def translate_dict_item(var, key, callback):
    newvar = var.copy()
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                newvar = callback(var, k)
            elif hasattr(v, 'items'):
                newvar[k] = translate_dict_item(v, key, callback)
            elif isinstance(v, list):
                newvar[k] = [translate_dict_item(d, key, callback) for d in v]
        return newvar
    else:
        return var


def translate_dict(var, keys, callback):
    newvar = var.copy()
    if hasattr(var, 'items'):
        if set(var.keys()) & set(keys):
            newvar = callback(var)
        for k, v in newvar.items():
            if hasattr(v, 'items'):
                newvar[k] = translate_dict(v, keys, callback)
            elif isinstance(v, list):
                newvar[k] = [translate_dict(d, keys, callback) for d in v]
        return newvar
    else:
        return var


def check_output(*popenargs, **kwargs):
    """Copy from python 2.7, `subprocess.check_output`."""
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    LOGGER.debug("Calling " + str(popenargs))
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    del unused_err
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise RuntimeError(output)
    return output

def xrit(pathname, destination=None, cmd="./xRITDecompress"):
    """Unpacks xrit data."""
    opath, ofile = os.path.split(pathname)
    destination = destination or "/tmp/"
    dest_url = urlparse(destination)
    expected = os.path.join((destination or opath), ofile[:-2] + "__")
    if dest_url.scheme in ("", "file"):
        if ofile != os.path.basename(expected):
           check_output([cmd, pathname], cwd=(destination or opath))
    else:
        LOGGER.exception("Can not extract file " + pathname + " to " +
                         destination + ", destination has to be local.")
    LOGGER.info("Successfully extracted " + pathname + " to " + destination)
    return expected


# bzip
BLOCK_SIZE = 1024
def bzip(origin, destination=None):
    """Unzip files."""
    ofile = os.path.split(origin)[1]
    destfile = os.path.join(destination or "/tmp/", ofile[:-4])
    if os.path.exists(destfile):
        return destfile
    with open(destfile, "wb") as dest:
        try:
            orig = bz2.BZ2File(origin, "r")
            while True:
                block = orig.read(BLOCK_SIZE)

                if not block:
                    break
                dest.write(block)
            LOGGER.debug("Bunzipped " + origin + " to " + destfile)
        finally:
            orig.close()
    return destfile

def purge_dir(dir_base, destination_size):
    """ Purge older subdirectories

        Args:
            dir_base (string): base directory in which subdirectories are generated
            destination_size (int): maximum number of subdirectories

        Returns:
            deleted_count (int): number of deleted subdirectories
    """
    deleted_count = 0
    dest_list = os.listdir(dir_base)
    dest_listsubdir = []
    for dest_dir in dest_list:
        if os.path.isdir(os.path.join(dir_base, dest_dir)):
            dest_listsubdir.append(dest_dir)
    LOGGER.debug("Purging subdir: dir found " + str(len(dest_listsubdir)) + " max size " + str(destination_size))
    if len(dest_listsubdir) > destination_size:
        # Number of subdirs exceeding maximum size
        dest_listsubdir.sort()
        dest_todel = len(dest_listsubdir) - destination_size
        for x in range(0, dest_todel):
            dest_dirtodel = os.path.join(dir_base, dest_listsubdir[x])
            LOGGER.debug("Purging subdir - deleting " + str(dest_dirtodel))
            shutil.rmtree(dest_dirtodel)
            deleted_count += 1
    else:
        LOGGER.debug("Purging subdir: No dir to purge")
    return deleted_count

def generate_ref(dest_dir, filename, ref_file):
    """ Generate reference file

        Args:
            dest_dir: string
                referenced directory (where satellite data are stored/decompressed)
            filename: string
                reference file name
            ref_file: string
                reference file full path
    """
    dest_epistr = "[REF]\r\n"
    dest_epistr += "SourcePath = " + dest_dir + "\r\n"
    dest_epistr += "FileName = " + filename + "\r\n"
    dest_epifilefp = open(ref_file, "w")
    dest_epifilefp.write(dest_epistr)
    dest_epifilefp.close()
    return ref_file

def trigger_ref(dest_dir, ref_dir):
    """ Trigger reference file: only if epilogue segment is present in data

        Args:
            dest_dir (string): referenced directory - where satellite data are stored/decompressed
            ref_dir (string): directory where reference file is saved
    """
    dest_epifile = None
    for fname in os.listdir(dest_dir):
        if fname.find("-EPI")>0:
            dest_epifile = ref_dir + "/" + fname
            dest_file = fname
            break
    if dest_epifile is not None:
        if os.path.isfile(dest_epifile):
            #touch the ref file
            LOGGER.debug("Retrigger reference file: " + dest_epifile)
            os.remove(dest_epifile)
            ref_filename = ref_dir + "/" + dest_file
            generate_ref(dest_dir, dest_file, ref_filename)


def is_epilogue(filename):
    """ Check if filename is an epilogue segment
    """
    if filename.find("-EPI") > 0:
        return True
    return False
