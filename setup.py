#!/usr/bin/python
# Copyright (c) 2015-2023 Pytroll Developers
#

# Author(s):
#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

"""Setup file."""

from setuptools import setup
import versioneer


extras_require = {
    's3': [
        's3fs',
    ],
    'server': [
        'inotify',
        'paramiko',
        'scp',
        'watchdog',
    ],
    "remote_fs": ["pytroll-collectors>=0.13.0"],
}

all_extras = []
for extra_deps in extras_require.values():
    all_extras.extend(extra_deps)
extras_require['all'] = list(set(all_extras))

setup(name="trollmoves",
      version=versioneer.get_version(),
      description='Pytroll file utilities',
      author='Martin Raspaud',
      author_email='martin.raspaud@smhi.se',
      cmdclass=versioneer.get_cmdclass(),
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/trollmoves",
      scripts=['bin/move_it.py',
               'bin/move_it_server.py',
               'bin/move_it_client.py',
               'bin/move_it_mirror.py',
               'bin/remove_it.py',
               'bin/dispatcher.py',
               'bin/s3downloader.py',
               ],
      data_files=[],
      packages=['trollmoves'],
      zip_safe=False,
      install_requires=[
          'posttroll>=1.5.1',
          'trollsift',
          'netifaces',
          'pyinotify',
          'pyyaml',
          'pyzmq',
      ],
      tests_require=["pytest", "pytest-reraise", "pytest-bdd"],
      extras_require=extras_require,
      )
