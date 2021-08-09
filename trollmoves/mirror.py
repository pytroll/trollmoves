#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 Trollmoves developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""All you need for mirroring."""

import os

from urllib.parse import urlparse

from trollmoves.client import request_push
from trollmoves.server import RequestManager, Deleter

# FIXME: don't use globals
file_registry = {}


class MirrorRequestManager(RequestManager):
    """Handle requests as a mirror."""

    def __init__(self, port, attrs):
        """Set up this mirror request manager."""
        RequestManager.__init__(self, port, attrs)
        self._deleter = MirrorDeleter(attrs)

    def push(self, message):
        """Push the file."""
        new_uri = None
        for source_message in file_registry.get(message.data['uid'], []):
            request_push(source_message, publisher=None, **self._attrs)
            destination = urlparse(self._attrs['destination']).path
            new_uri = os.path.join(destination, message.data['uid'])
            if os.path.exists(new_uri):
                break
        if new_uri is None:
            raise KeyError('No source message found for %s',
                           str(message.data['uid']))
        message.data['uri'] = new_uri
        return RequestManager.push(self, message)


class MirrorDeleter(Deleter):
    """Deleter for mirroring."""

    def __init__(self, attrs=None):
        """Set up the deleter."""
        super().__init__(attrs)

    @staticmethod
    def delete(filename):
        """Delete the file."""
        Deleter.delete(filename)
        # Pop is atomic, so we don't need a lock.
        file_registry.pop(os.path.basename(filename), None)
