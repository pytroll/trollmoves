#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019
#
# Author(s):
#
#   Trygve Aspenes <trygveas@met.no>
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
"""Test the ssh server."""

import os
import shutil
import signal
import time
from datetime import datetime
from queue import Queue
from tempfile import NamedTemporaryFile, gettempdir, mkdtemp
from threading import get_ident
from unittest.mock import Mock, patch
import unittest
from posttroll.message import Message

import yaml
#import mockssh
#import pytest

from trollmoves.server import RequestManager

import trollmoves

class TestSSHMovers(unittest.TestCase):

    def setUp(self):
        self.origin_dir = mkdtemp()
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            self.origin = the_file.name

        self.dest_dir = mkdtemp()

        self.hostname = 'localhost'
        self.login = 'user'
        self.port = 22

        #self.server = mockssh.Server({self.login: '/home/polarsat/anaconda3/envs/conda-pytroll/lib/python3.7/site-packages/mockssh/sample-user-key'})
        #print(self.server)
        #print(self.server.client(self.login))

    def tearDown(self):
        try:
            shutil.rmtree(self.origin_dir, ignore_errors=True)
            shutil.rmtree(self.dest_dir, ignore_errors=True)
        except OSError:
            pass

    def test_scp(self):
        """Check getting destination urls."""
        from trollmoves.movers import ScpMover
        with NamedTemporaryFile('w', delete=False, dir=self.origin_dir) as the_file:
            origin = the_file.name
        print(origin)
        destination = 'scp://' + self.hostname + ':' + str(self.port) + '/' + self.dest_dir
        _attrs = {}

        with patch('trollmoves.movers.ScpMover') as sm:
            print(sm)
            sm_instanse = sm.return_value
            print(sm_instanse)
            sm_instanse.run.return_value = {u'dataObjectID': u'test1'}
            #sm.return_value.get_connection.return_value = 'testing'

            scp_mover = trollmoves.movers.ScpMover(origin, destination, attrs=_attrs)
                #self.hostname, self.port)
            print(scp_mover)
            sm.assert_called_once_with(origin, destination, attrs=_attrs) #self.hostname, self.port)
            #self.assertEqual(scp_mover, 'testing')
            #self.assert_has_calls(mock.Call('test1', None))
        #connection = scp_mover.open_connection()
        #print(connection)
        #is_connected = scp_mover.is_connected(connection)
        #print(is_connected)
        #scp_mover.copy()

        # Need to kill the timer before exiting
        #print(scp_mover.active_connections)
        #scp_mover.active_connections[(self.hostname, self.port, None)][1].cancel()
            
        #scp_mover.close_connection(connection)

        
        #from trollmoves.server import RequestManager
        #with NamedTemporaryFile('w', delete=False) as the_file:
        #    fname = the_file.name
        #print(fname)
        #_port = 9876
        #_attrs = {}
        #_attrs['station'] = 'teststation'
        #_attrs['origin'] = ('/tmp/{sensor:s}_{start_time:%Y%m%d_%H%M%S}_{platform_name:s}_{orbit_number:s}_ear_o_coa_'
        #                    '{ears_station:s}_ovw.l2_bufr')
        #req_m = RequestManager(port=_port, attrs=_attrs)
        #
        #orig = ('pytroll://TEST push user@host 2019-10-21T08:13:41.143878 v1.01 application/json '
        #        '{"sensor": "ascat", "start_time": "2019-10-18T03:47:00", "platform_name": "metopb", '
        #        '"orbit_number": "36746", "ears_station": "sva", "uri": '
        #        '"/tmp/ascat_20191018_034700_metopb_36746_ear_o_coa_sva_ovw.l2_bufr", '
        #        '"uid": "ascat_20191018_034700_metopb_36746_ear_o_coa_sva_ovw.l2_bufr", '
        #        '"request_address": "10.10.10.10:9901", "destination": "scp://host/tmp"}')

        #message = Message(rawstr=orig)
        #push_msg = req_m.push(message)
        #print(push_msg)


if __name__ == '__main__':
    unittest.main()
