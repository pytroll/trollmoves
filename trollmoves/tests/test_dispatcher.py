#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019
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
"""Test the dispatcher."""

import os
import shutil
import signal
import time
from datetime import datetime
from queue import Queue
from tempfile import NamedTemporaryFile, gettempdir
from threading import get_ident
from unittest.mock import Mock, patch, call

import yaml
import pytest

from trollmoves.dispatcher import (
    Dispatcher, YAMLConfig, check_conditions, dispatch
)

test_yaml1 = """
target1:
  host: ftp://ftp.target1.com
  connection_parameters:
    connection_uptime: 20
  filepattern: '{platform_name}_{start_time:%Y%m%d%H%M}.{format}'
  directory: /input_data/{sensor}
  dispatch_configs:
    - topics:
        - /level2/viirs
        - /level2/avhrr
      conditions:
        # key matches metadata items or provides default
        - product: [green_snow, true_color]
          sensor: viirs
        - product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15
    - topics:
        - /level3/cloudtype
      directory: /input/cloud_products
      conditions:
        - area: omerc_bb
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'
"""

test_yaml2 = test_yaml1 + """
target2:
  host: ssh://server.target2.com
  connection_parameters:
    ssh_key_filename: ~/.ssh/rsa_id.pub
  filepattern: 'sat_{start_time:%Y%m%d%H%M}_{platform_name}.{format}'
  directory: /satellite/{sensor}
  dispatch_configs:
    - topics:
        - /level2/viirs
        - /level2/avhrr
      conditions:
        # key matches metadata items or provides default
        - product: [green_snow, true_color]
          sensor: viirs
        - product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15
    - topics:
        - /level3/cloudtype
      directory: /input/cloud_products
      conditions:
        - area: omerc_bb
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'
"""

test_yaml_ssh_scp = test_yaml2 + """
target3:
  host: scp://user@server.target3.com
  connection_parameters:
    ssh_key_filename: ~/.ssh/rsa_id.pub
  filepattern: 'sat_{start_time:%Y%m%d%H%M}_{platform_name}.{format}'
  directory: /satellite/{sensor}
  dispatch_configs:
    - topics:
        - /level2/viirs

target4:
  host: scp://user@server.target4.com
  connection_parameters:
    ssh_key_filename: ~/.ssh/rsa_id.pub
  directory: /satellite/{sensor}
  dispatch_configs:
    - topics:
        - /level2/viirs
"""

test_yaml_no_default_directory = """
target1:
  host: ftp://ftp.target1.com
  connection_parameters:
    connection_uptime: 20
  filepattern: '{platform_name}_{start_time:%Y%m%d%H%M}.{format}'
  dispatch_configs:
    - topics:
        - /level2/viirs
        - /level2/avhrr
      conditions:
        # key matches metadata items or provides default
        - product: [green_snow, true_color]
          sensor: viirs
        - product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15
      directory: /input_data/{sensor}
    - topics:
        - /level3/cloudtype
      directory: /input/cloud_products
      conditions:
        - area: omerc_bb
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'
      directory: /input_data/{sensor}
"""


test_yaml_aliases_simple = """
target1:
  host: ftp://ftp.target1.com
  connection_parameters:
    connection_uptime: 20
  filepattern: '{platform_name}_{product}_{start_time:%Y%m%d%H%M}.{format}'
  directory: /input_data/{sensor}
  aliases:
    product:
      green_snow: gs
    variant:
      DR: direct_readout
  dispatch_configs:
    - topics:
        - /level2/viirs
"""

test_yaml_aliases_multiple = """
target1:
  host: ftp://ftp.target1.com
  connection_parameters:
    connection_uptime: 20
  filepattern: '{platform_name}_{product}_{start_time:%Y%m%d%H%M}.{format}'
  directory: /input_data/{product_dir}
  aliases:
    product:
      - _alias_name: product_dir
        green_snow: alternate_dir_for_green_snow
      - green_snow: gs
    variant:
      DR: direct_readout
  dispatch_configs:
    - topics:
        - /level2/viirs
"""


test_local = """
target3:
  host: ""
  filepattern: '{platform_name}_{start_time:%Y%m%d%H%M}.{format}'
  directory: """ + os.path.join(gettempdir(), 'dptest') + """
  subscribe_addresses:
    - tcp://127.0.0.1:40000
  nameserver: 127.0.0.1
  subscribe_services:
    - service_name_1
    - service_name_2

  dispatch_configs:
    - topics:
        - /level2/viirs
        - /level2/avhrr
      conditions:
        # key matches metadata items or provides default
        - product: [green_snow, true_color]
          sensor: viirs
        - product: [green_snow, overview]
          sensor: avhrr
          # special section "except" for negating
          except:
            platform_name: NOAA-15
    - topics:
        - /level3/cloudtype
      directory: /input/cloud_products
      conditions:
        - area: omerc_bb
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'
    """


test_yaml_pub = test_yaml2 + """
target3:
  host: scp://user@server.target2.com
  filepattern: 'sat_{start_time:%Y%m%d%H%M}_{platform_name}.{format}'
  directory: /satellite/{sensor}
  publish_topic: "/topic/{platform_name}"
  dispatch_configs:
    - topics:
        - /level2/viirs
"""


@pytest.fixture
def check_conditions_string_config():
    config_item = yaml.safe_load("""
        topics:
          - /level2/viirs
          - /level2/avhrr
        conditions:
          # key matches metadata items or provides default
          - product: [green_snow, true_color]
            sensor: viirs
          - product: [green_snow, overview]
            sensor: avhrr
          # special section "except" for negating
            except:
              platform_name: NOAA-15
              product: [green_snow, cloudtop]""")
    yield config_item


@pytest.fixture
def get_destinations_message():
    msg = Mock()
    msg.type = 'file'
    msg.subject = '/level2/viirs'
    msg.data = {'sensor': 'viirs', 'product': 'green_snow', 'platform_name': 'NOAA-20',
                'start_time': datetime(2019, 9, 19, 9, 19), 'format': 'tif',
                'uid': '201909190919_NOAA-20_viirs.tif'}
    yield msg


@pytest.fixture
def create_dest_url_message(get_destinations_message):
    get_destinations_message.data['uri'] = '/data/viirs/201909190919_NOAA-20_viirs.tif'
    get_destinations_message.data['uid'] = '67e91f4a778adc59e5f1a4f0475e388b'

    yield get_destinations_message


@pytest.fixture
def publisher_config_file_name():
    with NamedTemporaryFile('w', delete=False) as config_file:
        config_file_name = config_file.name
        config_file.write(test_yaml_pub)
        config_file.flush()
        config_file.close()
    yield config_file_name


def test_config_reading():
    """Test reading the config."""
    with NamedTemporaryFile('w', delete=False) as config_file:
        fname = config_file.name
        try:
            with patch.object(YAMLConfig, 'read_config') as rc:
                assert rc.call_count == 0
                yconf = YAMLConfig(fname)
                time.sleep(.1)
                assert rc.call_count == 1
                config_file.write(test_yaml1)
                config_file.flush()
                config_file.close()
                time.sleep(.1)
                assert rc.call_count == 2
                signal.pthread_kill(get_ident(), signal.SIGUSR1)
                time.sleep(.1)
                assert rc.call_count == 3
                os.remove(fname)
                signal.pthread_kill(get_ident(), signal.SIGUSR1)
                time.sleep(.1)
                yconf.close()
        finally:
            yconf.close()
            try:
                os.remove(fname)
            except FileNotFoundError:
                pass

    expected = yaml.safe_load(test_yaml1)
    with NamedTemporaryFile('w', delete=False) as config_file:
        fname = config_file.name
        try:
            yconf = YAMLConfig(fname)
            time.sleep(.1)
            config_file.write(test_yaml1)
            config_file.flush()
            config_file.close()
            time.sleep(.1)
            assert yconf.config == expected
        finally:
            yconf.close()
            try:
                os.remove(fname)
            except FileNotFoundError:
                pass


def test_check_conditions_strings(check_conditions_string_config):
    """Check condition checking for string items."""
    msg = Mock()
    msg.data = {'sensor': 'viirs', 'product': 'green_snow', 'platform_name': 'NOAA-20'}
    assert check_conditions(msg, check_conditions_string_config) is True
    msg.data = {'sensor': 'viirs', 'product': 'overview', 'platform_name': 'NOAA-20'}
    assert check_conditions(msg, check_conditions_string_config) is False
    msg.data = {'sensor': 'avhrr', 'product': 'overview', 'platform_name': 'NOAA-19'}
    assert check_conditions(msg, check_conditions_string_config) is True


def test_check_conditions_strings_negation(check_conditions_string_config):
    """Check condition checking for string items and negation."""
    msg = Mock()
    msg.data = {'sensor': 'avhrr', 'product': 'green_snow', 'platform_name': 'NOAA-15'}
    assert check_conditions(msg, check_conditions_string_config) is False
    msg.data = {'sensor': 'avhrr', 'product': 'overview', 'platform_name': 'NOAA-15'}
    assert check_conditions(msg, check_conditions_string_config) is True


def test_check_conditions_strings_missing_keys(check_conditions_string_config):
    """Check condition checking for string items and missing message keys."""
    msg = Mock()
    msg.data = {'product': 'overview', 'platform_name': 'NOAA-19'}
    assert check_conditions(msg, check_conditions_string_config) is False
    msg.data = {'sensor': 'avhrr', 'product': 'green_snow'}
    assert check_conditions(msg, check_conditions_string_config) is False


def test_check_conditions_numbers():
    """Check condition checking for numerical items."""
    config_item = yaml.safe_load("""
      topics:
        - /level3/cloudtype
      directory: /input/cloud_products
      conditions:
        - area: omerc_bb
          # ' 122'.strip().isdigit() -> True
          daylight: '<30'
          coverage: '>50'""")
    msg = Mock()
    msg.data = {'daylight': 18.3, 'area': 'omerc_bb', 'coverage': '77.1'}
    assert check_conditions(msg, config_item) is True
    msg.data = {'daylight': 48.3, 'area': 'omerc_bb', 'coverage': '77.1'}
    assert check_conditions(msg, config_item) is False
    msg.data = {'daylight': 18.3, 'area': 'omerc_bb', 'coverage': '27.1'}
    assert check_conditions(msg, config_item) is False


def _get_dispatcher(config):
    with patch('trollmoves.dispatcher.DispatchConfig'):
        with NamedTemporaryFile('w') as fid:
            fname = fid.name
            dispatcher = Dispatcher(fname)
            dispatcher.config = yaml.safe_load(config)
    return dispatcher


def test_get_destinations_single_destination(get_destinations_message):
    """Check getting destination urls for single destination."""
    dispatcher = _get_dispatcher(test_yaml1)

    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}

    res = dispatcher.get_destinations(get_destinations_message)
    assert len(res) == 1
    url, attrs, client = res[0]
    assert url == expected_url
    assert attrs == expected_attrs
    assert client == "target1"


def test_get_destinations_two_destinations(get_destinations_message):
    """Check getting destination urls for two destinations."""
    dispatcher = _get_dispatcher(test_yaml2)

    res = dispatcher.get_destinations(get_destinations_message)

    assert len(res) == 2


def _listify(itm):
    if not isinstance(itm, list):
        itm = [itm]
    return itm


def _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client):
    expected_url = _listify(expected_url)
    expected_attrs = _listify(expected_attrs)
    expected_client = _listify(expected_client)

    assert len(res) == expected_length
    for i, (url, attrs, client) in enumerate(res):
        assert url == expected_url[i]
        assert attrs == expected_attrs[i]
        assert client == expected_client[i]


def test_get_destinations_no_default_directory_single_destination(get_destinations_message):
    """Check getting destination urls when default directory isn't configured."""
    dispatcher = _get_dispatcher(test_yaml1)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(get_destinations_message)
    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destinations_with_aliases(get_destinations_message):
    """Check getting destination urls with aliases."""
    dispatcher = _get_dispatcher(test_yaml_aliases_simple)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_gs_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(get_destinations_message)

    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destinations_aliases_multiple(get_destinations_message):
    """Check getting destination urls with multiple aliases."""
    dispatcher = _get_dispatcher(test_yaml_aliases_multiple)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/alternate_dir_for_green_snow/NOAA-20_gs_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(get_destinations_message)

    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destionations_two_targets(get_destinations_message):
    """Check getting destinations for two target locations."""
    dispatcher = _get_dispatcher(test_yaml2)

    expected_length = 2
    expected_urls = ['ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif',
                     'ssh://server.target2.com/satellite/viirs/sat_201909190919_NOAA-20.tif']
    expected_attrs = [{'connection_uptime': 20},
                      {'ssh_key_filename': '~/.ssh/rsa_id.pub'}]
    expected_clients = ['target1', 'target2']

    res = dispatcher.get_destinations(get_destinations_message)

    _assert_get_destinations_res(res, expected_length, expected_urls, expected_attrs, expected_clients)


def test_dispatcher(get_destinations_message):
    """Test the dispatcher class."""
    dp = None
    try:
        dest_dir = os.path.join(gettempdir(), 'dptest')
        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir)
        with patch('trollmoves.dispatcher.ListenerContainer') as lc:
            queue = Queue()
            lc.return_value.output_queue = queue
            with NamedTemporaryFile('w', delete=False) as config_file:
                config_file_name = config_file.name
                config_file.write(test_local)
                config_file.flush()
                config_file.close()
                dp = Dispatcher(config_file_name)
                dp.start()
                dest_dir = os.path.join(gettempdir(), 'dptest')
                assert not os.path.exists(dest_dir)
                with NamedTemporaryFile('w') as test_file:
                    get_destinations_message.data['uri'] = test_file.name
                    expected_file = os.path.join(dest_dir, 'NOAA-20_201909190919.tif')
                    queue.put(get_destinations_message)
                    time.sleep(.1)
                    assert os.path.exists(expected_file)
            # Check that the listener config items are passed correctly
            lc.assert_called_once_with(
                addresses=['tcp://127.0.0.1:40000'],
                nameserver='127.0.0.1',
                services=['service_name_1', 'service_name_2'],
                topics={'/level3/cloudtype', '/level2/viirs', '/level2/avhrr'})
    finally:
        if dp is not None:
            dp.close()
        os.remove(expected_file)
        os.rmdir(dest_dir)
        os.remove(config_file_name)


def _write_config_file(config):
    with NamedTemporaryFile('w', delete=False) as config_file:
        config_file_name = config_file.name
        config_file.write(config)
        config_file.flush()
        config_file.close()
    return config_file_name


def _create_dest_url_dispatcher():
    with patch('trollmoves.dispatcher.ListenerContainer') as lc:
        queue = Queue()
        lc.return_value.output_queue = queue
        config_file_name = _write_config_file(test_yaml_ssh_scp)
        dispatcher = Dispatcher(config_file_name)
    return dispatcher, config_file_name


def test_create_dest_url_ssh_no_username(create_dest_url_message):
    """Test creation of destination URL for ssh without username."""
    dispatcher = None
    try:
        dispatcher, config_file_name = _create_dest_url_dispatcher()
        config = yaml.safe_load(test_yaml_ssh_scp)

        url, params, client = dispatcher.create_dest_url(
            create_dest_url_message, 'target2', config['target2'])

        expected_url = "ssh://server.target2.com/satellite/viirs/sat_201909190919_NOAA-20.tif"
        assert url == expected_url
        assert params == {'ssh_key_filename': '~/.ssh/rsa_id.pub'}
        assert client == "target2"
    finally:
        if dispatcher is not None:
            dispatcher.close()
        os.remove(config_file_name)


def test_create_dest_url_scp_with_username(create_dest_url_message):
    """Test creation of destination URL for scp with username."""
    dispatcher = None
    try:
        dispatcher, config_file_name = _create_dest_url_dispatcher()
        config = yaml.safe_load(test_yaml_ssh_scp)

        url, params, client = dispatcher.create_dest_url(create_dest_url_message, 'target3',
                                                         config['target3'])

        expected_url = "scp://user@server.target3.com/satellite/viirs/sat_201909190919_NOAA-20.tif"
        assert url == expected_url
        assert client == "target3"
    finally:
        if dispatcher is not None:
            dispatcher.close()
        os.remove(config_file_name)


def test_create_dest_url_ssh_no_filepattern(create_dest_url_message):
    """Test creation of destination URL for SSH without filepattern."""
    dispatcher = None
    try:
        dispatcher, config_file_name = _create_dest_url_dispatcher()
        config = yaml.safe_load(test_yaml_ssh_scp)

        url, params, client = dispatcher.create_dest_url(create_dest_url_message, 'target4',
                                                         config['target4'])

        expected_url = "scp://user@server.target4.com/satellite/viirs/201909190919_NOAA-20_viirs.tif"
        assert url == expected_url
    finally:
        if dispatcher is not None:
            dispatcher.close()
        os.remove(config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_no_port(NoisyPublisher, ListenerContainer, Message, publisher_config_file_name):
    """Test the publisher is initialized when no port is defined."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name)
            assert dispatcher.publisher is None
            NoisyPublisher.assert_not_called()
        finally:
            if dispatcher is not None:
                dispatcher.close()
    finally:
        os.remove(publisher_config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_no_port_with_nameserver(NoisyPublisher, ListenerContainer, Message, publisher_config_file_name):
    """Test the publisher is initialized without port but with nameservers."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name, publish_nameservers=["asd"])
            assert dispatcher.publisher is None
            NoisyPublisher.assert_not_called()
        finally:
            if dispatcher is not None:
                dispatcher.close()
    finally:
        os.remove(publisher_config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_with_random_publish_port(NoisyPublisher, ListenerContainer, Message,
                                                 publisher_config_file_name):
    """Test the publisher is initialized with randomly selected publish port."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name, publish_port=0)
            init_call = call("dispatcher", port=0, nameservers=None)
            assert init_call in NoisyPublisher.mock_calls
        finally:
            if dispatcher is not None:
                dispatcher.close()
            dispatcher.publisher.stop.assert_called_once()
    finally:
        os.remove(publisher_config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_publish_port_no_nameserver(NoisyPublisher, ListenerContainer, Message,
                                                   publisher_config_file_name):
    """Test the publisher is initialized with port but no nameservers."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name, publish_port=40000)
            init_call = call("dispatcher", port=40000, nameservers=None)
            assert init_call in NoisyPublisher.mock_calls
        finally:
            if dispatcher is not None:
                dispatcher.close()
            dispatcher.publisher.stop.assert_called_once()
    finally:
        os.remove(publisher_config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_port_and_nameservers(NoisyPublisher, ListenerContainer, Message, publisher_config_file_name):
    """Test the publisher is initialized with port and nameservers."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name, publish_port=40000,
                                    publish_nameservers=["asd"])

            assert dispatcher.publisher is pub
            init_call = call("dispatcher", port=40000, nameservers=["asd"])
            assert init_call in NoisyPublisher.mock_calls
        finally:
            if dispatcher is not None:
                dispatcher.close()
                dispatcher.publisher.stop.assert_called_once()
    finally:
        os.remove(publisher_config_file_name)


@patch('trollmoves.dispatcher.Message')
@patch('trollmoves.dispatcher.ListenerContainer')
@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_call(NoisyPublisher, ListenerContainer, Message, publisher_config_file_name):
    """Test the publisher being called properly."""
    pub = Mock()
    NoisyPublisher.return_value = pub
    try:
        try:
            dispatcher = Dispatcher(publisher_config_file_name, publish_port=40000,
                                    publish_nameservers=["asd"])
            msg = Mock(data={'uri': 'original_path',
                             'platform_name': 'platform'})
            destinations = [['url1', 'params1', 'target2'],
                            ['url2', 'params2', 'target3']]
            success = {'target2': False, 'target3': True}
            dispatcher._publish(msg, destinations, success)
            dispatcher.publisher.send.assert_called_once()
            # The message topic has been composed and uri has been replaced
            msg_call = call('/topic/platform', 'file',
                            {'uri': 'url2', 'platform_name': 'platform'})
            assert msg_call in Message.mock_calls
        finally:
            if dispatcher is not None:
                dispatcher.close()
                dispatcher.publisher.stop.assert_called()
    finally:
        os.remove(publisher_config_file_name)


def _run_dispatch(destinations):
    with NamedTemporaryFile('w') as source_file:
        source_file_name = source_file.name
        source_file.flush()

        res = dispatch(source_file_name, destinations)
    return res


@patch('trollmoves.dispatcher.move_it')
def test_dispatch_two_successful_dispatches(move_it):
    """Test dispatching with two successful dispatches."""
    destinations = [['url1', 'params1', 'target1'],
                    ['url2', 'params2', 'target2']]
    res = _run_dispatch(destinations)
    assert res == {'target1': True, 'target2': True}
    assert len(move_it.mock_calls) == 2


def test_dispatch_identical_names():
    """Test dispatching with two identical client names."""
    destinations = [['url1', 'params1', 'target1'],
                    ['url2', 'params2', 'target1']]

    try:
        _ = _run_dispatch(destinations)
    except NotImplementedError:
        pass
    else:
        raise AssertionError("Identical clients should not work")


@patch('trollmoves.dispatcher.move_it')
def test_dispatch_one_dispatch_fails(move_it, caplog):
    """Test dispatching with one dispatch failing."""
    destinations = [['url1', 'params1', 'target1'],
                    ['url2', 'params2', 'target2']]
    move_it.side_effect = [None, IOError('test')]

    res = _run_dispatch(destinations)

    assert "Could not dispatch to url2: test" in caplog.text
    assert res == {'target1': True, 'target2': False}
