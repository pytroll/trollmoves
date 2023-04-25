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
from datetime import datetime
from glob import glob
from queue import Queue
from tempfile import NamedTemporaryFile, gettempdir
from threading import Thread
from unittest.mock import Mock, patch, call

import pytest
import yaml
from posttroll.message import Message

from trollmoves.dispatcher import (
    Dispatcher, read_config, check_conditions, dispatch, PublisherReporter
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
target1:
  host: ""
  filepattern: '{platform_name}_{start_time:%Y%m%d%H%M}.{format}'
  directory: """ + os.path.join(gettempdir(), 'dptest') + """

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

test_local_with_listener = """
posttroll_subscriber:
    subscribe_addresses:
      - tcp://127.0.0.1:40000
    nameserver: 127.0.0.1
    subscribe_services:
      - service_name_1
      - service_name_2""" + test_local


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

test_local_creation_time = """
target1:
  host: ""
  filepattern: '{platform_name}_{product}_{start_time:%Y%m%d%H%M}_{file_creation_time:%Y%m%d%H%M%S}.{format}'
  directory: """ + os.path.join(gettempdir(), 'dptest') + """

  dispatch_configs:
    - topics:
        - /level2/viirs
      conditions:
        # key matches metadata items or provides default
        - product: [green_snow, true_color]
          sensor: viirs
    """

test_dataset_samefilenames = """
target1:
  host: ""
  directory: """ + os.path.join(gettempdir(), 'dptest') + """
  aliases:
    platform_name:
      Suomi-NPP: npp
      NOAA-20: j01
      NOAA-21: j02

  dispatch_configs:
    - topics:
        - /atms/sdr/1
      conditions:
        - sensor: [atms, [atms]]
          format: SDR
          variant: DR
    """


@pytest.fixture
def check_conditions_string_config():
    """Check conditions string config."""
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
def viirs_green_snow_message(tmp_path):
    """Get the destinations message."""
    uid = '201909190919_NOAA-20_viirs.tif'
    filename = os.fspath(tmp_path / uid)

    msg = Message("/level2/viirs", "file",
                  data={'sensor': 'viirs', 'product': 'green_snow', 'platform_name': 'NOAA-20',
                        'start_time': datetime(2019, 9, 19, 9, 19), 'format': 'tif',
                        'uid': uid,
                        'uri': filename
                        })
    create_empty_file(filename)
    yield msg


@pytest.fixture
def atms_sdr_dataset_message(tmp_path):
    """Get the message with ATMS SDR files in a dataset."""
    SDR_UIDS = ['SATMS_npp_d20230405_t0839333_e0847009_b59261_c20230405084834951682_cspp_dev.h5',
                'GATMO_npp_d20230405_t0839333_e0847009_b59261_c20230405084835126023_cspp_dev.h5',
                'TATMS_npp_d20230405_t0839333_e0847009_b59261_c20230405084835042744_cspp_dev.h5']
    uris = []
    uids = []
    for uid in SDR_UIDS:
        filename = os.fspath(tmp_path / uid)
        uids.append(uid)
        uris.append(filename)
        create_empty_file(filename)

    msg = Message("/atms/sdr/1", "dataset",
                  data={"start_time": datetime(2023, 4, 5, 8, 39, 0, 700000),
                        "end_time": datetime(2023, 4, 5, 8, 48, 4, 600000),
                        "orbit_number": 59261,
                        "platform_name": "Suomi-NPP",
                        "sensor": "atms",
                        "data_processing_level": "1B",
                        "variant": "DR",
                        "collection_area_id": "euron1",
                        "type": "HDF5", "format": "SDR",
                        "dataset": [{'uri': uris[0],
                                     'uid': uids[0]},
                                    {'uri': uris[1],
                                     'uid': uids[1]},
                                    {'uri': uris[2],
                                     'uid': uids[2]}]
                        })
    yield msg


@pytest.fixture
def create_dest_url_message(tmp_path, viirs_green_snow_message):
    """Create the destination url message."""
    viirs_green_snow_message.data['uid'] = '67e91f4a778adc59e5f1a4f0475e388b'

    yield viirs_green_snow_message


@pytest.fixture
def publisher_config_file_name():
    """Create a temporary config file name."""
    with NamedTemporaryFile('w', delete=False) as config_file:
        config_file_name = config_file.name
        config_file.write(test_yaml_pub)
        config_file.flush()
        config_file.close()
    yield config_file_name


def test_config_reading():
    """Test reading the configuration."""
    expected = yaml.safe_load(test_yaml1)
    with NamedTemporaryFile('w', delete=False) as config_file:
        fname = config_file.name
        try:
            config_file.write(test_yaml1)
            config_file.flush()
            config_file.close()
            config = read_config(fname)

            assert config == expected
        finally:
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


@pytest.fixture
def dispatcher_creator(tmp_path):
    """Create a dispatcher factory."""
    def _create_dispatcher(config):
        config_file = tmp_path / "my_config"
        with open(config_file, mode="w") as fd:
            fd.write(config)
        return Dispatcher(os.fspath(config_file), messages=["some", "messages"])

    return _create_dispatcher


def test_get_destinations_single_destination(viirs_green_snow_message, dispatcher_creator):
    """Check getting destination urls for single destination."""
    dispatcher = dispatcher_creator(test_yaml1)

    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}

    res = dispatcher.get_destinations(viirs_green_snow_message)
    assert len(res) == 1
    url, attrs, client = res[0]
    assert url == expected_url
    assert attrs == expected_attrs
    assert client == "target1"


def test_get_destinations_two_destinations(viirs_green_snow_message, dispatcher_creator):
    """Check getting destination urls for two destinations."""
    dispatcher = dispatcher_creator(test_yaml2)

    res = dispatcher.get_destinations(viirs_green_snow_message)

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


def test_get_destinations_no_default_directory_single_destination(viirs_green_snow_message, dispatcher_creator):
    """Check getting destination urls when default directory isn't configured."""
    dispatcher = dispatcher_creator(test_yaml_no_default_directory)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(viirs_green_snow_message)
    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destinations_with_aliases(viirs_green_snow_message, dispatcher_creator):
    """Check getting destination urls with aliases."""
    dispatcher = dispatcher_creator(test_yaml_aliases_simple)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/viirs/NOAA-20_gs_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(viirs_green_snow_message)

    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destinations_aliases_multiple(viirs_green_snow_message, dispatcher_creator):
    """Check getting destination urls with multiple aliases."""
    dispatcher = dispatcher_creator(test_yaml_aliases_multiple)

    expected_length = 1
    expected_url = 'ftp://ftp.target1.com/input_data/alternate_dir_for_green_snow/NOAA-20_gs_201909190919.tif'
    expected_attrs = {'connection_uptime': 20}
    expected_client = "target1"

    res = dispatcher.get_destinations(viirs_green_snow_message)

    _assert_get_destinations_res(res, expected_length, expected_url, expected_attrs, expected_client)


def test_get_destionations_two_targets(viirs_green_snow_message, dispatcher_creator):
    """Check getting destinations for two target locations."""
    dispatcher = dispatcher_creator(test_yaml2)

    expected_length = 2
    expected_urls = ['ftp://ftp.target1.com/input_data/viirs/NOAA-20_201909190919.tif',
                     'ssh://server.target2.com/satellite/viirs/sat_201909190919_NOAA-20.tif']
    expected_attrs = [{'connection_uptime': 20},
                      {'ssh_key_filename': '~/.ssh/rsa_id.pub'}]
    expected_clients = ['target1', 'target2']

    res = dispatcher.get_destinations(viirs_green_snow_message)

    _assert_get_destinations_res(res, expected_length, expected_urls, expected_attrs, expected_clients)


def test_dispatcher(tmp_path, viirs_green_snow_message):
    """Test the dispatcher class."""
    dp = None
    try:

        dest_dir = tmp_path / 'dptest'
        config_filepath = tmp_path / "config_file"

        create_config_file(config_filepath, test_local, dest_dir)

        assert not os.path.exists(dest_dir)

        dp = Dispatcher(os.fspath(config_filepath), messages=[viirs_green_snow_message])
        dp.run()

        expected_file = dest_dir / 'NOAA-20_201909190919.tif'
        assert os.path.exists(expected_file)

    finally:
        pass
        # if dp is not None:
        #     dp.close()


def test_dispatch_dataset(tmp_path, atms_sdr_dataset_message):
    """Test the dispatcher class dispatching a dataset."""
    dp = None
    try:

        dest_dir = tmp_path / 'dptest'
        config_filepath = tmp_path / "config_file"

        create_config_file(config_filepath, test_dataset_samefilenames, dest_dir)

        assert not os.path.exists(dest_dir)

        dp = Dispatcher(os.fspath(config_filepath), messages=[atms_sdr_dataset_message])
        dp.run()
        expected_file = dest_dir / 'SATMS_npp_d20230405_t0839333_e0847009_b59261_c20230405084834951682_cspp_dev.h5'
        assert os.path.exists(expected_file)
        expected_file = dest_dir / 'GATMO_npp_d20230405_t0839333_e0847009_b59261_c20230405084835126023_cspp_dev.h5'
        assert os.path.exists(expected_file)
        expected_file = dest_dir / 'TATMS_npp_d20230405_t0839333_e0847009_b59261_c20230405084835042744_cspp_dev.h5'
        assert os.path.exists(expected_file)

    finally:
        pass


def test_dispatcher_uses_listener_container_config(tmp_path, viirs_green_snow_message):
    """Test the dispatcher class with listener_container."""
    with patch('trollmoves.dispatcher.ListenerContainer') as lc:
        queue = Queue()
        lc.return_value.output_queue = queue

        dest_dir = tmp_path / 'dptest'
        config_filepath = tmp_path / "config_file"

        create_config_file(config_filepath, test_local_with_listener, dest_dir)

        dp = Dispatcher(os.fspath(config_filepath))

        thread = Thread(target=dp.run)
        thread.start()
        dp.close()
        thread.join()
        # Check that the listener config items are passed correctly
        lc.assert_called_once_with(
            addresses=['tcp://127.0.0.1:40000'],
            nameserver='127.0.0.1',
            services=['service_name_1', 'service_name_2'],
            topics={'/level3/cloudtype', '/level2/viirs', '/level2/avhrr'})


def test_dispatcher_uses_listener_to_act_on_messages(tmp_path, viirs_green_snow_message):
    """Test the dispatcher class with listener_container."""
    with patch('trollmoves.dispatcher.ListenerContainer') as lc:
        queue = Queue()
        lc.return_value.output_queue = queue

        dest_dir = tmp_path / 'dptest'
        config_filepath = tmp_path / "config_file"

        create_config_file(config_filepath, test_local_with_listener, dest_dir)

        assert not os.path.exists(dest_dir)

        dp = Dispatcher(os.fspath(config_filepath))
        thread = Thread(target=dp.run)
        thread.start()
        queue.put(viirs_green_snow_message)
        dp.close()
        thread.join()
        expected_file = dest_dir / 'NOAA-20_201909190919.tif'
        assert os.path.exists(expected_file)


def create_empty_file(filename):
    """Create an empty file."""
    with open(filename, mode="a"):
        pass


def create_config_file(config_filepath, config, dest_dir):
    """Create an actual configuration file."""
    local_config = yaml.safe_load(config)
    local_config["target1"]["directory"] = os.fspath(dest_dir)
    local_config = yaml.dump(local_config)
    with open(config_filepath, mode="w") as fd:
        fd.write(local_config)


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


@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_no_port(NoisyPublisher, publisher_config_file_name):
    """Test the publisher is initialized when no port is defined."""
    NoisyPublisher.return_value = Mock()

    dispatcher = Dispatcher(publisher_config_file_name)
    assert dispatcher.publisher is None
    NoisyPublisher.assert_not_called()

    dispatcher.close()


@patch('trollmoves.dispatcher.NoisyPublisher')
def test_publisher_init_no_port_with_nameserver(NoisyPublisher, publisher_config_file_name):
    """Test the publisher is initialized without port but with nameservers."""
    NoisyPublisher.return_value = Mock()

    dispatcher = Dispatcher(publisher_config_file_name, publish_nameservers=["asd"])
    assert dispatcher.publisher is None
    NoisyPublisher.assert_not_called()

    dispatcher.close()


@patch('trollmoves.dispatcher.create_publisher_from_dict_config')
def test_publisher_init_with_random_publish_port(create_publisher, publisher_config_file_name):
    """Test the publisher is initialized with randomly selected publish port."""
    create_publisher.return_value = Mock()

    dispatcher = Dispatcher(publisher_config_file_name, publish_port=0)
    init_call = call({"name": "dispatcher", "port": 0, "nameservers": None})
    assert init_call in create_publisher.mock_calls

    dispatcher.close()
    create_publisher.return_value.stop.assert_called_once()


@patch('trollmoves.dispatcher.create_publisher_from_dict_config')
def test_publisher_init_publish_port_no_nameserver(create_publisher, publisher_config_file_name):
    """Test the publisher is initialized with port but no nameservers."""
    create_publisher.return_value = Mock()

    dispatcher = Dispatcher(publisher_config_file_name, publish_port=40000)
    init_call = call({"name": "dispatcher", "port": 40000, "nameservers": None})
    assert init_call in create_publisher.mock_calls

    dispatcher.close()
    create_publisher.return_value.stop.assert_called_once()


@patch('trollmoves.dispatcher.create_publisher_from_dict_config')
def test_publisher_init_port_and_nameservers(create_publisher, publisher_config_file_name):
    """Test the publisher is initialized with port and nameservers."""
    pub = Mock()
    create_publisher.return_value = pub

    dispatcher = Dispatcher(publisher_config_file_name, publish_port=40000, publish_nameservers=["asd"])

    init_call = call({"name": "dispatcher", "port": 40000, "nameservers": ["asd"]})
    assert init_call in create_publisher.mock_calls

    dispatcher.close()
    create_publisher.return_value.stop.assert_called_once()


@patch('trollmoves.dispatcher.Message', wraps=Message)
@patch('trollmoves.dispatcher.create_publisher_from_dict_config')
def test_publisher_call(create_publisher, Message, publisher_config_file_name):
    """Test the publisher being called properly."""
    create_publisher.return_value = Mock()

    publisher = PublisherReporter(read_config(publisher_config_file_name),
                                  publish_port=40000, publish_nameservers=["asd"])
    msg = Message("/some/data", "file",
                  data={'uri': 'original_path',
                        'platform_name': 'platform'})
    destinations = [['url1', 'params1', 'target2'],
                    ['url2', 'params2', 'target3']]
    success = {'target2': False, 'target3': True}
    publisher.publish(msg, destinations, success)
    publisher.publisher.send.assert_called_once()
    # The message topic has been composed and uri has been replaced
    msg_call = call('/topic/platform', 'file',
                    {'uri': 'url2', 'platform_name': 'platform'})
    assert msg_call in Message.mock_calls

    publisher.stop()


@patch('trollmoves.dispatcher.Message', wraps=Message)
@patch('trollmoves.dispatcher.create_publisher_from_dict_config')
def test_publisher_not_called_when_topic_missing(NoisyPublisher, Message, tmp_path, caplog):
    """Test the publisher being called properly."""
    NoisyPublisher.return_value = Mock()

    dest_dir = tmp_path / 'dptest'
    config_filepath = tmp_path / "config_file"

    create_config_file(config_filepath, test_local, dest_dir)

    publisher = PublisherReporter(read_config(config_filepath), publish_port=40000, publish_nameservers=["asd"])
    msg = Message("/some/data", "file",
                  data={'uri': 'original_path',
                        'platform_name': 'platform'})
    destinations = [['url1', 'params1', 'target2'],
                    ['url2', 'params2', 'target3']]
    success = {'target2': False, 'target3': True}
    publisher.publish(msg, destinations, success)
    assert "Publish topic not configured for 'target3'" in caplog.text


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


def test_dispatch_local_with_file_creation_time(tmp_path, viirs_green_snow_message):
    """Test the dispatcher class."""
    dest_dir = tmp_path / 'dptest'
    config_filepath = tmp_path / "config_file"

    create_config_file(config_filepath, test_local_creation_time, dest_dir)

    dp = Dispatcher(os.fspath(config_filepath))
    assert not os.path.exists(dest_dir)

    dp.dispatch_from_message(viirs_green_snow_message)

    expected_file = dest_dir / 'NOAA-20_green_snow_201909190919_*.tif'
    found_files = glob(os.fspath(expected_file))
    assert len(found_files) == 1

    filename, _ = os.path.splitext(found_files[0])
    _, timestamp = filename.rsplit("_", 1)
    creation_time = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    assert 0 < (datetime.now() - creation_time).total_seconds() < 2
