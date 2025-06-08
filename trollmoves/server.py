#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2023
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
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

"""Classes and functions for Trollmoves server."""
import argparse
import bz2
import datetime
import errno
import fnmatch
import glob
import logging.handlers
import os
import subprocess
import tempfile
import time
import warnings
from collections import deque
from configparser import ConfigParser
from contextlib import suppress
from functools import partial
from queue import Empty, Queue
from threading import Lock, Thread
from urllib.parse import urlparse

from posttroll import get_context
from posttroll.message import Message, MessageError
from posttroll.publisher import get_own_ip
from posttroll.subscriber import Subscribe
from trollsift import globify, parse
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from zmq import NOBLOCK, POLLIN, PULL, PUSH, ROUTER, Poller, ZMQError

from trollmoves.client import DEFAULT_REQ_TIMEOUT
from trollmoves.logging import add_logging_options_to_parser
from trollmoves.move_it_base import (MoveItBase, WatchdogChangeHandler,
                                     WatchdogCreationHandler, create_publisher)
from trollmoves.movers import move_it
from trollmoves.utils import (clean_url, gen_dict_contains, gen_dict_extract,
                              is_file_local)

LOGGER = logging.getLogger(__name__)


file_cache = deque(maxlen=61000)
file_cache_lock = Lock()
START_TIME = datetime.datetime.now(datetime.timezone.utc)

CONNECTION_CONFIG_ITEMS = ["connection_uptime", "ssh_key_filename", "ssh_connection_timeout", "ssh_private_key_file"]


class RequestManager(Thread):
    """Manage requests."""

    def __init__(self, port, attrs=None):
        """Initialize request manager."""
        Thread.__init__(self)

        self.port = port
        self._attrs = attrs
        self._loop = True
        self.out_socket = None
        self.in_socket = None
        self._poller = None
        self._station = None

        self._validate_file_pattern()
        self._set_out_socket()
        self._set_in_socket()
        self._set_station()
        self._create_poller()
        self._deleter = Deleter(attrs)

    def _set_out_socket(self):
        self.out_socket = get_context().socket(ROUTER)
        self.out_socket.bind("tcp://*:" + str(self.port))

    def _set_in_socket(self):
        self.in_socket = get_context().socket(PULL)
        self.in_socket.bind("inproc://replies" + str(self.port))

    def _set_station(self):
        try:
            self._station = self._attrs["station"]
        except (KeyError, TypeError):
            LOGGER.warning("Station is not defined in config file")
            self._station = "unknown"
        LOGGER.debug("Station is '%s'", self._station)

    def _create_poller(self):
        self._poller = Poller()
        self._poller.register(self.out_socket, POLLIN)
        self._poller.register(self.in_socket, POLLIN)

    def _validate_file_pattern(self):
        try:
            _ = globify(self._attrs["origin"])
        except ValueError as err:
            raise ConfigError('Invalid file pattern: ' + str(err))
        except KeyError:
            if 'listen' not in self._attrs:
                raise

    def start(self):
        """Start the request manager."""
        self._deleter.start()
        Thread.start(self)

    def pong(self, message):
        """Reply to ping."""
        return Message(message.subject, "pong", {"station": self._station})

    def push(self, message):
        """Reply to push request."""
        new_msg = self._move_files(message)
        if new_msg and new_msg.type != 'err':
            _destination = clean_url(new_msg.data['destination'])
            new_msg = Message(message.subject,
                              _get_push_message_type(message),
                              data=message.data.copy())
            new_msg.data['destination'] = _destination

        return new_msg

    def _move_files(self, message):
        return_message = None
        for data in gen_dict_contains(message.data, 'uri'):
            pathname = urlparse(data['uri']).path
            rel_path = data.get('path', None)
            return_message = self._validate_requested_file(pathname, message)
            if return_message is not None:
                break
            return_message = self._move_file(pathname, message, rel_path)
            if return_message.type == "err":
                break

        return return_message

    def _validate_requested_file(self, pathname, message):
        # FIXME: check against file_cache
        if 'origin' in self._attrs and not fnmatch.fnmatch(
                os.path.basename(pathname),
                os.path.basename(globify(self._attrs["origin"]))):
            LOGGER.warning('Client trying to get invalid file: %s', pathname)
            return Message(message.subject, "err", data="{0:s} not reachable".format(pathname))
        return None

    def _move_file(self, pathname, message, rel_path):
        return_message = None
        try:
            destination = move_it(pathname, message.data['destination'],
                                  self._attrs["connection_parameters"],
                                  rel_path=rel_path,
                                  backup_targets=message.data.get('backup_targets', None))
            message.data['destination'] = destination
        except Exception as err:
            return_message = Message(message.subject, "err", data=str(err))
        else:
            self._add_to_deleter(pathname)
            return_message = message
        return return_message

    def _add_to_deleter(self, pathname):
        if self._attrs.get('compression') or self._is_delete_set():
            self._deleter.add(pathname)

    def _is_delete_set(self):
        return self._attrs.get('delete', False)

    def ack(self, message):
        """Reply with ack to a publication."""
        new_msg = None
        for url in gen_dict_extract(message.data, 'uri'):
            pathname = urlparse(url).path
            new_msg = self._validate_requested_file(pathname, message)
            if new_msg is not None:
                break
            self._add_to_deleter(pathname)

        if new_msg is None:
            new_msg = _get_cleaned_ack_message(message)

        return new_msg

    def info(self, message):
        """Collect information from file cache to message."""
        uptime = datetime.datetime.now(datetime.timezone.utc) - START_TIME
        files, max_count = _collect_cached_files(message)

        return Message(message.subject, "info", data={"files": files, "max_count": max_count, "uptime": str(uptime)})

    def unknown(self, message):
        """Reply to any unknown request."""
        return Message(message.subject, "unknown")

    def reply_and_send(self, fun, address, message):
        """Reply to request."""
        reply = Message(message.subject, "error")
        try:
            reply = fun(message)
        except Exception:
            LOGGER.exception("Something went wrong"
                             " when processing the request: %s", str(message))
        finally:
            self._send_multipart_reply(reply, address)

    def _send_multipart_reply(self, reply, address):
        LOGGER.debug("Response: %s", str(reply))
        in_socket = get_context().socket(PUSH)
        in_socket.connect("inproc://replies" + str(self.port))
        try:
            in_socket.send_multipart([address, b'', str(reply)])
        except TypeError:
            in_socket.send_multipart([address, b'', bytes(str(reply), 'utf-8')])

    def run(self):
        """Run request manager."""
        try:
            self._run()
        except Exception:
            LOGGER.exception("Request Manager died.")

    def _run(self):
        """Run request manager."""
        while self._loop:
            self._run_loop()

    def _run_loop(self):
        """Run request manager loop."""
        try:
            socks = dict(self._poller.poll(timeout=2000))
        except ZMQError:
            LOGGER.info("Poller interrupted.")
            return
        if socks.get(self.out_socket) == POLLIN:
            address, payload = self._get_address_and_payload()
            if payload is None:
                return
            try:
                self._process_request(Message(rawstr=payload), address)
            except MessageError:
                LOGGER.exception("Failed to create message from payload: %s with address %s",
                                 str(payload), str(address))
        elif socks.get(self.in_socket) == POLLIN:
            self.out_socket.send_multipart(self.in_socket.recv_multipart(NOBLOCK))

    def _get_address_and_payload(self):
        address, payload = None, None
        LOGGER.debug("Received a request")
        multiparts = self.out_socket.recv_multipart(NOBLOCK)
        try:
            address, _, payload = multiparts
        except ValueError:
            LOGGER.warning("Invalid request.")
            try:
                address = multiparts[0]
            except (TypeError, IndexError):
                LOGGER.warning("Address unknown, not sending an error message back.")
            else:
                message = Message('error', 'error', "Invalid message received")
                Thread(target=self.reply_and_send, args=(self.unknown, address, message)).start()
                LOGGER.warning("Sent error message back.")
        return address, payload

    def _process_request(self, message, address):
        LOGGER.debug("processing request: %s", str(_sanitize_message_destination(message)))
        if message.type == "ping":
            Thread(target=self.reply_and_send, args=(self.pong, address, message)).start()
        elif message.type == "push":
            Thread(target=self.reply_and_send, args=(self.push, address, message)).start()
        elif message.type == "ack":
            Thread(target=self.reply_and_send, args=(self.ack, address, message)).start()
        elif message.type == "info":
            Thread(target=self.reply_and_send, args=(self.info, address, message)).start()
        else:  # unknown request
            Thread(target=self.reply_and_send, args=(self.unknown, address, message)).start()

    def stop(self):
        """Stop the request manager."""
        self._loop = False
        self._deleter.stop()
        self.out_socket.close(1)
        self.in_socket.close(1)


class AbstractMoveItServer(MoveItBase):
    """Abstract base class for the move it server."""

    def terminate(self, publisher=None):
        """Terminate the given *chains* and stop the *publisher*."""
        for chain in self.chains.values():
            chain.stop()

        if publisher:
            publisher.stop()

        LOGGER.info("Shutting down.")
        print("Thank you for using pytroll/move_it_server."
              " See you soon on pytroll.org!")

    def reload_config(self, filename,
                      notifier_builder=None,
                      disable_backlog=False,
                      use_polling=False):
        """Rebuild chains if needed (if the configuration changed) from *filename*."""
        LOGGER.debug("New config file detected: %s", filename)

        new_chain_configs = read_config(filename)

        old_glob = _update_chains(self.chains, new_chain_configs, self.request_manager, use_polling,
                                  notifier_builder, self.function_to_run_on_matching_files)
        _disable_removed_chains(self.chains, new_chain_configs)
        LOGGER.debug("Reloaded config from %s", filename)
        _process_old_files(old_glob, disable_backlog)
        LOGGER.debug("done reloading config")

    def _run(self):
        try:
            self.publisher.heartbeat(30)
        except ZMQError:
            if self.running:
                raise
        except AttributeError:
            pass


class MoveItServer(AbstractMoveItServer):
    """Wrapper class for Trollmoves Server."""

    def __init__(self, cmd_args):
        """Initialize server."""
        self.name = "move_it_server"
        publisher = create_publisher(cmd_args.port, self.name)
        super().__init__(cmd_args, publisher=publisher)
        self.request_manager = RequestManager
        self.function_to_run_on_matching_files = partial(process_notification, publisher=self.publisher)

    def reload_cfg_file(self, filename):
        """Reload configuration file."""
        self.reload_config(filename,
                           disable_backlog=self.cmd_args.disable_backlog,
                           use_polling=self.cmd_args.watchdog)

    def signal_reload_cfg_file(self, *args):
        """Handle reload signal."""
        del args
        self.reload_cfg_file(self.cmd_args.config_file)


class ConfigError(Exception):
    """Configuration error."""

    pass


class Deleter(Thread):
    """Class for deleting moved files."""

    def __init__(self, attrs):
        """Initialize Deleter."""
        Thread.__init__(self)
        self.queue = Queue()
        self.timer = None
        self.loop = True
        self._attrs = attrs or dict()

    def add(self, filename):
        """Schedule file for deletion."""
        remove_delay = int(self._attrs.get('remove_delay', 30))
        LOGGER.debug('Scheduling %s for removal in %ds', filename, remove_delay)
        self.queue.put((filename, time.time() + remove_delay))

    def run(self):
        """Start the deleter."""
        while self.loop:
            try:
                filename, the_time = self.queue.get(True, 2)
            except Empty:
                continue
            while self.loop:
                time.sleep(min(2, max(the_time - time.time(), 0)))
                if the_time <= time.time():
                    try:
                        self.delete(filename)
                    except Exception:
                        LOGGER.exception(
                            'Something went wrong when deleting %s', filename)
                    else:
                        LOGGER.debug('Removed %s.', filename)
                    break

    @staticmethod
    def delete(filename):
        """Delete the given file.

        If the file is not present, this function does *not* raise an error.
        """
        try:
            os.remove(filename)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            LOGGER.debug("File already deleted: %s", filename)

    def stop(self):
        """Stop the deleter."""
        self.loop = False
        if self.timer:
            self.timer.cancel()


def _get_push_message_type(message):
    message_type = message.type
    if 'uri' in message.data:
        message_type = 'file'
    elif 'dataset' in message.data:
        message_type = 'dataset'
    elif 'collection' in message.data:
        message_type = 'collection'
    return message_type


def _get_cleaned_ack_message(message):
    new_msg = Message(message.subject, "ack", data=message.data.copy())
    try:
        new_msg.data['destination'] = clean_url(new_msg.data[
            'destination'])
    except KeyError:
        pass

    return new_msg


def _collect_cached_files(message):
    max_count = 2256  # Let's set a (close to arbitrary) limit on messages size.
    try:
        max_count = min(message.data.get("max_count", max_count), max_count)
    except AttributeError:
        pass
    files = []
    with file_cache_lock:
        for i in file_cache:
            if i.startswith(message.subject):
                files.append(i)
                if len(files) == max_count:
                    break
    return files, max_count


def _sanitize_message_destination(message):
    sanitized_message = Message(rawstr=str(message))
    try:
        _ = urlparse(message.data['destination'])
    except (KeyError, TypeError):
        pass
    else:
        sanitized_message.data['destination'] = clean_url(message.data['destination'])
    return sanitized_message


class Listener(Thread):
    """A message listener for the server."""

    def __init__(self, function_to_run_on_message, config):
        """Initialize the listener."""
        super(Listener, self).__init__()
        self.attrs = config
        self.function_to_run_on_message = function_to_run_on_message
        self.loop = True

    def run(self):
        """Start listening to messages."""
        with Subscribe(
            services=self.attrs.get('services', ''),
            topics=self.attrs.get('topics', self.attrs['listen']),
            addr_listener=bool(self.attrs.get('addr_listener', True)),
            addresses=self.attrs.get('addresses'),
            timeout=int(self.attrs.get('timeout', 10)),
            translate=bool(self.attrs.get('translate', False)),
            nameserver=self.attrs.get('nameserver'),
        ) as sub:
            self._run(sub)

    def _run(self, sub):
        for msg in sub.recv(1):
            if not self.loop:
                break
            if msg is None:
                continue
            if not _files_in_message_are_local(msg):
                break
            self.function_to_run_on_message(msg)

    def stop(self):
        """Stop the listener."""
        self.loop = False


def _files_in_message_are_local(msg):
    for uri in gen_dict_extract(msg.data, 'uri'):
        urlobj = urlparse(uri)
        if not is_file_local(urlobj):
            return False
    return True


def _collect_attribute_info(attrs):
    info = attrs.get("info", {})
    if info:
        info = dict((elt.strip().split('=') for elt in info.split(";")))
        for infokey, infoval in info.items():
            if "," in infoval:
                info[infokey] = infoval.split(",")
    return info


def read_config(filename):
    """Read the config file called *filename*."""
    return _read_ini_config(filename)


def _read_ini_config(filename):
    cp_ = ConfigParser(interpolation=None)
    with open(filename) as config_file:
        cp_.read_file(config_file)

    res = {}

    for section in cp_.sections():
        res[section] = dict(cp_.items(section))
        _set_config_defaults(res[section])
        _parse_nameserver(res[section], cp_[section])
        _parse_addresses(res[section])
        _parse_delete(res[section], cp_[section])
        res[section] = _create_config_sub_dicts(res[section])
        res[section] = _form_connection_parameters_dict(res[section])
        if not _check_origin_and_listen(res, section):
            continue
        if not _check_topic(res, section):
            continue
        _verify_publish_port(res[section])
    return res


def _set_config_defaults(conf):
    conf.setdefault("working_directory", None)
    conf.setdefault("compression", False)
    conf.setdefault("req_timeout", DEFAULT_REQ_TIMEOUT)
    conf.setdefault("transfer_req_timeout", 10 * DEFAULT_REQ_TIMEOUT)
    conf.setdefault("ssh_key_filename", None)
    conf.setdefault("delete", False)


def _parse_nameserver(conf, raw_conf):
    try:
        val = raw_conf.getboolean("nameserver")
    except ValueError:
        val = conf["nameserver"]
    conf["nameserver"] = val


def _parse_addresses(conf):
    val = conf.get("addresses")
    if isinstance(val, str):
        val = val.split()
    conf["addresses"] = val


def _parse_delete(conf, raw_conf):
    val = raw_conf.getboolean("delete")
    if val is not None:
        conf["delete"] = val


def _create_config_sub_dicts(original):
    # Take a copy so we can modify the values if necessary
    res = dict(original.items())
    for key in original.keys():
        parts = key.split("__")
        if len(parts) > 1:
            _create_dicts(res, parts, original[key])
            del res[key]
    return res


def _create_dicts(res, parts, val):
    cur = res
    for part in parts[:-1]:
        if part not in cur:
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = _check_bool(val)


def _check_bool(val):
    if val.lower() in ["0", "false"]:
        return False
    elif val.lower() in ["1", "true"]:
        return True
    return val


def _form_connection_parameters_dict(original):
    # Take a copy so we can modify the values if necessary
    res = dict(original.items())
    if "connection_parameters" not in res:
        res["connection_parameters"] = {}
    for key in original.keys():
        if key in CONNECTION_CONFIG_ITEMS:
            warnings.warn(
                f"Consider using connection_parameters__{key} instead of {key}.",
                category=UserWarning,
                stacklevel=2)
            res["connection_parameters"][key] = original[key]
            del res[key]
    return res


def _check_origin_and_listen(res, section):
    if ("origin" not in res[section]) and ('listen' not in res[section]):
        LOGGER.warning("Incomplete section %s: add an 'origin' or 'listen' item.", section)
        LOGGER.info("Ignoring section %s: incomplete.", section)
        del res[section]
        return False
    return True


def _check_topic(res, section):
    if "topic" not in res[section]:
        LOGGER.warning("Incomplete section %s: add an 'topic' item.", section)
        LOGGER.info("Ignoring section %s: incomplete.", section)
        return False
    return True


def _verify_publish_port(conf):
    try:
        conf["publish_port"] = int(conf["publish_port"])
    except (KeyError, ValueError):
        conf["publish_port"] = 0


def _update_chains(chains, new_chain_configs, manager, use_polling, notifier_builder,
                   function_to_run_on_matching_files):
    old_glob = []
    for chain_name, chain_config in new_chain_configs.items():
        chain_updated = False
        if chain_name in chains:
            if _chains_are_identical(chains, new_chain_configs, chain_name):
                continue
            chain_updated = True
            chains[chain_name].stop()

        try:
            chain = _add_chain(chains, chain_name, chain_config, manager)
        except ConfigError:
            continue

        chain.create_notifier(notifier_builder, use_polling, function_to_run_on_matching_files)
        chain.start()

        if 'origin' in chain_config:
            old_glob.append((globify(chain_config["origin"]), chain.function_to_run, chain_config))

        if chain_updated:
            LOGGER.debug("Updated %s", chain_name)
        else:
            LOGGER.debug("Added %s", chain_name)

    return old_glob


def _chains_are_identical(chains, new_chains, chain_name):
    for config_key, config_value in new_chains[chain_name].items():
        if ((config_key not in ["notifier", "publisher"]) and
            ((config_key not in chains[chain_name].config) or
                (chains[chain_name].config[config_key] != config_value))):
            return False
    return True


class Chain:
    """A chain for managing new incoming files."""

    def __init__(self, name, config):
        """Set up the chain."""
        self.name = name
        self.config = config.copy()
        self.request_manager = None
        self.notifier = None
        self.needs_manager = "request_port" in self.config
        self.function_to_run = None

    def create_manager(self, manager):
        """Create a request manager."""
        if manager is None or not self.needs_manager:
            return
        try:
            self.request_manager = manager(int(self.config["request_port"]), self.config)
            LOGGER.debug("Created request manager on port %s", self.config["request_port"])
        except (KeyError, NameError):
            LOGGER.exception('In reading config')
        except ConfigError as err:
            LOGGER.error('Invalid config parameters in %s: %s', self.name, str(err))
            LOGGER.warning('Remove and skip %s', self.name)
            raise

    def create_notifier(self, notifier_builder, use_polling, function_to_run_on_matching_files):
        """Create a notifier and get the function."""
        if notifier_builder is None:
            notifier_builder = _get_notifier_builder(use_polling, self.config)

        self.function_to_run = partial(function_to_run_on_matching_files, chain_config=self.config)

        self.notifier = notifier_builder(self.function_to_run)

    def start(self):
        """Start the chain."""
        if self.request_manager is not None:
            self.request_manager.start()
        self.notifier.start()

    def stop(self):
        """Stop the chain."""
        self.notifier.stop()
        with suppress(AttributeError):
            self.notifier.join()
        if self.request_manager is not None:
            self.request_manager.stop()
            LOGGER.debug('Stopped the request manager')


def _add_chain(chains, chain_name, chain_config, manager):
    """Add a chain."""
    current_chain = Chain(chain_name, chain_config)
    current_chain.create_manager(manager)
    chains[chain_name] = current_chain
    return current_chain


def _get_notifier_builder(use_polling, chain_config):
    if 'origin' in chain_config:
        pattern = globify(chain_config["origin"])
        timeout = float(chain_config.get("watchdog_timeout", 1.))
        LOGGER.debug("Watchdog timeout: %.1f", timeout)
        if use_polling:
            LOGGER.info("Using Watchdog notifier")
            notifier_builder = partial(create_watchdog_polling_notifier, pattern, timeout=timeout)
        else:
            LOGGER.info("Using os-based notifier")
            notifier_builder = partial(create_watchdog_os_notifier, pattern, timeout=timeout)
    elif 'listen' in chain_config:
        notifier_builder = partial(create_posttroll_notifier, config=chain_config)

    return notifier_builder


def create_watchdog_polling_notifier(pattern, function_to_run_on_matching_files, timeout=1.0):
    """Create a notifier from the specified configuration attributes *attrs*."""
    observer_class = partial(PollingObserver, timeout=timeout)
    handler_class = WatchdogCreationHandler
    return create_watchdog_notifier(pattern, function_to_run_on_matching_files, observer_class, handler_class)


def create_watchdog_os_notifier(pattern, function_to_run_on_matching_files, timeout=1.0):
    """Create a notifier from the specified configuration attributes *attrs*."""
    observer_class = partial(Observer, timeout=timeout, generate_full_events=True)
    handler_class = WatchdogChangeHandler
    return create_watchdog_notifier(pattern, function_to_run_on_matching_files, observer_class, handler_class)


def create_watchdog_notifier(pattern, function_to_run_on_matching_files, observer_class, handler_class):
    """Create a watchdog notifier."""
    opath = os.path.dirname(pattern)
    observer = observer_class()
    handler = handler_class(function_to_run_on_matching_files, pattern)

    observer.schedule(handler, opath)

    return observer


def process_notification(notification, publisher, chain_config):
    """Publish what we have."""
    if isinstance(notification, Message):
        process_message(chain_config, notification, publisher)
    else:
        process_path(chain_config, notification, publisher)


def process_message(chain_config, msg, publisher):
    """Modify and publish a message."""
    LOGGER.debug('We have a match: %s', str(msg))
    info = _collect_message_info(msg)
    msg = Message(chain_config["topic"], msg.type, info)
    publisher.send(str(msg))
    _add_files_to_cache(msg, chain_config)
    LOGGER.debug("Message sent: %s", str(msg))


def _collect_message_info(msg, config):
    info = _collect_attribute_info(config)
    info.update(msg.data)
    info['request_address'] = config.get(
        "request_address", get_own_ip()) + ":" + config["request_port"]
    return info


def _add_files_to_cache(msg, config):
    with file_cache_lock:
        for filename in gen_dict_extract(msg.data, 'uid'):
            file_cache.appendleft(config["topic"] + '/' + filename)


def process_path(chain_config, path, publisher):
    """Create a message and publish a file."""
    if os.stat(path).st_size == 0:
        LOGGER.debug("Ignoring empty file: %s", path)
    else:
        LOGGER.debug('We have a match: %s', path)
        pathname = unpack(path, **chain_config)
        publish_file(path, publisher, chain_config, pathname)


def publish_file(orig_pathname, publisher, attrs, unpacked_pathname):
    """Publish a file."""
    if "request_port" in attrs:
        msg = create_message_with_request_info(unpacked_pathname, orig_pathname, attrs)
    else:
        msg = create_message_with_remote_fs_info(unpacked_pathname, orig_pathname, attrs)
    publisher.send(str(msg))
    LOGGER.debug("Message sent: %s", str(msg))


def create_message_with_request_info(pathname, orig_pathname, attrs):
    """Create a message containing request info."""
    info = _get_notify_message_info(attrs, orig_pathname, pathname)
    msg = Message(attrs["topic"], 'file', info)
    with file_cache_lock:
        file_cache.appendleft(attrs["topic"] + '/' + info["uid"])
    return msg


def create_message_with_remote_fs_info(pathname, orig_pathname, attrs):
    """Create a message containing remote filesystem info."""
    from pytroll_collectors.fsspec_to_message import \
        extract_local_files_to_message_for_remote_use
    msg = extract_local_files_to_message_for_remote_use(pathname, attrs['topic'], attrs.get("unpack"))
    info = _collect_attribute_info(attrs)
    info.update(parse(attrs["origin"], orig_pathname))
    msg.data.update(info)
    return msg


def _get_notify_message_info(attrs, orig_pathname, pathname):
    info = _collect_attribute_info(attrs)
    info.update(parse(attrs["origin"], orig_pathname))
    info['uri'] = pathname
    info['uid'] = os.path.basename(pathname)
    if "request_port" in attrs:
        info['request_address'] = attrs.get("request_address",
                                            get_own_ip()) + ":" + attrs["request_port"]
    return info


def create_posttroll_notifier(function_to_run, config):
    """Create a notifier listening to posttroll messages from *attrs*."""
    listener = Listener(function_to_run, config)

    return listener, None


def _disable_removed_chains(chains, new_chains):
    for key in (set(chains.keys()) - set(new_chains.keys())):
        chains[key].stop()
        del chains[key]
        LOGGER.debug("Removed %s", key)


def _process_old_files(old_glob, disable_backlog):
    if old_glob and not disable_backlog:
        time.sleep(3)
        for pattern, fun, _ in old_glob:
            process_old_files(pattern, fun)


def process_old_files(pattern, fun):
    """Process files from *pattern* with function *fun*."""
    fnames = glob.glob(pattern)
    if fnames:
        LOGGER.debug("Touching old files")
        for fname in fnames:
            if os.path.exists(fname):
                fun(fname)


def xrit(pathname, destination=None, cmd="./xRITDecompress"):
    """Unpacks xrit data."""
    opath, ofile = os.path.split(pathname)
    destination = destination or tempfile.gettempdir()
    dest_url = urlparse(destination)
    expected = os.path.join((destination or opath), ofile[:-2] + "__")
    if dest_url.scheme in ("", "file"):
        subprocess.check_output([cmd, pathname], cwd=(destination or opath))
    else:
        LOGGER.exception("Can not extract file %s to %s, destination "
                         "has to be local.", pathname, destination)
    LOGGER.info("Successfully extracted %s to %s", pathname, destination)
    return expected


BLOCK_SIZE = 1024


def bzip(origin, destination=None):
    """Unzip files."""
    ofile = os.path.split(origin)[1]
    destfile = os.path.join(destination or tempfile.gettempdir(), ofile[:-4])
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
            LOGGER.debug("Bunzipped %s to %s", origin, destfile)
        finally:
            orig.close()
    return destfile


def unpack(pathname,
           compression=None,
           working_directory=None,
           prog=None,
           delete=False,
           **kwargs):
    """Unpack *pathname*."""
    del kwargs
    if compression:
        try:
            unpack_fun = eval(compression)
            if prog is not None:
                new_path = unpack_fun(pathname, working_directory, prog)
            else:
                new_path = unpack_fun(pathname, working_directory)
        except Exception:
            LOGGER.exception("Could not decompress %s", pathname)
        else:
            if delete:
                os.remove(pathname)
            return new_path
    return pathname


def parse_args(args=None, default_port=9010):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-p", "--port",
                        help="The port to publish on. 9010 is the default",
                        default=default_port)
    parser.add_argument("--disable-backlog",
                        help="Disable glob and handling of backlog of files at start/restart",
                        action='store_true')
    parser.add_argument("-w", "--watchdog", default=False, action="store_true",
                        help="Use Watchdog polling instead of os-based notifying")
    add_logging_options_to_parser(parser, legacy=True)
    return parser.parse_args(args)
