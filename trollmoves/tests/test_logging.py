"""Tests for logging utilities."""
import argparse
import logging
import logging.handlers
import os

import pytest

from trollmoves.logging import add_logging_options_to_parser, setup_logging

log_config = """version: 1
handlers:
  null_handler:
    class: logging.NullHandler
    level: DEBUG
loggers:
  simple_example:
    level: DEBUG
    handlers: [null_handler]
    propagate: no
root:
  level: DEBUG
  handlers: [null_handler]
"""


def test_logging_options_are_added(tmp_path):
    """Test that logging options are added to the parser."""
    config_file = os.fspath(tmp_path / "my_log_config")
    with open(config_file, "w") as fd:
        fd.write(log_config)
    cmd_args = _create_arg_parser(["-c", config_file])
    assert os.path.basename(cmd_args.log_config) == "my_log_config"


def _create_arg_parser(args, legacy=False):
    parser = argparse.ArgumentParser()
    add_logging_options_to_parser(parser, legacy=legacy)
    cmd_args = parser.parse_args(args)
    return cmd_args


def test_logging_without_options_creates_a_stream_handler():
    """Test that logging without options creates a stream handler."""
    cmd_args = _create_arg_parser("", legacy=True)

    logger = setup_logging("my_logger", cmd_args)
    assert any(isinstance(handler, logging.StreamHandler)
               for handler in logger.handlers + logger.parent.handlers)


def test_logger_has_right_name():
    """Test that the logger has the provided name."""
    logger = setup_logging("some_logger")
    assert logger.name == "some_logger"


def test_logger_with_options_applies_config(tmp_path):
    """Test that logger with options applies the passed config."""
    handlers = logging.getLogger("").handlers.copy()
    config_file = os.fspath(tmp_path / "my_log_config")
    logger = logger_from_config_file(config_file)

    handlers = list(set(logger.handlers + logger.parent.handlers) - set(handlers))
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.NullHandler)


def logger_from_config_file(config_file):
    """Create a logger from a config file."""
    with open(config_file, "w") as fd:
        fd.write(log_config)
    cmd_args = _create_arg_parser(["-c", config_file])
    logger = setup_logging("my_logger", cmd_args)
    return logger


def test_unknown_log_config_crashes(tmp_path):
    """Test that unknown log config crashes."""
    config_file = os.fspath(tmp_path / "unknown_config")
    cmd_args = _create_arg_parser(["-c", config_file])
    with pytest.raises(FileNotFoundError):
        setup_logging("my_logger", cmd_args)


def test_legacy_arguments_work(tmp_path):
    """Test that legacy arguments work."""
    handlers = logging.getLogger("").handlers.copy()
    parser = argparse.ArgumentParser()
    add_logging_options_to_parser(parser, legacy=True)
    log_file = tmp_path / "log.txt"
    args = ["-l", os.fspath(log_file), "-v"]
    cmd_args = parser.parse_args(args)
    logger = setup_logging("my_logger", cmd_args)
    handlers = list(set(logger.handlers + logger.parent.handlers) - set(handlers))
    assert len(handlers) == 1
    handler = handlers[0]
    assert isinstance(handler, logging.handlers.TimedRotatingFileHandler)
    assert handler.backupCount == 7
    assert handler.when.lower() == "midnight"
    assert handler.level == logging.DEBUG
    assert log_file.exists()


def test_legacy_arguments_raise_warnings(tmp_path):
    """Test that legacy arguments raise warnings."""
    parser = argparse.ArgumentParser()
    add_logging_options_to_parser(parser, legacy=True)
    log_file = tmp_path / "log.txt"
    args = ["-l", os.fspath(log_file)]
    with pytest.warns(RuntimeWarning, match="deprecation"):
        parser.parse_args(args)
    args = ["-v"]
    with pytest.warns(RuntimeWarning, match="deprecation"):
        parser.parse_args(args)


def test_legacy_activation_still_uses_config_first(tmp_path):
    """Test that legacy arguments do now override config-file."""
    handlers = logging.getLogger("").handlers.copy()
    parser = argparse.ArgumentParser()
    add_logging_options_to_parser(parser, legacy=True)

    config_file = os.fspath(tmp_path / "my_log_config")
    with open(config_file, "w") as fd:
        fd.write(log_config)

    log_file = tmp_path / "log.txt"
    cmd_args = parser.parse_args(["-l", os.fspath(log_file), "-c", config_file, "-v"])

    logger = setup_logging("my_logger", cmd_args)

    handlers = list(set(logger.handlers + logger.parent.handlers) - set(handlers))
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.NullHandler)
    # The log file of '-l' option is overridden by the config file
    assert not log_file.exists()
