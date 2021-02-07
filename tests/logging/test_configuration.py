import logging.handlers
from typing import Collection

import pytest

from kopf.engines.loggers import LogFormat, ObjectFormatter, ObjectJsonFormatter, \
                                 ObjectPrefixingJsonFormatter, ObjectPrefixingTextFormatter, \
                                 ObjectTextFormatter, configure


@pytest.fixture(autouse=True)
def _clear_own_handlers():
    logger = logging.getLogger()
    logger.handlers[:] = [
        handler for handler in logger.handlers
        if not isinstance(handler, logging.StreamHandler) or
           not isinstance(handler.formatter, ObjectFormatter)
    ]
    original_handlers = logger.handlers[:]
    yield
    logger.handlers[:] = original_handlers


def _get_own_handlers(logger: logging.Logger) -> Collection[logging.Handler]:
    return [
        handler for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler) and
           isinstance(handler.formatter, ObjectFormatter)
    ]


def test_own_formatter_is_used():
    configure()
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1


@pytest.mark.parametrize('log_format', [LogFormat.FULL, LogFormat.PLAIN, '%(message)s'])
def test_formatter_nonprefixed_text(log_format):
    configure(log_format=log_format, log_prefix=False)
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1
    assert type(own_handlers[0].formatter) is ObjectTextFormatter


@pytest.mark.parametrize('log_format', [LogFormat.FULL, LogFormat.PLAIN, '%(message)s'])
def test_formatter_prefixed_text(log_format):
    configure(log_format=log_format, log_prefix=True)
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1
    assert type(own_handlers[0].formatter) is ObjectPrefixingTextFormatter


@pytest.mark.parametrize('log_format', [LogFormat.JSON])
def test_formatter_nonprefixed_json(log_format):
    configure(log_format=log_format, log_prefix=False)
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1
    assert type(own_handlers[0].formatter) is ObjectJsonFormatter


@pytest.mark.parametrize('log_format', [LogFormat.JSON])
def test_formatter_prefixed_json(log_format):
    configure(log_format=log_format, log_prefix=True)
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1
    assert type(own_handlers[0].formatter) is ObjectPrefixingJsonFormatter


@pytest.mark.parametrize('log_format', [LogFormat.JSON])
def test_json_has_no_prefix_by_default(log_format):
    configure(log_format=log_format, log_prefix=None)
    logger = logging.getLogger()
    own_handlers = _get_own_handlers(logger)
    assert len(own_handlers) == 1
    assert type(own_handlers[0].formatter) is ObjectJsonFormatter


def test_error_on_unknown_formatter():
    with pytest.raises(ValueError):
        configure(log_format=object())


@pytest.mark.parametrize('verbose, debug, quiet, expected_level', [
    (None, None, None, logging.INFO),
    (True, None, None, logging.DEBUG),
    (None, True, None, logging.DEBUG),
    (True, True, True, logging.DEBUG),
    (None, None, True, logging.WARNING),
])
def test_levels(verbose, debug, quiet, expected_level):
    configure(verbose=verbose, debug=debug, quiet=quiet)
    logger = logging.getLogger()
    assert logger.level == expected_level
