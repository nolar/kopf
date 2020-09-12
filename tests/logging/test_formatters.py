import json
import logging.handlers

import pytest

from kopf.engines.loggers import LocalObjectLogger, ObjectJsonFormatter, \
                                 ObjectPrefixingJsonFormatter, ObjectPrefixingTextFormatter, \
                                 ObjectTextFormatter
from kopf.structs.bodies import Body


@pytest.fixture()
def ns_body():
    return Body({
        'kind': 'kind1',
        'apiVersion': 'api1/v1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'namespace1'},
    })


@pytest.fixture()
def cluster_body():
    return Body({
        'kind': 'kind1',
        'apiVersion': 'api1/v1',
        'metadata': {'uid': 'uid1', 'name': 'name1'},
    })


@pytest.fixture()
def ns_record(settings, ns_body):
    handler = logging.handlers.BufferingHandler(capacity=100)
    logger = LocalObjectLogger(body=ns_body, settings=settings)  # to avoid k8s-posting
    logger.logger.addHandler(handler)
    logger.info("hello")
    return handler.buffer[0]


@pytest.fixture()
def cluster_record(settings, cluster_body):
    handler = logging.handlers.BufferingHandler(capacity=100)
    logger = LocalObjectLogger(body=cluster_body, settings=settings)  # to avoid k8s-posting
    logger.logger.addHandler(handler)
    logger.info("hello")
    return handler.buffer[0]


def test_prefixing_text_formatter_adds_prefixes_when_namespaced(ns_record):
    formatter = ObjectPrefixingTextFormatter()
    formatted = formatter.format(ns_record)
    assert formatted == '[namespace1/name1] hello'


def test_prefixing_text_formatter_adds_prefixes_when_cluster(cluster_record):
    formatter = ObjectPrefixingTextFormatter()
    formatted = formatter.format(cluster_record)
    assert formatted == '[name1] hello'


def test_prefixing_json_formatter_adds_prefixes_when_namespaced(ns_record):
    formatter = ObjectPrefixingJsonFormatter()
    formatted = formatter.format(ns_record)
    decoded = json.loads(formatted)
    assert decoded['message'] == '[namespace1/name1] hello'


def test_prefixing_json_formatter_adds_prefixes_when_clustered(cluster_record):
    formatter = ObjectPrefixingJsonFormatter()
    formatted = formatter.format(cluster_record)
    decoded = json.loads(formatted)
    assert decoded['message'] == '[name1] hello'


def test_regular_text_formatter_omits_prefixes(ns_record):
    formatter = ObjectTextFormatter()
    formatted = formatter.format(ns_record)
    assert formatted == 'hello'


def test_regular_json_formatter_omits_prefixes(ns_record):
    formatter = ObjectJsonFormatter()
    formatted = formatter.format(ns_record)
    decoded = json.loads(formatted)
    assert decoded['message'] == 'hello'


@pytest.mark.parametrize('cls', [ObjectJsonFormatter, ObjectPrefixingJsonFormatter])
@pytest.mark.parametrize('levelno, expected_severity', [
    (0,  'debug'),
    (logging.DEBUG - 1, 'debug'),
    (logging.DEBUG, 'debug'),
    (logging.DEBUG + 1, 'info'),
    (logging.INFO - 1, 'info'),
    (logging.INFO, 'info'),
    (logging.INFO + 1, 'warn'),
    (logging.WARNING - 1, 'warn'),
    (logging.WARNING, 'warn'),
    (logging.WARNING + 1, 'error'),
    (logging.ERROR - 1, 'error'),
    (logging.ERROR, 'error'),
    (logging.ERROR + 1, 'fatal'),
    (logging.FATAL - 1, 'fatal'),
    (logging.FATAL, 'fatal'),
    (logging.FATAL + 1, 'fatal'),
    (999, 'fatal'),
])
def test_json_formatters_add_severity(ns_record, cls, levelno, expected_severity):
    ns_record.levelno = levelno
    ns_record.levelname = 'must-be-irrelevant'
    formatter = cls()
    formatted = formatter.format(ns_record)
    decoded = json.loads(formatted)
    assert 'severity' in decoded
    assert decoded['severity'] == expected_severity


@pytest.mark.parametrize('cls', [ObjectJsonFormatter, ObjectPrefixingJsonFormatter])
def test_json_formatters_add_refkey_with_default_key(ns_record, cls):
    formatter = cls()
    formatted = formatter.format(ns_record)
    decoded = json.loads(formatted)
    assert 'object' in decoded
    assert decoded['object'] == {
        'uid': 'uid1',
        'name': 'name1',
        'namespace': 'namespace1',
        'apiVersion': 'api1/v1',
        'kind': 'kind1',
    }


@pytest.mark.parametrize('cls', [ObjectJsonFormatter, ObjectPrefixingJsonFormatter])
def test_json_formatters_add_refkey_with_custom_key(ns_record, cls):
    formatter = cls(refkey='k8s-obj')
    formatted = formatter.format(ns_record)
    decoded = json.loads(formatted)
    assert 'k8s-obj' in decoded
    assert decoded['k8s-obj'] == {
        'uid': 'uid1',
        'name': 'name1',
        'namespace': 'namespace1',
        'apiVersion': 'api1/v1',
        'kind': 'kind1',
    }
