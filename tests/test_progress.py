import copy
import datetime
from unittest.mock import Mock

import freezegun
import pytest

from kopf.structs.status import (
    is_started,
    is_sleeping,
    is_awakened,
    is_finished,
    get_start_time,
    get_awake_time,
    get_retry_count,
    set_start_time,
    set_awake_time,
    set_retry_time,
    store_failure,
    store_success,
    store_result,
    purge_progress,
)

# Timestamps: time zero (0), before (B), after (A), and time zero+1s (1).
TSB = datetime.datetime(2020, 12, 31, 23, 59, 59, 000000)
TS0 = datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)
TS1 = datetime.datetime(2021,  1,  1, 00, 00, 00, 123456)
TSA = datetime.datetime(2020, 12, 31, 23, 59, 59, 999999)
TSB_ISO = '2020-12-31T23:59:59.000000'
TS0_ISO = '2020-12-31T23:59:59.123456'
TS1_ISO = '2021-01-01T00:00:00.123456'
TSA_ISO = '2020-12-31T23:59:59.999999'


@pytest.fixture()
def handler():
    return Mock(id='some-id', spec_set=['id'])


@pytest.mark.parametrize('expected, body', [
    (False, {}),
    (False, {'status': {}}),
    (False, {'status': {'kopf': {}}}),
    (False, {'status': {'kopf': {'progress': {}}}}),
    (False, {'status': {'kopf': {'progress': {'etc-id': {}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {}}}}}),
])
def test_is_started(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = is_started(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('expected, body', [
    (False, {}),
    (False, {'status': {}}),
    (False, {'status': {'kopf': {}}}),
    (False, {'status': {'kopf': {'progress': {}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': False}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': False}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'success': True}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'failure': True}}}}}),
])
def test_is_finished(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = is_finished(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('expected, body', [

    # Everything that is finished is not sleeping, no matter the sleep/awake field.
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': True}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': True}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': True, 'delayed': TS0_ISO}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': True, 'delayed': TS0_ISO}}}}}),

    # Everything with no sleep/awake field set is not sleeping either.
    (False, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': None, 'delayed': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': None, 'delayed': None}}}}}),

    # When not finished and has awake time, the output depends on the relation to "now".
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO, 'success': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO, 'failure': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO, 'success': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO, 'failure': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO, 'success': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO, 'failure': None}}}}}),
])
@freezegun.freeze_time(TS0)
def test_is_sleeping(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = is_sleeping(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('expected, body', [

    # Everything that is finished never awakens, no matter the sleep/awake field.
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': True}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': True}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'success': True, 'delayed': TS0_ISO}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'failure': True, 'delayed': TS0_ISO}}}}}),

    # Everything with no sleep/awake field is not sleeping, thus by definition is awake.
    (True , {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'success': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'failure': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'success': None, 'delayed': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'failure': None, 'delayed': None}}}}}),

    # When not finished and has awake time, the output depends on the relation to "now".
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO, 'success': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO, 'failure': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO, 'success': None}}}}}),
    (True , {'status': {'kopf': {'progress': {'some-id': {'delayed': TSB_ISO, 'failure': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO, 'success': None}}}}}),
    (False, {'status': {'kopf': {'progress': {'some-id': {'delayed': TSA_ISO, 'failure': None}}}}}),
])
@freezegun.freeze_time(TS0)
def test_is_awakened(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = is_awakened(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('expected, body', [
    (None, {}),
    (None, {'status': {}}),
    (None, {'status': {'kopf': {}}}),
    (None, {'status': {'kopf': {'progress': {}}}}),
    (None, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (None, {'status': {'kopf': {'progress': {'some-id': {'delayed': None}}}}}),
    (TS0, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO}}}}}),
])
def test_get_awake_time(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = get_awake_time(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('expected, body, patch', [
    (None, {}, {}),
    (None, {'status': {}}, {}),
    (None, {'status': {'kopf': {}}}, {}),
    (None, {'status': {'kopf': {'progress': {}}}}, {}),
    (None, {'status': {'kopf': {'progress': {'some-id': {}}}}}, {}),
    (None, {'status': {'kopf': {'progress': {'some-id': {'started': None}}}}}, {}),
    (TS0, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}, {}),
    (None, {}, {'status': {}}),
    (None, {}, {'status': {'kopf': {}}}),
    (None, {}, {'status': {'kopf': {'progress': {}}}}),
    (None, {}, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (None, {}, {'status': {'kopf': {'progress': {'some-id': {'started': None}}}}}),
    (TS0, {}, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}),
    (TSB,  # the patch has priority
     {'status': {'kopf': {'progress': {'some-id': {'started': TSA_ISO}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'started': TSB_ISO}}}}}),
])
def test_get_start_time(handler, expected, body, patch):
    origbody = copy.deepcopy(body)
    origpatch = copy.deepcopy(patch)
    result = get_start_time(body=body, patch=patch, handler=handler)
    assert result == expected
    assert body == origbody  # not modified
    assert patch == origpatch  # not modified


@pytest.mark.parametrize('expected, body', [
    (0, {}),
    (0, {'status': {}}),
    (0, {'status': {'kopf': {'progress': {}}}}),
    (0, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (0, {'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}),
    (6, {'status': {'kopf': {'progress': {'some-id': {'retries': 6}}}}}),
])
def test_get_retry_count(handler, expected, body):
    origbody = copy.deepcopy(body)
    result = get_retry_count(body=body, handler=handler)
    assert result == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('body, expected', [
    ({}, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}),
])
@freezegun.freeze_time(TS0)
def test_set_start_time(handler, expected, body):
    origbody = copy.deepcopy(body)
    patch = {}
    set_start_time(body=body, patch=patch, handler=handler)
    assert patch == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('body, delay, expected', [
    ({}, None, {'status': {'kopf': {'progress': {'some-id': {'delayed': None}}}}}),
    ({}, 0, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO}}}}}),
    ({}, 1, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS1_ISO}}}}}),
])
@freezegun.freeze_time(TS0)
def test_set_awake_time(handler, expected, body, delay):
    origbody = copy.deepcopy(body)
    patch = {}
    set_awake_time(body=body, patch=patch, handler=handler, delay=delay)
    assert patch == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('body, delay, expected', [
    ({}, None,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': None}}}}}),
    ({}, 0,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': TS0_ISO}}}}}),
    ({}, 1,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': TS1_ISO}}}}}),

    ({'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}, None,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': None}}}}}),
    ({'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}, 0,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': TS0_ISO}}}}}),
    ({'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}, 1,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 1, 'delayed': TS1_ISO}}}}}),

    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}, None,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 6, 'delayed': None}}}}}),
    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}, 0,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 6, 'delayed': TS0_ISO}}}}}),
    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}, 1,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 6, 'delayed': TS1_ISO}}}}}),
])
@freezegun.freeze_time(TS0)
def test_set_retry_time(handler, expected, body, delay):
    origbody = copy.deepcopy(body)
    patch = {}
    set_retry_time(body=body, patch=patch, handler=handler, delay=delay)
    assert patch == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('body, expected', [
    ({},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'failure': True,
                                                   'retries': 1,
                                                   'message': 'some-error'}}}}}),

    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'failure': True,
                                                   'retries': 6,
                                                   'message': 'some-error'}}}}}),
])
@freezegun.freeze_time(TS0)
def test_store_failure(handler, expected, body):
    origbody = copy.deepcopy(body)
    patch = {}
    store_failure(body=body, patch=patch, handler=handler, exc=Exception("some-error"))
    assert patch == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('result, body, expected', [

    # With no result, it updates only the progress.
    (None,
     {},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': True,
                                                   'retries': 1,
                                                   'message': None}}}}}),
    (None,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': True,
                                                   'retries': 6,
                                                   'message': None}}}}}),

    # With the result, it updates also the status.
    ({'field': 'value'},
     {},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': True,
                                                   'retries': 1,
                                                   'message': None}}},
                 'some-id': {'field': 'value'}}}),
    ({'field': 'value'},
     {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': True,
                                                   'retries': 6,
                                                   'message': None}}},
                 'some-id': {'field': 'value'}}}),
])
@freezegun.freeze_time(TS0)
def test_store_success(handler, expected, body, result):
    origbody = copy.deepcopy(body)
    patch = {}
    store_success(body=body, patch=patch, handler=handler, result=result)
    assert patch == expected
    assert body == origbody  # not modified



@pytest.mark.parametrize('result, expected', [
    (None,
     {}),
    ({'field': 'value'},
     {'status': {'some-id': {'field': 'value'}}}),
    ('string',
     {'status': {'some-id': 'string'}}),
])
def test_store_result(handler, expected, result):
    patch = {}
    store_result(patch=patch, handler=handler, result=result)
    assert patch == expected


@pytest.mark.parametrize('body', [
    ({}),
    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
])
def test_purge_progress(body):
    origbody = copy.deepcopy(body)
    patch = {}
    purge_progress(body=body, patch=patch)
    assert patch == {'status': {'kopf': {'progress': None}}}
    assert body == origbody  # not modified
