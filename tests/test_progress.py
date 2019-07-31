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


@pytest.mark.parametrize('body', [
    {},
    {'status': {}},
    {'status': {'kopf': {}}},
    {'status': {'kopf': {'progress': {}}}},
    {'status': {'kopf': {'progress': {'some-id': {}}}}},
])
def test_is_finished_with_partial_status_remains_readonly(handler, body):
    origbody = copy.deepcopy(body)
    result = is_finished(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result
    assert body == origbody  # not modified


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
def test_is_finished_when_not_finished(handler, finish_field, finish_value):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_finished(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


@pytest.mark.parametrize('finish_value', [True, 'good'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
def test_is_finished_when_finished(handler, finish_field, finish_value):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_finished(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert result


@pytest.mark.parametrize('body', [
    {},
    {'status': {}},
    {'status': {'kopf': {}}},
    {'status': {'kopf': {'progress': {}}}},
    {'status': {'kopf': {'progress': {'some-id': {}}}}},
])
def test_is_sleeping_with_partial_status_remains_readonly(handler, body):
    origbody = copy.deepcopy(body)
    result = is_finished(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result
    assert body == origbody  # not modified


@pytest.mark.parametrize('finish_value', [True, 'good'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({}, id='delayed-empty'),
    pytest.param({'delayed': None}, id='delayed-none'),
    pytest.param({'delayed': TSB_ISO}, id='delayed-before'),
    pytest.param({'delayed': TS0_ISO}, id='delayed-exact'),
    pytest.param({'delayed': TS1_ISO}, id='delayed-onesec'),
    pytest.param({'delayed': TSA_ISO}, id='delayed-after'),
])
@freezegun.freeze_time(TS0)
def test_is_sleeping_when_finished_regardless_of_delay(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_sleeping(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({}, id='delayed-empty'),
    pytest.param({'delayed': None}, id='delayed-none'),
])
@freezegun.freeze_time(TS0)
def test_is_sleeping_when_not_finished_and_not_delayed(
        handler, delayed_body, finish_field, finish_value):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_sleeping(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({'delayed': TSB_ISO}, id='delayed-before'),
    pytest.param({'delayed': TS0_ISO}, id='delayed-exact'),
])
@freezegun.freeze_time(TS0)
def test_is_sleeping_when_not_finished_and_delayed_until_before_now(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_sleeping(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({'delayed': TS1_ISO}, id='delayed-onesec'),
    pytest.param({'delayed': TSA_ISO}, id='delayed-after'),
])
@freezegun.freeze_time(TS0)
def test_is_sleeping_when_not_finished_and_delayed_until_after_now(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_sleeping(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert result


@pytest.mark.parametrize('body', [
    {},
    {'status': {}},
    {'status': {'kopf': {}}},
    {'status': {'kopf': {'progress': {}}}},
    {'status': {'kopf': {'progress': {'some-id': {}}}}},
])
def test_is_awakened_with_partial_status_remains_readonly(handler, body):
    origbody = copy.deepcopy(body)
    result = is_awakened(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert result
    assert body == origbody  # not modified


@pytest.mark.parametrize('finish_value', [True, 'good'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({}, id='delayed-empty'),
    pytest.param({'delayed': None}, id='delayed-none'),
    pytest.param({'delayed': TSB_ISO}, id='delayed-before'),
    pytest.param({'delayed': TS0_ISO}, id='delayed-exact'),
    pytest.param({'delayed': TS1_ISO}, id='delayed-onesec'),
    pytest.param({'delayed': TSA_ISO}, id='delayed-after'),
])
@freezegun.freeze_time(TS0)
def test_is_awakened_when_finished_regardless_of_delay(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_awakened(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({}, id='delayed-empty'),
    pytest.param({'delayed': None}, id='delayed-none'),
])
@freezegun.freeze_time(TS0)
def test_is_awakened_when_not_finished_and_not_delayed(
        handler, delayed_body, finish_field, finish_value):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_awakened(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({'delayed': TSB_ISO}, id='delayed-before'),
    pytest.param({'delayed': TS0_ISO}, id='delayed-exact'),
])
@freezegun.freeze_time(TS0)
def test_is_awakened_when_not_finished_and_delayed_until_before_now(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_awakened(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert result


@pytest.mark.parametrize('finish_value', [None, False, 'bad'])
@pytest.mark.parametrize('finish_field', ['failure', 'success'])
@pytest.mark.parametrize('delayed_body', [
    pytest.param({'delayed': TS1_ISO}, id='delayed-onesec'),
    pytest.param({'delayed': TSA_ISO}, id='delayed-after'),
])
@freezegun.freeze_time(TS0)
def test_is_awakened_when_not_finished_and_delayed_until_after_now(
        handler, finish_field, finish_value, delayed_body):
    body = {'status': {'kopf': {'progress': {'some-id': {}}}}}
    body['status']['kopf']['progress']['some-id'].update(delayed_body)
    body['status']['kopf']['progress']['some-id'][finish_field] = finish_value
    result = is_awakened(body=body, digest='good', handler=handler)
    assert isinstance(result, bool)
    assert not result


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
                                                   'failure': 'digest',
                                                   'retries': 1,
                                                   'message': 'some-error'}}}}}),

    ({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'failure': 'digest',
                                                   'retries': 6,
                                                   'message': 'some-error'}}}}}),
])
@freezegun.freeze_time(TS0)
def test_store_failure(handler, expected, body):
    origbody = copy.deepcopy(body)
    patch = {}
    store_failure(body=body, patch=patch, digest='digest',
                  handler=handler, exc=Exception("some-error"))
    assert patch == expected
    assert body == origbody  # not modified


@pytest.mark.parametrize('result, body, expected', [

    # With no result, it updates only the progress.
    (None,
     {},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': 'digest',
                                                   'retries': 1,
                                                   'message': None}}}}}),
    (None,
     {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': 'digest',
                                                   'retries': 6,
                                                   'message': None}}}}}),

    # With the result, it updates also the status.
    ({'field': 'value'},
     {},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': 'digest',
                                                   'retries': 1,
                                                   'message': None}}},
                 'some-id': {'field': 'value'}}}),
    ({'field': 'value'},
     {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}},
     {'status': {'kopf': {'progress': {'some-id': {'stopped': TS0_ISO,
                                                   'success': 'digest',
                                                   'retries': 6,
                                                   'message': None}}},
                 'some-id': {'field': 'value'}}}),
])
@freezegun.freeze_time(TS0)
def test_store_success(handler, expected, body, result):
    origbody = copy.deepcopy(body)
    patch = {}
    store_success(body=body, patch=patch, digest='digest',
                  handler=handler, result=result)
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
