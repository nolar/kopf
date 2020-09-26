import datetime
from unittest.mock import Mock

import freezegun
import pytest

from kopf.storage.progress import SmartProgressStorage, StatusProgressStorage
from kopf.storage.states import HandlerOutcome, State, deliver_results
from kopf.structs.bodies import Body
from kopf.structs.patches import Patch

# Timestamps: time zero (0), before (B), after (A), and time zero+1s (1).
TSB = datetime.datetime(2020, 12, 31, 23, 59, 59, 000000)
TS0 = datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)
TS1 = datetime.datetime(2021,  1,  1, 00, 00, 00, 123456)
TSA = datetime.datetime(2020, 12, 31, 23, 59, 59, 999999)
TSB_ISO = '2020-12-31T23:59:59.000000'
TS0_ISO = '2020-12-31T23:59:59.123456'
TS1_ISO = '2021-01-01T00:00:00.123456'
TSA_ISO = '2020-12-31T23:59:59.999999'
ZERO_DELTA = datetime.timedelta(seconds=0)


# Use only the status-populating storages, to keep the tests with their original assertions.
# The goal is to test the states, not the storages. The storages are tested in test_storages.py.
@pytest.fixture(params=[StatusProgressStorage, SmartProgressStorage])
def storage(request):
    return request.param()


@pytest.fixture()
def handler():
    return Mock(id='some-id', spec_set=['id'])


@freezegun.freeze_time(TS0)
def test_created_empty_from_scratch(storage, handler):
    state = State.from_scratch()
    assert len(state) == 0


@pytest.mark.parametrize('expected, body', [
    (TS0_ISO, {}),
    (TS0_ISO, {'status': {}}),
    (TS0_ISO, {'status': {'kopf': {}}}),
    (TS0_ISO, {'status': {'kopf': {'progress': {}}}}),
])
@freezegun.freeze_time(TS0)
def test_created_empty_from_empty_storage(storage, handler, body, expected):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    assert len(state) == 0


@freezegun.freeze_time(TS0)
def test_started_from_scratch(storage, handler):
    patch = Patch()
    state = State.from_scratch()
    state = state.with_handlers([handler])
    state.store(body=Body({}), patch=patch, storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['started'] == TS0_ISO


@pytest.mark.parametrize('expected, body', [
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': None}}}}}),
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}),
    (TSB_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TSB_ISO}}}}}),
    (TSA_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TSA_ISO}}}}}),
])
@freezegun.freeze_time(TS0)
def test_started_from_storage(storage, handler, body, expected):
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.store(body=Body({}), patch=patch, storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['started'] == expected


@pytest.mark.parametrize('expected, body', [
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': None}}}}}),
    (TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}),
    (TSB_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TSB_ISO}}}}}),
    (TSA_ISO, {'status': {'kopf': {'progress': {'some-id': {'started': TSA_ISO}}}}}),
])
def test_started_from_storage_is_preferred_over_from_scratch(storage, handler, body, expected):
    with freezegun.freeze_time(TS0):
        state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    with freezegun.freeze_time(TS1):
        state = state.with_handlers([handler])
    patch = Patch()
    state.store(body=Body({}), patch=patch, storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['started'] == expected


@pytest.mark.parametrize('expected, body', [
    (ZERO_DELTA, {}),
    (ZERO_DELTA, {'status': {}}),
    (ZERO_DELTA, {'status': {'kopf': {}}}),
    (ZERO_DELTA, {'status': {'kopf': {'progress': {}}}}),
    (ZERO_DELTA, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (ZERO_DELTA, {'status': {'kopf': {'progress': {'some-id': {'started': None}}}}}),
    (ZERO_DELTA, {'status': {'kopf': {'progress': {'some-id': {'started': TS0_ISO}}}}}),
    (TS0 - TSB, {'status': {'kopf': {'progress': {'some-id': {'started': TSB_ISO}}}}}),
    (TS0 - TSA, {'status': {'kopf': {'progress': {'some-id': {'started': TSA_ISO}}}}}),
])
@freezegun.freeze_time(TS0)
def test_runtime(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].runtime
    assert result == expected


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
def test_finished_flag(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].finished
    assert result == expected


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
def test_sleeping_flag(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].sleeping
    assert result == expected


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
def test_awakened_flag(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].awakened
    assert result == expected


@pytest.mark.parametrize('expected, body', [
    (None, {}),
    (None, {'status': {}}),
    (None, {'status': {'kopf': {}}}),
    (None, {'status': {'kopf': {'progress': {}}}}),
    (None, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (None, {'status': {'kopf': {'progress': {'some-id': {'delayed': None}}}}}),
    (TS0, {'status': {'kopf': {'progress': {'some-id': {'delayed': TS0_ISO}}}}}),
])
def test_awakening_time(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].delayed
    assert result == expected


@pytest.mark.parametrize('expected, body', [
    (0, {}),
    (0, {'status': {}}),
    (0, {'status': {'kopf': {'progress': {}}}}),
    (0, {'status': {'kopf': {'progress': {'some-id': {}}}}}),
    (0, {'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}),
    (6, {'status': {'kopf': {'progress': {'some-id': {'retries': 6}}}}}),
])
def test_get_retry_count(storage, handler, expected, body):
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    result = state[handler.id].retries
    assert result == expected


@pytest.mark.parametrize('body, delay, expected', [
    ({}, None, None),
    ({}, 0, TS0_ISO),
    ({}, 1, TS1_ISO),
])
@freezegun.freeze_time(TS0)
def test_set_awake_time(storage, handler, expected, body, delay):
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: HandlerOutcome(final=False, delay=delay)})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id'].get('delayed') == expected


@pytest.mark.parametrize('expected_retries, expected_delayed, delay, body', [
    (1, None, None, {}),
    (1, TS0_ISO, 0, {}),
    (1, TS1_ISO, 1, {}),

    (1, None, None, {'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}),
    (1, TS0_ISO, 0, {'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}),
    (1, TS1_ISO, 1, {'status': {'kopf': {'progress': {'some-id': {'retries': None}}}}}),

    (6, None, None, {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
    (6, TS0_ISO, 0, {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
    (6, TS1_ISO, 1, {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
])
@freezegun.freeze_time(TS0)
def test_set_retry_time(storage, handler, expected_retries, expected_delayed, body, delay):
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: HandlerOutcome(final=False, delay=delay)})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['retries'] == expected_retries
    assert patch['status']['kopf']['progress']['some-id']['delayed'] == expected_delayed


def test_subrefs_added_to_empty_state(storage, handler):
    body = {}
    patch = Patch()
    outcome_subrefs = ['sub2/b', 'sub2/a', 'sub2', 'sub1', 'sub3']
    expected_subrefs = ['sub1', 'sub2', 'sub2/a', 'sub2/b', 'sub3']
    outcome = HandlerOutcome(final=True, subrefs=outcome_subrefs)
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: outcome})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['subrefs'] == expected_subrefs


def test_subrefs_added_to_preexisting_subrefs(storage, handler):
    body = {'status': {'kopf': {'progress': {'some-id': {'subrefs': ['sub9/2', 'sub9/1']}}}}}
    patch = Patch()
    outcome_subrefs = ['sub2/b', 'sub2/a', 'sub2', 'sub1', 'sub3']
    expected_subrefs = ['sub1', 'sub2', 'sub2/a', 'sub2/b', 'sub3', 'sub9/1', 'sub9/2']
    outcome = HandlerOutcome(final=True, subrefs=outcome_subrefs)
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: outcome})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['subrefs'] == expected_subrefs


def test_subrefs_ignored_when_not_specified(storage, handler):
    body = {}
    patch = Patch()
    outcome = HandlerOutcome(final=True, subrefs=[])
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: outcome})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['subrefs'] is None


@pytest.mark.parametrize('expected_retries, expected_stopped, body', [
    (1, TS0_ISO, {}),
    (6, TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
])
@freezegun.freeze_time(TS0)
def test_store_failure(storage, handler, expected_retries, expected_stopped, body):
    error = Exception('some-error')
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: HandlerOutcome(final=True, exception=error)})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['success'] is False
    assert patch['status']['kopf']['progress']['some-id']['failure'] is True
    assert patch['status']['kopf']['progress']['some-id']['retries'] == expected_retries
    assert patch['status']['kopf']['progress']['some-id']['stopped'] == expected_stopped
    assert patch['status']['kopf']['progress']['some-id']['message'] == 'some-error'


@pytest.mark.parametrize('expected_retries, expected_stopped, body', [
    (1, TS0_ISO, {}),
    (6, TS0_ISO, {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}),
])
@freezegun.freeze_time(TS0)
def test_store_success(storage, handler, expected_retries, expected_stopped, body):
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state = state.with_handlers([handler])
    state = state.with_outcomes(outcomes={handler.id: HandlerOutcome(final=True)})
    state.store(patch=patch, body=Body(body), storage=storage)
    assert patch['status']['kopf']['progress']['some-id']['success'] is True
    assert patch['status']['kopf']['progress']['some-id']['failure'] is False
    assert patch['status']['kopf']['progress']['some-id']['retries'] == expected_retries
    assert patch['status']['kopf']['progress']['some-id']['stopped'] == expected_stopped
    assert patch['status']['kopf']['progress']['some-id']['message'] is None


@pytest.mark.parametrize('result, expected_patch', [
    (None, {}),
    ('string', {'status': {'some-id': 'string'}}),
    ({'field': 'value'}, {'status': {'some-id': {'field': 'value'}}}),
])
def test_store_result(handler, expected_patch, result):
    patch = Patch()
    outcomes = {handler.id: HandlerOutcome(final=True, result=result)}
    deliver_results(outcomes=outcomes, patch=patch)
    assert patch == expected_patch


def test_purge_progress_when_exists_in_body(storage, handler):
    body = {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[handler])
    assert patch == {'status': {'kopf': {'progress': {'some-id': None}}}}


def test_purge_progress_when_already_empty_in_body_and_patch(storage, handler):
    body = {}
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[handler])
    assert not patch


def test_purge_progress_when_already_empty_in_body_but_not_in_patch(storage, handler):
    body = {}
    patch = Patch({'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}})
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[handler])
    assert not patch


def test_purge_progress_when_known_at_restoration_only(storage, handler):
    body = {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[])
    assert patch == {'status': {'kopf': {'progress': {'some-id': None}}}}


def test_purge_progress_when_known_at_purge_only(storage, handler):
    body = {'status': {'kopf': {'progress': {'some-id': {'retries': 5}}}}}
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[handler])
    assert patch == {'status': {'kopf': {'progress': {'some-id': None}}}}


def test_purge_progress_cascades_to_subrefs(storage, handler):
    body = {'status': {'kopf': {'progress': {
        'some-id': {'subrefs': ['sub1', 'sub2', 'sub3']},
        'sub1': {},
        'sub2': {},
        # 'sub3' is intentionally absent -- should not be purged as already non-existent.
        'sub-unrelated': {},  # should be ignored, as not related to the involved handlers.
    }}}}
    patch = Patch()
    state = State.from_storage(body=Body(body), handlers=[handler], storage=storage)
    state.purge(patch=patch, body=Body(body), storage=storage, handlers=[handler])
    assert patch == {'status': {'kopf': {'progress': {
        'some-id': None,
        'sub1': None,
        'sub2': None,
    }}}}


def test_original_body_is_not_modified_by_storing(storage, handler):
    body = Body({})
    patch = Patch()
    state = State.from_storage(body=body, handlers=[handler], storage=storage)
    state.store(body=body, patch=patch, storage=storage)
    assert dict(body) == {}
