import json
from typing import Type

import pytest

from kopf.storage.progress import AnnotationsProgressStorage, ProgressRecord, ProgressStorage, \
                                  SmartProgressStorage, StatusProgressStorage
from kopf.structs.bodies import Body
from kopf.structs.ids import HandlerId
from kopf.structs.patches import Patch

ALL_STORAGES = [AnnotationsProgressStorage, StatusProgressStorage, SmartProgressStorage]
ANNOTATIONS_POPULATING_STORAGES = [AnnotationsProgressStorage, SmartProgressStorage]
STATUS_POPULATING_STORAGES = [StatusProgressStorage, SmartProgressStorage]

CONTENT_DATA_1 = ProgressRecord(
    started='2020-01-01T00:00:00',
    stopped='2020-12-31T23:59:59',
    delayed='3000-01-01T00:00:00',
    retries=123,
    success=False,
    failure=False,
    message=None,
    subrefs=None,
)

CONTENT_DATA_2 = ProgressRecord(
    started='2021-01-01T00:00:00',
    stopped='2021-12-31T23:59:59',
    delayed='3001-01-01T00:00:00',
    retries=456,
    success=False,
    failure=False,
    message="Some error.",
    subrefs=['sub1', 'sub2'],
)

CONTENT_JSON_1 = json.dumps(CONTENT_DATA_1, separators=(',', ':'))
CONTENT_JSON_2 = json.dumps(CONTENT_DATA_2, separators=(',', ':'))


#
# Storage creation.
#


def test_status_storage_with_defaults():
    storage = StatusProgressStorage()
    assert storage.field == ('status', 'kopf', 'progress')  # as before the change
    assert storage.touch_field == ('status', 'kopf', 'dummy')  # as before the change


def test_status_storage_with_name():
    storage = StatusProgressStorage(name='my-operator')
    assert storage.field == ('status', 'my-operator', 'progress')
    assert storage.touch_field == ('status', 'my-operator', 'dummy')


def test_status_storage_with_field():
    storage = StatusProgressStorage(field='status.my-operator', touch_field='status.my-dummy')
    assert storage.field == ('status', 'my-operator')
    assert storage.touch_field == ('status', 'my-dummy')


def test_annotations_storage_with_defaults():
    storage = AnnotationsProgressStorage()
    assert storage.prefix == 'kopf.zalando.org'


def test_annotations_storage_with_prefix():
    storage = AnnotationsProgressStorage(prefix='my-operator.my-company.com')
    assert storage.prefix == 'my-operator.my-company.com'


def test_smart_storage_with_defaults():
    storage = SmartProgressStorage()
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'kopf.zalando.org'
    assert storage.storages[1].field == ('status', 'kopf', 'progress')
    assert storage.storages[1].touch_field == ('status', 'kopf', 'dummy')


def test_smart_storage_with_name():
    storage = SmartProgressStorage(name='my-operator')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'kopf.zalando.org'
    assert storage.storages[1].field == ('status', 'my-operator', 'progress')
    assert storage.storages[1].touch_field == ('status', 'my-operator', 'dummy')


def test_smart_storage_with_field():
    storage = SmartProgressStorage(field='status.my-operator', touch_field='status.my-dummy')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'kopf.zalando.org'
    assert storage.storages[1].field == ('status', 'my-operator')
    assert storage.storages[1].touch_field == ('status', 'my-dummy')


def test_smart_storage_with_prefix():
    storage = SmartProgressStorage(prefix='my-operator.my-company.com')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'my-operator.my-company.com'
    assert storage.storages[1].field == ('status', 'kopf', 'progress')
    assert storage.storages[1].touch_field == ('status', 'kopf', 'dummy')


#
# Common behaviour.
#


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_fetching_from_empty_body_returns_none(
        cls: Type[ProgressStorage]):
    storage = cls()
    body = Body({})
    data = storage.fetch(body=body, key=HandlerId('id1'))
    assert data is None


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_purging_already_empty_body_does_nothing(
        cls: Type[ProgressStorage]):
    storage = cls()
    patch = Patch()
    body = Body({})
    storage.purge(body=body, patch=patch, key=HandlerId('id1'))
    assert not patch


#
# Annotations-populating.
#


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_fetching_from_annotations_storage(cls):
    storage = cls(prefix='my-operator.example.com', verbose=True)
    body = Body({'metadata': {'annotations': {
        'my-operator.example.com/id1': CONTENT_JSON_1,
    }}})
    content = storage.fetch(body=body, key=HandlerId('id1'))

    assert content == CONTENT_DATA_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_populates_keys(cls):
    storage = cls(prefix='my-operator.example.com', verbose=True)
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    
    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/id1'] == CONTENT_JSON_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_appends_keys(cls):
    storage = cls(prefix='my-operator.example.com', verbose=True)
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=body, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_2)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/id1'] == CONTENT_JSON_1
    assert patch['metadata']['annotations']['my-operator.example.com/id2'] == CONTENT_JSON_2


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_overwrites_old_content(cls):
    storage = cls(prefix='my-operator.example.com', verbose=True)
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_2)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/id1'] == CONTENT_JSON_2


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_cleans_content(cls):
    storage = cls(prefix='my-operator.example.com')  # no verbose=
    patch = Patch()
    body = Body({})
    content = ProgressRecord(
        started=None,
        stopped=None,
        delayed=None,
        retries=None,
        success=None,
        failure=None,
        message=None,
        subrefs=None,
    )
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=content)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/id1'] == json.dumps({})


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_purging_of_annotations_storage_nullifies_content(cls):
    storage = cls(prefix='my-operator.example.com', verbose=True)
    patch = Patch()
    body = Body({'metadata': {'annotations': {
        'my-operator.example.com/id1': CONTENT_JSON_1,
    }}})
    storage.purge(body=body, patch=patch, key=HandlerId('id1'))

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/id1'] is None


@pytest.mark.parametrize('body_data', [
    pytest.param({}, id='without-data'),
    pytest.param({'metadata': {'annotations': {'my-operator.example.com/my-dummy': 'something'}}}, id='with-data'),
])
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_touching_via_annotations_storage_with_payload(cls, body_data):
    storage = cls(prefix='my-operator.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body(body_data)
    storage.touch(body=body, patch=patch, value='hello')

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/my-dummy'] == 'hello'


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_touching_via_annotations_storage_with_none_when_absent(cls):
    storage = cls(prefix='my-operator.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({})
    storage.touch(body=body, patch=patch, value=None)

    assert not patch


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_touching_via_annotations_storage_with_none_when_present(cls):
    storage = cls(prefix='my-operator.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({'metadata': {'annotations': {'my-operator.example.com/my-dummy': 'something'}}})
    storage.touch(body=body, patch=patch, value=None)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/my-dummy'] is None


#
# Status-populating.
#


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_fetching_from_status_storage(cls):
    storage = cls(field='status.my-operator')
    body = Body({'status': {'my-operator': {'id1': CONTENT_DATA_1, 'id2': CONTENT_DATA_2}}})
    content = storage.fetch(body=body, key=HandlerId('id1'))

    assert content == CONTENT_DATA_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_populates_keys(cls):
    storage = cls(field='status.my-operator')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    assert patch
    assert patch['status']['my-operator']['id1'] == CONTENT_DATA_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_appends_keys(cls):
    storage = cls(field='status.my-operator')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=body, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_1)

    assert patch
    assert patch['status']['my-operator']['id1'] == CONTENT_DATA_1
    assert patch['status']['my-operator']['id2'] == CONTENT_DATA_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_overwrites_old_content(cls):
    storage = cls(field='status.my-operator')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_2)

    assert patch
    assert patch['status']['my-operator']['id1'] == CONTENT_DATA_2


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_purging_of_status_storage_nullifies_content(cls):
    storage = cls(field='status.my-operator')
    patch = Patch()
    body = Body({'status': {'my-operator': {'id1': CONTENT_DATA_1}}})
    storage.purge(body=body, patch=patch, key=HandlerId('id1'))

    assert patch
    assert patch['status']['my-operator']['id1'] is None


@pytest.mark.parametrize('body_data', [
    pytest.param({}, id='without-data'),
    pytest.param({'status': {'my-dummy': 'something'}}, id='with-data'),
])
@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_touching_via_status_storage_with_payload(cls, body_data):
    storage = cls(field='status.my-operator', touch_field='status.my-dummy')
    patch = Patch()
    body = Body(body_data)
    storage.touch(body=body, patch=patch, value='hello')

    assert patch
    assert patch['status']['my-dummy'] == 'hello'


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_touching_via_status_storage_with_none_when_absent(cls):
    storage = cls(touch_field='status.my-dummy')
    patch = Patch()
    body = Body({})
    storage.touch(body=body, patch=patch, value=None)

    assert not patch


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_touching_via_status_storage_with_none_when_present(cls):
    storage = cls(touch_field='status.my-dummy')
    patch = Patch()
    body = Body({'status': {'my-dummy': 'something'}})
    storage.touch(body=body, patch=patch, value=None)

    assert patch
    assert patch['status']['my-dummy'] is None
