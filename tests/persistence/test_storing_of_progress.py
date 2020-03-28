import json
from typing import Type

import pytest

from kopf.structs.bodies import Body
from kopf.structs.handlers import HandlerId
from kopf.structs.patches import Patch
from kopf.storage.progress import (
    ProgressStorage, ProgressRecord,
    AnnotationsProgressStorage, StatusProgressStorage, SmartProgressStorage,
)

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
)

CONTENT_DATA_2 = ProgressRecord(
    started='2021-01-01T00:00:00',
    stopped='2021-12-31T23:59:59',
    delayed='3001-01-01T00:00:00',
    retries=456,
    success=False,
    failure=False,
    message="Some error.",
)

CONTENT_JSON_1 = json.dumps(CONTENT_DATA_1)  # the same serialisation for all environments
CONTENT_JSON_2 = json.dumps(CONTENT_DATA_2)  # the same serialisation for all environments


#
# Storage creation.
#


def test_status_storage_with_defaults():
    storage = StatusProgressStorage()
    assert storage.field == ('status', 'kopf', 'progress')  # as before the change


def test_status_storage_with_name():
    storage = StatusProgressStorage(name='my-operator')
    assert storage.field == ('status', 'my-operator', 'progress')


def test_status_storage_with_field():
    storage = StatusProgressStorage(field='status.my-operator')
    assert storage.field == ('status', 'my-operator')


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


def test_smart_storage_with_name():
    storage = SmartProgressStorage(name='my-operator')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'kopf.zalando.org'
    assert storage.storages[1].field == ('status', 'my-operator', 'progress')


def test_smart_storage_with_field():
    storage = SmartProgressStorage(field='status.my-operator')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'kopf.zalando.org'
    assert storage.storages[1].field == ('status', 'my-operator')


def test_smart_storage_with_prefix():
    storage = SmartProgressStorage(prefix='my-operator.my-company.com')
    assert isinstance(storage.storages[0], AnnotationsProgressStorage)
    assert isinstance(storage.storages[1], StatusProgressStorage)
    assert storage.storages[0].prefix == 'my-operator.my-company.com'
    assert storage.storages[1].field == ('status', 'kopf', 'progress')


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
