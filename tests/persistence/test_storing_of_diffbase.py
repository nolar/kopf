import json
from typing import Type

import pytest

from kopf.storage.diffbase import AnnotationsDiffBaseStorage, DiffBaseStorage, \
                                  MultiDiffBaseStorage, StatusDiffBaseStorage
from kopf.structs.bodies import Body, BodyEssence
from kopf.structs.dicts import FieldSpec
from kopf.structs.patches import Patch


class DualDiffBaseStorage(MultiDiffBaseStorage):
    def __init__(
            self,
            prefix: str = 'kopf.zalando.org',
            key: str = 'last-handled-configuration',
            field: FieldSpec = 'status.kopf.last-handled-configuration',
    ):
        super().__init__([
            AnnotationsDiffBaseStorage(prefix=prefix, key=key),
            StatusDiffBaseStorage(field=field),
        ])


ALL_STORAGES = [AnnotationsDiffBaseStorage, StatusDiffBaseStorage, DualDiffBaseStorage]
ANNOTATIONS_POPULATING_STORAGES = [AnnotationsDiffBaseStorage, DualDiffBaseStorage]
STATUS_POPULATING_STORAGES = [StatusDiffBaseStorage, DualDiffBaseStorage]

ESSENCE_DATA_1 = BodyEssence(
    spec={
        'string-field': 'value1',
        'integer-field': 123,
        'float-field': 123.456,
        'false-field': False,
        'true-field': True,
        # Nones/nulls are not stored by K8s, so we do not test them.
    },
)

ESSENCE_DATA_2 = BodyEssence(
    spec={
        'hello': 'world',
        'the-cake': False,
        # Nones/nulls are not stored by K8s, so we do not test them.
    },
)

ESSENCE_JSON_1 = json.dumps(ESSENCE_DATA_1, separators=(',', ':'))
ESSENCE_JSON_2 = json.dumps(ESSENCE_DATA_2, separators=(',', ':'))


#
# Creation and parametrization.
#


def test_annotations_store_with_defaults():
    storage = AnnotationsDiffBaseStorage()
    assert storage.prefix == 'kopf.zalando.org'
    assert storage.key == 'last-handled-configuration'


def test_annotations_storage_with_prefix_and_key():
    storage = AnnotationsDiffBaseStorage(prefix='my-operator.my-company.com', key='diff-base')
    assert storage.prefix == 'my-operator.my-company.com'
    assert storage.key == 'diff-base'


def test_status_storage_with_field():
    storage = StatusDiffBaseStorage(field='status.my-operator.diff-base')
    assert storage.field == ('status', 'my-operator', 'diff-base')


#
# Common behaviour.
#


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_fetching_from_empty_body_returns_none(
        cls: Type[DiffBaseStorage]):
    storage = cls()
    body = Body({})
    data = storage.fetch(body=body)
    assert data is None


#
# Annotations-populating.
#


@pytest.mark.parametrize('suffix', [
    pytest.param('', id='no-suffix'),
    pytest.param('\n', id='newline-suffix'),
])
@pytest.mark.parametrize('prefix', [
    pytest.param('', id='no-prefix'),
    pytest.param('\n', id='newline-prefix'),
])
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_fetching_from_annotations_storage(cls, prefix, suffix):
    storage = cls(prefix='my-operator.example.com', key='diff-base')
    body = Body({'metadata': {'annotations': {
        'my-operator.example.com/diff-base': prefix + ESSENCE_JSON_1 + suffix,
    }}})
    content = storage.fetch(body=body)

    assert content == ESSENCE_DATA_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_populates_keys(cls):
    storage = cls(prefix='my-operator.example.com', key='diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)

    assert patch
    assert patch.meta.annotations['my-operator.example.com/diff-base'][0] != '\n'
    assert patch.meta.annotations['my-operator.example.com/diff-base'][-1] == '\n'
    assert patch.meta.annotations['my-operator.example.com/diff-base'].strip() == ESSENCE_JSON_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_overwrites_old_content(cls):
    storage = cls(prefix='my-operator.example.com', key='diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_2)

    assert patch
    assert patch.meta.annotations['my-operator.example.com/diff-base'][0] != '\n'
    assert patch.meta.annotations['my-operator.example.com/diff-base'][-1] == '\n'
    assert patch.meta.annotations['my-operator.example.com/diff-base'].strip() == ESSENCE_JSON_2


#
# Status-populating.
#


@pytest.mark.parametrize('suffix', [
    pytest.param('', id='no-suffix'),
    pytest.param('\n', id='newline-suffix'),
])
@pytest.mark.parametrize('prefix', [
    pytest.param('', id='no-prefix'),
    pytest.param('\n', id='newline-prefix'),
])
@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_fetching_from_status_storage(cls, prefix, suffix):
    storage = cls(field='status.my-operator.diff-base')
    body = Body({'status': {'my-operator': {'diff-base': prefix + ESSENCE_JSON_1 + suffix}}})
    content = storage.fetch(body=body)

    assert content == ESSENCE_DATA_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_populates_keys(cls):
    storage = cls(field='status.my-operator.diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)

    assert patch
    assert patch.status['my-operator']['diff-base'][0] != '\n'
    assert patch.status['my-operator']['diff-base'][-1] != '\n'
    assert patch.status['my-operator']['diff-base'] == ESSENCE_JSON_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_overwrites_old_content(
        cls: Type[DiffBaseStorage]):
    storage = cls(field='status.my-operator.diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_2)

    assert patch
    assert patch.status['my-operator']['diff-base'][0] != '\n'
    assert patch.status['my-operator']['diff-base'][-1] != '\n'
    assert patch.status['my-operator']['diff-base'] == ESSENCE_JSON_2
