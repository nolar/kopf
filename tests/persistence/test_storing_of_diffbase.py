import json
from typing import Type

import pytest

from kopf.storage.diffbase import (
    DiffBaseStorage, AnnotationsDiffBaseStorage, StatusDiffBaseStorage, MultiDiffBaseStorage,
)
from kopf.structs.bodies import Body, BodyEssence
from kopf.structs.patches import Patch
from kopf.structs.dicts import FieldSpec


class DualDiffBaseStore(MultiDiffBaseStorage):
    def __init__(
            self,
            name: str = 'kopf.zalando.org/last-handled-configuration',
            field: FieldSpec = 'status.kopf.last-handled-configuration',
    ):
        super().__init__([
            AnnotationsDiffBaseStorage(name=name),
            StatusDiffBaseStorage(field=field),
        ])


ALL_STORAGES = [AnnotationsDiffBaseStorage, StatusDiffBaseStorage, DualDiffBaseStore]
ANNOTATIONS_POPULATING_STORAGES = [AnnotationsDiffBaseStorage, DualDiffBaseStore]
STATUS_POPULATING_STORAGES = [StatusDiffBaseStorage, DualDiffBaseStore]

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

ESSENCE_JSON_1 = json.dumps(ESSENCE_DATA_1)  # the same serialisation for all environments
ESSENCE_JSON_2 = json.dumps(ESSENCE_DATA_2)  # the same serialisation for all environments


#
# Creation and parametrization.
#


def test_annotations_store_with_defaults():
    storage = AnnotationsDiffBaseStorage()
    assert storage.name == 'kopf.zalando.org/last-handled-configuration'


def test_annotations_storage_with_name():
    storage = AnnotationsDiffBaseStorage(name='my-operator.my-company.com/diff-base')
    assert storage.name == 'my-operator.my-company.com/diff-base'


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


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_fetching_from_annotations_storage(cls):
    storage = cls(name='my-operator.example.com/diff-base')
    body = Body({'metadata': {'annotations': {
        'my-operator.example.com/diff-base': ESSENCE_JSON_1,
    }}})
    content = storage.fetch(body=body)

    assert content == ESSENCE_DATA_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_populates_keys(cls):
    storage = cls(name='my-operator.example.com/diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/diff-base'] == ESSENCE_JSON_1


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_storing_to_annotations_storage_overwrites_old_content(cls):
    storage = cls(name='my-operator.example.com/diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_2)

    assert patch
    assert patch['metadata']['annotations']['my-operator.example.com/diff-base'] == ESSENCE_JSON_2


#
# Status-populating.
#


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_fetching_from_status_storage(cls):
    storage = cls(field='status.my-operator.diff-base')
    body = Body({'status': {'my-operator': {'diff-base': ESSENCE_JSON_1}}})
    content = storage.fetch(body=body)

    assert content == ESSENCE_DATA_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_populates_keys(cls):
    storage = cls(field='status.my-operator.diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)

    assert patch
    assert patch['status']['my-operator']['diff-base'] == ESSENCE_JSON_1


@pytest.mark.parametrize('cls', STATUS_POPULATING_STORAGES)
def test_storing_to_status_storage_overwrites_old_content(
        cls: Type[DiffBaseStorage]):
    storage = cls(field='status.my-operator.diff-base')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_2)

    assert patch
    assert patch['status']['my-operator']['diff-base'] == ESSENCE_JSON_2
