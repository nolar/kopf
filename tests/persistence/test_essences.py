from typing import Type

import pytest

from kopf.storage.diffbase import DiffBaseStorage, AnnotationsDiffBaseStorage, StatusDiffBaseStorage
from kopf.storage.diffbase import LAST_SEEN_ANNOTATION
from kopf.structs.bodies import Body

ALL_STORAGES = [AnnotationsDiffBaseStorage, StatusDiffBaseStorage]


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_resource_references(
        cls: Type[DiffBaseStorage],
):
    body = Body({'apiVersion': 'group/version', 'kind': 'Kind'})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_system_fields_and_cleans_parents(
        field: str,
        cls: Type[DiffBaseStorage],
):
    body = Body({'metadata': {field: 'x'}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_system_fields_but_keeps_extra_fields(
        field: str,
        cls: Type[DiffBaseStorage],
):
    body = Body({'metadata': {field: 'x', 'other': 'y'}})
    storage = cls()
    essence = storage.build(body=body, extra_fields=['metadata.other'])
    assert essence == {'metadata': {'other': 'y'}}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_garbage_annotations_and_cleans_parents(
        annotation: str,
        cls: Type[DiffBaseStorage],
):
    body = Body({'metadata': {'annotations': {annotation: 'x'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_garbage_annotations_but_keeps_others(
        annotation: str,
        cls: Type[DiffBaseStorage],
):
    body = Body({'metadata': {'annotations': {annotation: 'x', 'other': 'y'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {'metadata': {'annotations': {'other': 'y'}}}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_status_and_cleans_parents(
        cls: Type[DiffBaseStorage],
):
    body = Body({'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_status_but_keeps_extra_fields(
        cls: Type[DiffBaseStorage],
):
    body = Body({'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}})
    storage = cls()
    essence = storage.build(body=body, extra_fields=['status.other'])
    assert essence == {'status': {'other': 'z'}}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_clones_body(
        cls: Type[DiffBaseStorage],
):
    body = Body({'spec': {'depth': {'field': 'x'}}})
    storage = cls()
    essence = storage.build(body=body)
    body['spec']['depth']['field'] = 'y'
    assert essence is not body
    assert essence['spec'] is not body['spec']
    assert essence['spec']['depth'] is not body['spec']['depth']
    assert essence['spec']['depth']['field'] == 'x'
