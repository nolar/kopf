import pytest

import kopf


def test_adding_to_dict():
    obj = {}
    kopf.label(obj, {'label-1': 'value-1', 'label-2': 'value-2'})
    assert 'metadata' in obj
    assert 'labels' in obj['metadata']
    assert isinstance(obj['metadata']['labels'], dict)
    assert len(obj['metadata']['labels']) == 2
    assert 'label-1' in obj['metadata']['labels']
    assert 'label-2' in obj['metadata']['labels']
    assert obj['metadata']['labels']['label-1'] == 'value-1'
    assert obj['metadata']['labels']['label-2'] == 'value-2'


def test_adding_to_dicts(multicls):
    obj1 = {}
    obj2 = {}
    objs = multicls([obj1, obj2])
    kopf.label(objs, {'label-1': 'value-1', 'label-2': 'value-2'})
    assert isinstance(obj1['metadata']['labels'], dict)
    assert len(obj1['metadata']['labels']) == 2
    assert 'label-1' in obj1['metadata']['labels']
    assert 'label-2' in obj1['metadata']['labels']
    assert obj1['metadata']['labels']['label-1'] == 'value-1'
    assert obj1['metadata']['labels']['label-2'] == 'value-2'

    assert isinstance(obj2['metadata']['labels'], dict)
    assert len(obj2['metadata']['labels']) == 2
    assert 'label-1' in obj2['metadata']['labels']
    assert 'label-2' in obj2['metadata']['labels']
    assert obj2['metadata']['labels']['label-1'] == 'value-1'
    assert obj2['metadata']['labels']['label-2'] == 'value-2'


def test_forcing_true_warns_on_deprecated_option():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    with pytest.deprecated_call(match=r"use forced="):
        kopf.label(obj, {'label': 'new-value'}, force=True)
    assert obj['metadata']['labels']['label'] == 'new-value'


def test_forcing_false_warns_on_deprecated_option():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    with pytest.deprecated_call(match=r"use forced="):
        kopf.label(obj, {'label': 'new-value'}, force=False)
    assert obj['metadata']['labels']['label'] == 'old-value'


def test_forcing_true_to_dict():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'}, forced=True)
    assert obj['metadata']['labels']['label'] == 'new-value'


def test_forcing_false_to_dict():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'}, forced=False)
    assert obj['metadata']['labels']['label'] == 'old-value'


def test_forcing_default_to_dict():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'})
    assert obj['metadata']['labels']['label'] == 'old-value'


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate',), id='tuple'),
    pytest.param(['spec.jobTemplate'], id='list'),
    pytest.param({'spec.jobTemplate'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_true_to_dict(nested):
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'jobTemplate': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=nested, forced=True)
    assert obj['metadata']['labels']['label'] == 'new-value'
    assert obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate',), id='tuple'),
    pytest.param(['spec.jobTemplate'], id='list'),
    pytest.param({'spec.jobTemplate'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_false_to_dict(nested):
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'jobTemplate': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=nested, forced=False)
    assert obj['metadata']['labels']['label'] == 'old-value'
    assert obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'
