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


def test_adding_to_pykube_object(pykube_object):
    del pykube_object.obj['metadata']
    kopf.label(pykube_object, {'label-1': 'value-1', 'label-2': 'value-2'})
    assert len(pykube_object.labels) == 2
    assert 'label-1' in pykube_object.labels
    assert 'label-2' in pykube_object.labels
    assert pykube_object.labels['label-1'] == 'value-1'
    assert pykube_object.labels['label-2'] == 'value-2'


def test_adding_to_kubernetes_model(kubernetes_model):
    kubernetes_model.metadata = None
    kopf.label(kubernetes_model, {'label-1': 'value-1', 'label-2': 'value-2'})
    assert len(kubernetes_model.metadata.labels) == 2
    assert 'label-1' in kubernetes_model.metadata.labels
    assert 'label-2' in kubernetes_model.metadata.labels
    assert kubernetes_model.metadata.labels['label-1'] == 'value-1'
    assert kubernetes_model.metadata.labels['label-2'] == 'value-2'


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


def test_forcing_true_to_pykube_object(pykube_object):
    pykube_object.labels['label'] = 'old-value'
    kopf.label(pykube_object, {'label': 'new-value'}, forced=True)
    assert pykube_object.labels['label'] == 'new-value'


def test_forcing_false_to_pykube_object(pykube_object):
    pykube_object.labels['label'] = 'old-value'
    kopf.label(pykube_object, {'label': 'new-value'}, forced=False)
    assert pykube_object.labels['label'] == 'old-value'


def test_forcing_default_to_pykube_object(pykube_object):
    pykube_object.labels['label'] = 'old-value'
    kopf.label(pykube_object, {'label': 'new-value'})
    assert pykube_object.labels['label'] == 'old-value'


def test_forcing_true_to_kubernetes_model(kubernetes_model):
    kubernetes_model.metadata.labels = {'label': 'old-value'}
    kopf.label(kubernetes_model, {'label': 'new-value'}, forced=True)
    assert kubernetes_model.metadata.labels['label'] == 'new-value'


def test_forcing_false_to_kubernetes_model(kubernetes_model):
    kubernetes_model.metadata.labels = {'label': 'old-value'}
    kopf.label(kubernetes_model, {'label': 'new-value'}, forced=False)
    assert kubernetes_model.metadata.labels['label'] == 'old-value'


def test_forcing_default_to_kubernetes_model(kubernetes_model):
    kubernetes_model.metadata.labels = {'label': 'old-value'}
    kopf.label(kubernetes_model, {'label': 'new-value'})
    assert kubernetes_model.metadata.labels['label'] == 'old-value'


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_true_to_dict(nested):
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'jobTemplate': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=nested, forced=True)
    assert obj['metadata']['labels']['label'] == 'new-value'
    assert obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'
    assert 'unexistent' not in obj['spec']


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_true_to_pykube_object(nested, pykube_object):
    pykube_object.labels.update({'label': 'old-value'})
    pykube_object.obj.update({'spec': {'jobTemplate': {}}})
    kopf.label(pykube_object, {'label': 'new-value'}, nested=nested, forced=True)
    assert pykube_object.labels['label'] == 'new-value'
    assert pykube_object.obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'
    assert 'unexistent' not in pykube_object.obj['spec']


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_true_to_kubernetes_model(nested, kubernetes_model):
    kubernetes_model.metadata.labels = {'label': 'old-value'}
    kopf.label(kubernetes_model, {'label': 'new-value'}, nested=nested, forced=True)
    assert kubernetes_model.metadata.labels['label'] == 'new-value'
    assert kubernetes_model.spec.job_template.metadata.labels['label'] == 'new-value'
    assert not hasattr(kubernetes_model.spec, 'unexistent')


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_false_to_dict(nested):
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'jobTemplate': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=nested, forced=False)
    assert obj['metadata']['labels']['label'] == 'old-value'
    assert obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'
    assert 'unexistent' not in obj['spec']


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_false_to_pykube_object(nested, pykube_object):
    pykube_object.labels.update({'label': 'old-value'})
    pykube_object.obj.update({'spec': {'jobTemplate': {}}})
    kopf.label(pykube_object, {'label': 'new-value'}, nested=nested, forced=False)
    assert pykube_object.labels['label'] == 'old-value'
    assert pykube_object.obj['spec']['jobTemplate']['metadata']['labels']['label'] == 'new-value'
    assert 'unexistent' not in pykube_object.obj['spec']


@pytest.mark.parametrize('nested', [
    pytest.param(('spec.jobTemplate', 'spec.unexistent'), id='tuple'),
    pytest.param(['spec.jobTemplate', 'spec.unexistent'], id='list'),
    pytest.param({'spec.jobTemplate', 'spec.unexistent'}, id='set'),
    pytest.param('spec.jobTemplate', id='string'),
])
def test_nested_with_forced_false_to_kubernetes_model(nested, kubernetes_model):
    kubernetes_model.metadata.labels = {'label': 'old-value'}
    kopf.label(kubernetes_model, {'label': 'new-value'}, nested=nested, forced=False)
    assert kubernetes_model.metadata.labels['label'] == 'old-value'
    assert kubernetes_model.spec.job_template.metadata.labels['label'] == 'new-value'
    assert not hasattr(kubernetes_model.spec, 'unexistent')
