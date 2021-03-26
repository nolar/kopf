import copy

import pytest

import kopf

forced_mode = pytest.mark.parametrize('forcedness', [
    pytest.param(dict(forced=True), id='forcedTrue'),
])
non_forced_mode = pytest.mark.parametrize('forcedness', [
    pytest.param(dict(forced=False), id='forcedFalse'),
    pytest.param(dict(), id='forcedAbsent'),
])
any_forced_mode = pytest.mark.parametrize('forcedness', [
    pytest.param(dict(forced=True), id='forcedTrue'),
    pytest.param(dict(forced=False), id='forcedFalse'),
    pytest.param(dict(), id='forcedAbsent'),
])

obj1_with_namespace = pytest.mark.parametrize('obj1', [
    pytest.param({'metadata': {'namespace': 'a'}}, id='withnamespace'),
])
obj2_with_namespace = pytest.mark.parametrize('obj2', [
    pytest.param({'metadata': {'namespace': 'a'}}, id='withnamespace'),
])
obj1_without_namespace = pytest.mark.parametrize('obj1', [
    pytest.param({}, id='withoutmetadata'),
    pytest.param({'metadata': {}}, id='withoutnamespace'),
])
obj2_without_namespace = pytest.mark.parametrize('obj2', [
    pytest.param({}, id='withoutmetadata'),
    pytest.param({'metadata': {}}, id='withoutnamespace'),
])


# In the NON-FORCED mode, the EXISTING namespaces are preserved.
@obj1_with_namespace
@non_forced_mode
def test_preserved_namespace_of_dict(forcedness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.adjust_namespace(obj1, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] != 'provided-namespace'


@obj2_with_namespace
@obj1_with_namespace
@non_forced_mode
def test_preserved_namespaces_of_dicts(forcedness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.adjust_namespace(objs, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert 'namespace' in obj2['metadata']
    assert obj1['metadata']['namespace'] != 'provided-namespace'
    assert obj2['metadata']['namespace'] != 'provided-namespace'


@obj1_with_namespace
@non_forced_mode
def test_preserved_namespace_of_pykube_object(forcedness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.adjust_namespace(pykube_object, namespace='provided-namespace', **forcedness)
    assert pykube_object.obj['metadata'].get('namespace') != 'provided-namespace'


@obj1_with_namespace
@non_forced_mode
def test_preserved_namespace_of_kubernetes_model(forcedness, kubernetes_model, obj1):
    kubernetes_model.metadata.namespace = obj1.get('metadata', {}).get('namespace')
    kopf.adjust_namespace(kubernetes_model, namespace='provided-namespace', **forcedness)
    assert kubernetes_model.metadata.namespace != 'provided-namespace'


#
# In the FORCED mode, the EXISTING namespaces are overwritten.
#
@obj1_with_namespace
@forced_mode
def test_overwriting_of_namespace_of_dict(forcedness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.adjust_namespace(obj1, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'


@obj2_with_namespace
@obj1_with_namespace
@forced_mode
def test_overwriting_of_namespaces_of_dicts(forcedness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.adjust_namespace(objs, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert 'namespace' in obj2['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'
    assert obj2['metadata']['namespace'] == 'provided-namespace'


@obj1_with_namespace
@forced_mode
def test_overwriting_namespace_of_pykube_object(forcedness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.adjust_namespace(pykube_object, namespace='provided-namespace', **forcedness)
    assert pykube_object.obj['metadata'].get('namespace') == 'provided-namespace'


@obj1_with_namespace
@forced_mode
def test_overwriting_namespace_of_kubernetes_model(forcedness, kubernetes_model, obj1):
    kubernetes_model.metadata.namespace = obj1.get('metadata', {}).get('namespace')
    kopf.adjust_namespace(kubernetes_model, namespace='provided-namespace', **forcedness)
    assert kubernetes_model.metadata.namespace == 'provided-namespace'


#
# When namespaces are ABSENT, they are added regardless of the forced mode.
#
@obj1_without_namespace
@any_forced_mode
def test_assignment_of_namespace_of_dict(forcedness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.adjust_namespace(obj1, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'


@obj2_without_namespace
@obj1_without_namespace
@any_forced_mode
def test_assignment_of_namespaces_of_dicts(forcedness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.adjust_namespace(objs, namespace='provided-namespace', **forcedness)
    assert 'namespace' in obj1['metadata']
    assert 'namespace' in obj2['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'
    assert obj2['metadata']['namespace'] == 'provided-namespace'


@obj1_without_namespace
@any_forced_mode
def test_assignment_namespace_of_pykube_object(forcedness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.adjust_namespace(pykube_object, namespace='provided-namespace', **forcedness)
    assert pykube_object.obj['metadata'].get('namespace') == 'provided-namespace'


@any_forced_mode
def test_assignment_namespace_of_kubernetes_model(forcedness, kubernetes_model):
    kubernetes_model.metadata = None
    kopf.adjust_namespace(kubernetes_model, namespace='provided-namespace', **forcedness)
    assert kubernetes_model.metadata.namespace == 'provided-namespace'
