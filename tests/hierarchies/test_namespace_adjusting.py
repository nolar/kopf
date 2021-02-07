import copy

import pytest

import kopf

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


# The EXISTING namespaces are preserved.
@obj1_with_namespace
def test_preserved_namespace_of_dict(obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.adjust_namespace(obj1, namespace='provided-namespace')
    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] != 'provided-namespace'


@obj2_with_namespace
@obj1_with_namespace
def test_preserved_namespaces_of_dicts(multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.adjust_namespace(objs, namespace='provided-namespace')
    assert 'namespace' in obj1['metadata']
    assert 'namespace' in obj2['metadata']
    assert obj1['metadata']['namespace'] != 'provided-namespace'
    assert obj2['metadata']['namespace'] != 'provided-namespace'


# When namespaces are ABSENT, they are added.
@obj1_without_namespace
def test_assignment_of_namespace_of_dict(obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.adjust_namespace(obj1, namespace='provided-namespace')
    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'


@obj2_without_namespace
@obj1_without_namespace
def test_assignment_of_namespaces_of_dicts(multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.adjust_namespace(objs, namespace='provided-namespace')
    assert 'namespace' in obj1['metadata']
    assert 'namespace' in obj2['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'
    assert obj2['metadata']['namespace'] == 'provided-namespace'
