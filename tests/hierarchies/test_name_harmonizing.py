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

strict_mode = pytest.mark.parametrize('strictness', [
    pytest.param(dict(strict=True), id='strictTrue'),
])
non_strict_mode = pytest.mark.parametrize('strictness', [
    pytest.param(dict(strict=False), id='strictFalse'),
    pytest.param(dict(), id='strictAbsent'),
])
any_strict_mode = pytest.mark.parametrize('strictness', [
    pytest.param(dict(strict=True), id='strictTrue'),
    pytest.param(dict(strict=False), id='strictFalse'),
    pytest.param(dict(), id='strictAbsent'),
])

obj1_with_names = pytest.mark.parametrize('obj1', [
    pytest.param({'metadata': {'name': 'a'}}, id='regularname'),
    pytest.param({'metadata': {'generateName': 'b'}}, id='generatename'),
    pytest.param({'metadata': {'name': 'c', 'generateName': 'd'}}, id='bothnames'),
])
obj2_with_names = pytest.mark.parametrize('obj2', [
    pytest.param({'metadata': {'name': 'a'}}, id='regularname'),
    pytest.param({'metadata': {'generateName': 'b'}}, id='generatename'),
    pytest.param({'metadata': {'name': 'c', 'generateName': 'd'}}, id='bothnames'),
])
obj1_without_names = pytest.mark.parametrize('obj1', [
    pytest.param({}, id='withoutmeta'),
    pytest.param({'metadata': {}}, id='withmeta'),
])
obj2_without_names = pytest.mark.parametrize('obj2', [
    pytest.param({}, id='withoutmeta'),
    pytest.param({'metadata': {}}, id='withmeta'),
])


# In the NON-FORCED mode, the EXISTING names are preserved.
# The strictness is not involved due to this (no new names added).
@obj1_with_names
@any_strict_mode
@non_forced_mode
def test_preserved_name_of_dict(forcedness, strictness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.harmonize_naming(obj1, name='provided-name', **forcedness, **strictness)
    assert obj1['metadata'].get('name') != 'provided-name'
    assert obj1['metadata'].get('generateName') != 'provided-name'


@obj2_with_names
@obj1_with_names
@any_strict_mode
@non_forced_mode
def test_preserved_names_of_dicts(forcedness, strictness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.harmonize_naming(objs, name='provided-name', **forcedness, **strictness)
    assert obj1['metadata'].get('name') != 'provided-name'
    assert obj2['metadata'].get('name') != 'provided-name'
    assert obj1['metadata'].get('generateName') != 'provided-name'
    assert obj2['metadata'].get('generateName') != 'provided-name'


@obj1_with_names
@any_strict_mode
@non_forced_mode
def test_preserved_names_of_pykube_object(forcedness, strictness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.harmonize_naming(pykube_object, name='provided-name', **forcedness, **strictness)
    assert pykube_object.obj['metadata'].get('name') != 'provided-name'
    assert pykube_object.obj['metadata'].get('generateName') != 'provided-name'


@obj1_with_names
@any_strict_mode
@non_forced_mode
def test_preserved_names_of_kubernetes_model(forcedness, strictness, kubernetes_model, obj1):
    kubernetes_model.metadata.name = obj1.get('metadata', {}).get('name')
    kubernetes_model.metadata.generate_name = obj1.get('metadata', {}).get('generateName')
    kopf.harmonize_naming(kubernetes_model, name='provided-name', **forcedness, **strictness)
    assert kubernetes_model.metadata.name != 'provided-name'
    assert kubernetes_model.metadata.generate_name != 'provided-name'


# In the FORCED mode, the EXISTING names are overwritten.
# It only depends which of the names -- regular or generated -- is left.
@obj1_with_names
@strict_mode
@forced_mode
def test_overwriting_of_strict_name_of_dict(forcedness, strictness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.harmonize_naming(obj1, name='provided-name', **forcedness, **strictness)
    assert 'name' in obj1['metadata']
    assert 'generateName' not in obj1['metadata']
    assert obj1['metadata']['name'] == 'provided-name'


@obj2_with_names
@obj1_with_names
@strict_mode
@forced_mode
def test_overwriting_of_strict_names_of_dicts(forcedness, strictness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.harmonize_naming(objs, name='provided-name', **forcedness, **strictness)
    assert 'name' in obj1['metadata']
    assert 'name' in obj2['metadata']
    assert 'generateName' not in obj1['metadata']
    assert 'generateName' not in obj2['metadata']
    assert obj2['metadata']['name'] == 'provided-name'
    assert obj1['metadata']['name'] == 'provided-name'


@obj1_with_names
@strict_mode
@forced_mode
def test_overwriting_of_strict_name_of_pykube_object(forcedness, strictness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.harmonize_naming(pykube_object, name='provided-name', **forcedness, **strictness)
    assert pykube_object.obj['metadata'].get('name') == 'provided-name'
    assert pykube_object.obj['metadata'].get('generateName') is None


@obj1_with_names
@strict_mode
@forced_mode
def test_overwriting_of_strict_name_of_kubernetes_model(forcedness, strictness, kubernetes_model, obj1):
    kubernetes_model.metadata.name = obj1.get('metadata', {}).get('name')
    kubernetes_model.metadata.generate_name = obj1.get('metadata', {}).get('generateName')
    kopf.harmonize_naming(kubernetes_model, name='provided-name', **forcedness, **strictness)
    assert kubernetes_model.metadata.name == 'provided-name'
    assert kubernetes_model.metadata.generate_name is None


@obj1_with_names
@non_strict_mode
@forced_mode
def test_overwriting_of_relaxed_name_of_dict(forcedness, strictness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.harmonize_naming(obj1, name='provided-name', **forcedness, **strictness)
    assert 'name' not in obj1['metadata']
    assert 'generateName' in obj1['metadata']
    assert obj1['metadata']['generateName'] == 'provided-name-'


@obj2_with_names
@obj1_with_names
@non_strict_mode
@forced_mode
def test_overwriting_of_relaxed_names_of_dicts(forcedness, strictness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.harmonize_naming(objs, name='provided-name', **forcedness, **strictness)
    assert 'name' not in obj1['metadata']
    assert 'name' not in obj2['metadata']
    assert 'generateName' in obj1['metadata']
    assert 'generateName' in obj2['metadata']
    assert obj1['metadata']['generateName'] == 'provided-name-'
    assert obj2['metadata']['generateName'] == 'provided-name-'


@obj1_with_names
@non_strict_mode
@forced_mode
def test_overwriting_of_relaxed_name_of_pykube_object(forcedness, strictness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.harmonize_naming(pykube_object, name='provided-name', **forcedness, **strictness)
    assert pykube_object.obj['metadata'].get('name') is None
    assert pykube_object.obj['metadata'].get('generateName') == 'provided-name-'


@obj1_with_names
@non_strict_mode
@forced_mode
def test_overwriting_of_relaxed_name_of_kubernetes_model(forcedness, strictness, kubernetes_model, obj1):
    kubernetes_model.metadata.name = obj1.get('metadata', {}).get('name')
    kubernetes_model.metadata.generate_name = obj1.get('metadata', {}).get('generateName')
    kopf.harmonize_naming(kubernetes_model, name='provided-name', **forcedness, **strictness)
    assert kubernetes_model.metadata.name is None
    assert kubernetes_model.metadata.generate_name == 'provided-name-'


# When names are ABSENT, they are added regardless of the forced mode.
# The only varying part is which name is added: regular or generated.
@obj1_without_names
@strict_mode
@any_forced_mode
def test_assignment_of_strict_name_of_dict(forcedness, strictness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.harmonize_naming(obj1, name='provided-name', **forcedness, **strictness)
    assert 'name' in obj1['metadata']
    assert 'generateName' not in obj1['metadata']
    assert obj1['metadata']['name'] == 'provided-name'


@obj2_without_names
@obj1_without_names
@strict_mode
@any_forced_mode
def test_assignment_of_strict_names_of_dicts(forcedness, strictness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.harmonize_naming(objs, name='provided-name', **forcedness, **strictness)
    assert 'name' in obj1['metadata']
    assert 'name' in obj2['metadata']
    assert 'generateName' not in obj1['metadata']
    assert 'generateName' not in obj2['metadata']
    assert obj1['metadata']['name'] == 'provided-name'
    assert obj2['metadata']['name'] == 'provided-name'


@obj1_without_names
@strict_mode
@any_forced_mode
def test_assignment_of_strict_name_of_pykube_object(forcedness, strictness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.harmonize_naming(pykube_object, name='provided-name', **forcedness, **strictness)
    assert pykube_object.obj['metadata'].get('name') == 'provided-name'
    assert pykube_object.obj['metadata'].get('generateName') is None


@strict_mode
@any_forced_mode
def test_assignment_of_strict_name_of_kubernetes_model(forcedness, strictness, kubernetes_model):
    kubernetes_model.metadata = None
    kopf.harmonize_naming(kubernetes_model, name='provided-name', **forcedness, **strictness)
    assert kubernetes_model.metadata.name == 'provided-name'
    assert kubernetes_model.metadata.generate_name is None


@obj1_without_names
@non_strict_mode
@any_forced_mode
def test_assignment_of_nonstrict_name_of_dict(forcedness, strictness, obj1):
    obj1 = copy.deepcopy(obj1)
    kopf.harmonize_naming(obj1, name='provided-name', **forcedness, **strictness)
    assert 'name' not in obj1['metadata']
    assert 'generateName' in obj1['metadata']
    assert obj1['metadata']['generateName'] == 'provided-name-'


@obj2_without_names
@obj1_without_names
@non_strict_mode
@any_forced_mode
def test_assignment_of_nonstrict_names_of_dicts(forcedness, strictness, multicls, obj1, obj2):
    obj1, obj2 = copy.deepcopy(obj1), copy.deepcopy(obj2)
    objs = multicls([obj1, obj2])
    kopf.harmonize_naming(objs, name='provided-name', **forcedness, **strictness)
    assert 'name' not in obj1['metadata']
    assert 'name' not in obj2['metadata']
    assert 'generateName' in obj1['metadata']
    assert 'generateName' in obj2['metadata']
    assert obj1['metadata']['generateName'] == 'provided-name-'
    assert obj2['metadata']['generateName'] == 'provided-name-'


@obj1_without_names
@non_strict_mode
@any_forced_mode
def test_assignment_of_nonstrict_name_of_pykube_object(forcedness, strictness, pykube_object, obj1):
    pykube_object.obj = copy.deepcopy(obj1)
    kopf.harmonize_naming(pykube_object, name='provided-name', **forcedness, **strictness)
    assert pykube_object.obj['metadata'].get('name') is None
    assert pykube_object.obj['metadata'].get('generateName') == 'provided-name-'


@non_strict_mode
@any_forced_mode
def test_assignment_of_nonstrict_name_of_kubernetes_model(forcedness, strictness, kubernetes_model):
    kubernetes_model.metadata = None
    kopf.harmonize_naming(kubernetes_model, name='provided-name', **forcedness, **strictness)
    assert kubernetes_model.metadata.name is None
    assert kubernetes_model.metadata.generate_name == 'provided-name-'
