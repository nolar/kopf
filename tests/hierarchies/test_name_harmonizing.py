import pytest

import kopf


@pytest.fixture(params=[
    dict(strict=True),
    dict(strict=False),
    dict(),
], ids=['strict', 'relaxed', 'default'])
def all_modes_kwargs(request):
    return request.param


@pytest.fixture(params=[
    dict(strict=True),
], ids=['strict'])
def strict_kwargs(request):
    return request.param


@pytest.fixture(params=[
    dict(strict=False),
    dict(),  # no kwargs, the function defaults are used.
], ids=['relaxed', 'default'])
def relaxed_kwargs(request):
    return request.param

#
# No matter what `strict` mode is, the pre-existing names are preserved.
#

def test_preserved_name_of_dict(all_modes_kwargs):
    obj = {'metadata': {'name': 'preexisting-name'}}

    kopf.harmonize_naming(obj, name='provided-name', **all_modes_kwargs)

    assert 'name' in obj['metadata']
    assert 'generateName' not in obj['metadata']
    assert obj['metadata']['name'] == 'preexisting-name'


def test_preserved_names_of_multiple_objects(all_modes_kwargs, multicls):
    obj1 = {'metadata': {'name': 'preexisting-name-1'}}
    obj2 = {'metadata': {'name': 'preexisting-name-2'}}
    objs = multicls([obj1, obj2])

    kopf.harmonize_naming(objs, name='provided-name', **all_modes_kwargs)

    assert 'name' in obj1['metadata']
    assert 'generateName' not in obj1['metadata']
    assert obj1['metadata']['name'] == 'preexisting-name-1'

    assert 'name' in obj2['metadata']
    assert 'generateName' not in obj2['metadata']
    assert obj2['metadata']['name'] == 'preexisting-name-2'

#
# In strict mode and with the absent names, the provided name is used.
#

def test_assigned_name_of_dict(strict_kwargs):
    obj = {}

    kopf.harmonize_naming(obj, name='provided-name', **strict_kwargs)

    assert 'name' in obj['metadata']
    assert 'generateName' not in obj['metadata']
    assert obj['metadata']['name'] == 'provided-name'


def test_assigned_names_of_multiple_objects(strict_kwargs, multicls):
    obj1 = {'metadata': {'name': 'preexisting-name-1'}}
    obj2 = {'metadata': {'name': 'preexisting-name-2'}}
    objs = multicls([obj1, obj2])

    kopf.harmonize_naming(objs, name='provided-name', **strict_kwargs)

    assert 'name' in obj1['metadata']
    assert 'generateName' not in obj1['metadata']
    assert obj1['metadata']['name'] == 'preexisting-name-1'

    assert 'name' in obj2['metadata']
    assert 'generateName' not in obj2['metadata']
    assert obj2['metadata']['name'] == 'preexisting-name-2'

#
# In relaxed mode, if the names are absent, they are auto-generated.
#

def test_prefixed_name_of_dict(relaxed_kwargs):
    obj = {}

    kopf.harmonize_naming(obj, name='provided-name', **relaxed_kwargs)

    assert 'name' not in obj['metadata']
    assert 'generateName' in obj['metadata']
    assert obj['metadata']['generateName'] == 'provided-name-'


def test_prefixed_names_of_multiple_objects(relaxed_kwargs, multicls):
    obj1 = {}
    obj2 = {}
    objs = multicls([obj1, obj2])

    kopf.harmonize_naming(objs, name='provided-name', **relaxed_kwargs)

    assert 'name' not in obj1['metadata']
    assert 'generateName' in obj1['metadata']
    assert obj1['metadata']['generateName'] == 'provided-name-'

    assert 'name' not in obj2['metadata']
    assert 'generateName' in obj2['metadata']
    assert obj2['metadata']['generateName'] == 'provided-name-'
