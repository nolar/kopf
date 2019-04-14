import kopf


#
# If namespace is set, it should be preserved, unmodified.
#

def test_preserved_namespace_of_dict():
    obj = {'metadata': {'namespace': 'preexisting-namespace'}}

    kopf.adjust_namespace(obj, namespace='provided-namespace')

    assert 'namespace' in obj['metadata']
    assert obj['metadata']['namespace'] == 'preexisting-namespace'


def test_preserved_namespace_of_multiple_objects(multicls):
    obj1 = {'metadata': {'namespace': 'preexisting-namespace-1'}}
    obj2 = {'metadata': {'namespace': 'preexisting-namespace-2'}}
    objs = multicls([obj1, obj2])

    kopf.adjust_namespace(objs, namespace='provided-namespace')

    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] == 'preexisting-namespace-1'

    assert 'namespace' in obj2['metadata']
    assert obj2['metadata']['namespace'] == 'preexisting-namespace-2'

#
# If the namespace is absent, it should be assigned as provided.
#

def test_assigned_namespace_of_dict():
    obj = {}

    kopf.adjust_namespace(obj, namespace='provided-namespace')

    assert 'namespace' in obj['metadata']
    assert obj['metadata']['namespace'] == 'provided-namespace'


def test_assigned_namespace_of_multiple_objects(multicls):
    obj1 = {}
    obj2 = {}
    objs = multicls([obj1, obj2])

    kopf.adjust_namespace(objs, namespace='provided-namespace')

    assert 'namespace' in obj1['metadata']
    assert obj1['metadata']['namespace'] == 'provided-namespace'

    assert 'namespace' in obj2['metadata']
    assert obj2['metadata']['namespace'] == 'provided-namespace'
