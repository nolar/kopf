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


def test_adding_to_multiple_objects(multicls):
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


def test_forcing_true():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'}, force=True)
    assert obj['metadata']['labels']['label'] == 'new-value'


def test_forcing_false():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'}, force=False)
    assert obj['metadata']['labels']['label'] == 'old-value'


def test_forcing_default():
    obj = {'metadata': {'labels': {'label': 'old-value'}}}
    kopf.label(obj, {'label': 'new-value'})
    assert obj['metadata']['labels']['label'] == 'old-value'


def test_nested_with_forced_true():
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'template': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=['spec.template'], force=True)
    assert obj['metadata']['labels']['label'] == 'new-value'
    assert obj['spec']['template']['metadata']['labels']['label'] == 'new-value'


def test_nested_with_forced_false():
    obj = {'metadata': {'labels': {'label': 'old-value'}},
           'spec': {'template': {}}}
    kopf.label(obj, {'label': 'new-value'}, nested=['spec.template'], force=False)
    assert obj['metadata']['labels']['label'] == 'old-value'
    assert obj['spec']['template']['metadata']['labels']['label'] == 'new-value'
