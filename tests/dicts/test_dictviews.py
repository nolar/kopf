import pytest

from kopf.structs.dicts import MappingView, MutableMappingView, ReplaceableMappingView

all_classes = pytest.mark.parametrize('cls', [
    MappingView, MutableMappingView, ReplaceableMappingView])


@all_classes
def test_creation_with_source(cls):
    cls({})


@all_classes
def test_creation_without_source(cls):
    with pytest.raises(TypeError):
        cls()


@all_classes
def test_access_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    value = view['field']
    assert value == 'value'


@all_classes
def test_access_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    value = view['field']
    assert value == 'value'


@all_classes
def test_unexistent_key_for_root(cls):
    data = {}
    view = cls(data)
    with pytest.raises(KeyError):
        view['field']


@all_classes
def test_unexistent_key_for_path(cls):
    data = {}
    view = cls(data, 'spec')
    with pytest.raises(KeyError):
        view['field']


@all_classes
def test_live_access_for_root(cls):
    data = {}
    view = cls(data)
    data['field'] = 'value'
    value = view['field']
    assert value == 'value'


@all_classes
def test_live_access_for_path(cls):
    data = {}
    view = cls(data, 'spec')
    data['spec'] = {'field': 'value'}
    value = view['field']
    assert value == 'value'


@all_classes
def test_length_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert len(view) == 1


@all_classes
def test_length_for_path(cls):
    data = {'spec': {'field': 'value'}, 'extra-key': None}
    view = cls(data, 'spec')
    assert len(view) == 1


@all_classes
def test_bool_true_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert bool(view)


@all_classes
def test_bool_false_for_root(cls):
    data = {}
    view = cls(data)
    assert not bool(view)


@all_classes
def test_bool_true_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    assert bool(view)


@all_classes
def test_bool_false_for_path(cls):
    data = {}
    view = cls(data, 'spec')
    assert not bool(view)


@all_classes
def test_keys_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert list(view.keys()) == ['field']


@all_classes
def test_keys_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    assert list(view.keys()) == ['field']


@all_classes
def test_values_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert list(view.values()) == ['value']


@all_classes
def test_values_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    assert list(view.values()) == ['value']


@all_classes
def test_items_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert list(view.items()) == [('field', 'value')]


@all_classes
def test_items_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    assert list(view.items()) == [('field', 'value')]


@all_classes
def test_dict_for_root(cls):
    data = {'field': 'value'}
    view = cls(data)
    assert dict(view) == {'field': 'value'}


@all_classes
def test_dict_for_path(cls):
    data = {'spec': {'field': 'value'}}
    view = cls(data, 'spec')
    assert dict(view) == {'field': 'value'}


def test_update_for_root():
    data = {'field': 'value'}
    view = MutableMappingView(data)
    view['field'] = 'new-value'
    assert data == {'field': 'new-value'}


def test_update_for_path():
    data = {'spec': {'field': 'value'}}
    view = MutableMappingView(data, 'spec')
    view['field'] = 'new-value'
    assert data == {'spec': {'field': 'new-value'}}


def test_delete_for_root():
    data = {'field': 'value'}
    view = MutableMappingView(data)
    del view['field']
    assert data == {}


def test_delete_for_path():
    data = {'spec': {'field': 'value'}}
    view = MutableMappingView(data, 'spec')
    del view['field']
    assert data == {'spec': {}}


def test_chain_creation():
    data = {}
    view = MutableMappingView(data, 'spec')
    view['field'] = 'new-value'
    assert data == {'spec': {'field': 'new-value'}}


def test_replacing_from_another_view():
    data1 = {'field1': 'value1'}
    data2 = {'field2': 'value2'}
    view1 = ReplaceableMappingView(data1)
    view2 = ReplaceableMappingView(data2)
    view1._replace_from(view2)
    assert view1._src is not view2
    assert view1._src is view2._src
    assert dict(view1) == {'field2': 'value2'}


def test_replacing_with_another_view():
    data1 = {'field1': 'value1'}
    data2 = {'field2': 'value2'}
    view1 = ReplaceableMappingView(data1)
    view2 = ReplaceableMappingView(data2)
    view1._replace_with(view2)
    assert view1._src is view2
    assert dict(view1) == {'field2': 'value2'}


def test_replacing_with_regular_dict():
    data1 = {'field1': 'value1'}
    data2 = {'field2': 'value2'}
    view1 = ReplaceableMappingView(data1)
    view1._replace_with(data2)
    assert view1._src is data2
    assert dict(view1) == {'field2': 'value2'}
