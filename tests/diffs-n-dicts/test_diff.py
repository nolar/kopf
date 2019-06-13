from kopf.structs.diffs import diff


def test_scalars_equal():
    a = 100
    b = 100
    d = diff(a, b)
    assert d == ()


def test_scalars_unequal():
    a = 100
    b = 200
    d = diff(a, b)
    assert d == (('change', (), 100, 200),)


def test_strings_equal():
    a = 'hello'
    b = 'hello'
    d = diff(a, b)
    assert d == ()


def test_strings_unequal():
    a = 'hello'
    b = 'world'
    d = diff(a, b)
    assert d == (('change', (), 'hello', 'world'),)


def test_lists_equal():
    a = [100, 200, 300]
    b = [100, 200, 300]
    d = diff(a, b)
    assert d == ()


def test_lists_unequal():
    a = [100, 200, 300]
    b = [100, 666, 300]
    d = diff(a, b)
    assert d == (('change', (), [100, 200, 300], [100, 666, 300]),)


def test_dicts_equal():
    a = {'hello': 'world', 'key': 'val'}
    b = {'key': 'val', 'hello': 'world'}
    d = diff(a, b)
    assert d == ()


def test_dicts_with_keys_added():
    a = {'hello': 'world'}
    b = {'hello': 'world', 'key': 'val'}
    d = diff(a, b)
    assert d == (('add', ('key',), None, 'val'),)


def test_dicts_with_keys_removed():
    a = {'hello': 'world', 'key': 'val'}
    b = {'hello': 'world'}
    d = diff(a, b)
    assert d == (('remove', ('key',), 'val', None),)


def test_dicts_with_keys_changed():
    a = {'hello': 'world', 'key': 'old'}
    b = {'hello': 'world', 'key': 'new'}
    d = diff(a, b)
    assert d == (('change', ('key',), 'old', 'new'),)


def test_dicts_with_subkeys_changed():
    a = {'main': {'hello': 'world', 'key': 'old'}}
    b = {'main': {'hello': 'world', 'key': 'new'}}
    d = diff(a, b)
    assert d == (('change', ('main', 'key'), 'old', 'new'),)


def test_dicts_adding_label():
    body_before_labelling = {'metadata': {}}
    body_after_labelling  = {'metadata': {'labels': 'LABEL'}}

    d = diff(body_before_labelling, body_after_labelling)
    assert d == (('add', ('metadata', 'labels'), None, 'LABEL'),)


def test_dicts_updating_storage_size():
    body_before_storage_size_update = {'spec': {'size': '42G'}}
    body_after_storage_size_update  = {'spec': {'size': '76G'}}

    d = diff(body_before_storage_size_update, body_after_storage_size_update)
    assert d == (('change', ('spec', 'size'), '42G', '76G'),)


def test_dicts_different_items_handled():
    body_before_storage_size_update = {'spec': {'items': ['task1', 'task2']}}
    body_after_storage_size_update  = {'spec': {'items': ['task3', 'task4']}}

    d = diff(body_before_storage_size_update, body_after_storage_size_update)
    assert d == (('change', ('spec', 'items'), ['task1', 'task2'], ['task3', 'task4']),)
