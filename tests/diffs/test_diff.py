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
