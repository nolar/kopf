from kopf.structs.dicts import walk


def test_over_a_none():
    result = list(walk(None))
    assert len(result) == 0


def test_over_a_dict():
    obj = {}
    result = list(walk(obj))
    assert len(result) == 1
    assert result[0] is obj


def test_over_a_list_of_dicts():
    obj1 = {}
    obj2 = {}
    result = list(walk([obj1, obj2]))
    assert len(result) == 2
    assert result[0] is obj1
    assert result[1] is obj2


def test_over_a_tuple_of_dicts():
    obj1 = {}
    obj2 = {}
    result = list(walk((obj1, obj2)))
    assert len(result) == 2
    assert result[0] is obj1
    assert result[1] is obj2


def test_none_is_ignored():
    obj1 = {}
    obj2 = {}
    result = list(walk([obj1, None, obj2]))
    assert len(result) == 2
    assert result[0] is obj1
    assert result[1] is obj2


def test_simple_nested():
    obj1 = {'field': {'subfield': 'val'}}
    obj2 = {'field': {}}
    result = list(walk([obj1, obj2], nested=['field.subfield']))
    assert len(result) == 3
    assert result[0] is obj1
    assert result[1] == 'val'
    assert result[2] is obj2


def test_double_nested():
    obj1 = {'field': {'subfield': 'val'}}
    obj2 = {'field': {}}
    result = list(walk([obj1, obj2], nested=['field.subfield', 'field']))
    assert len(result) == 5
    assert result[0] is obj1
    assert result[1] == 'val'
    assert result[2] == {'subfield': 'val'}
    assert result[3] is obj2
    assert result[4] == {}
