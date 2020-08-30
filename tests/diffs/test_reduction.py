import pytest

from kopf.structs.diffs import Diff, DiffItem, DiffOperation, reduce

DIFF = Diff([
    DiffItem(DiffOperation.ADD   , ('key1',), None, 'new1'),
    DiffItem(DiffOperation.CHANGE, ('key2',), 'old2', 'new2'),
    DiffItem(DiffOperation.ADD   , ('key2', 'suba'), 'olda', 'newa'),
    DiffItem(DiffOperation.REMOVE, ('key2', 'subb'), 'oldb', 'newb'),
    DiffItem(DiffOperation.REMOVE, ('key3',), 'old3', None),
    DiffItem(DiffOperation.CHANGE, ('key4',),
             {'suba': 'olda', 'subc': 'oldc'},
             {'subb': 'newb', 'subc': 'newc'}),
])


@pytest.mark.parametrize('diff', [
    [['op', ['key', 'sub'], 'old', 'new']],
    [['op', ('key', 'sub'), 'old', 'new']],
    [('op', ['key', 'sub'], 'old', 'new')],
    [('op', ('key', 'sub'), 'old', 'new')],
    (['op', ['key', 'sub'], 'old', 'new'],),
    (['op', ('key', 'sub'), 'old', 'new'],),
    (('op', ['key', 'sub'], 'old', 'new'),),
    (('op', ('key', 'sub'), 'old', 'new'),),
], ids=[
    'lll-diff', 'llt-diff', 'ltl-diff', 'ltt-diff',
    'tll-diff', 'tlt-diff', 'ttl-diff', 'ttt-diff',
])
@pytest.mark.parametrize('path', [
    ['key', 'sub'],
    ('key', 'sub'),
], ids=['list-path', 'tuple-path'])
def test_type_ignored_for_inputs_but_is_tuple_for_output(diff, path):
    result = reduce(diff, path)
    assert result == (('op', (), 'old', 'new'),)


def test_empty_path_selects_all_ops():
    result = reduce(DIFF, [])
    assert result == DIFF


def test_existent_path_selects_relevant_ops():
    result = reduce(DIFF, ['key2'])
    assert result == (
        ('change', (), 'old2', 'new2'),
        ('add'   , ('suba',), 'olda', 'newa'),
        ('remove', ('subb',), 'oldb', 'newb'),
    )


@pytest.mark.parametrize('path', [
    ['nonexistent-key'],
    ['key1', 'nonexistent-key'],
    ['key2', 'nonexistent-key'],
    ['key3', 'nonexistent-key'],
    ['key4', 'nonexistent-key'],
    ['key4', 'suba', 'nonexistent-key'],
    ['key4', 'subb', 'nonexistent-key'],
    ['key4', 'subc', 'nonexistent-key'],
    ['key4', 'nonexistent-dict', 'nonexistent-key'],
])
def test_nonexistent_path_selects_nothing(path):
    result = reduce(DIFF, path)
    assert result == ()


def test_overly_specific_path_dives_into_dicts_for_addition():
    result = reduce(DIFF, ['key4', 'subb'])
    assert result == (
        ('add', (), None, 'newb'),
    )


def test_overly_specific_path_dives_into_dicts_for_removal():
    result = reduce(DIFF, ['key4', 'suba'])
    assert result == (
        ('remove', (), 'olda', None),
    )


def test_overly_specific_path_dives_into_dicts_for_change():
    result = reduce(DIFF, ['key4', 'subc'])
    assert result == (
        ('change', (), 'oldc', 'newc'),
    )
