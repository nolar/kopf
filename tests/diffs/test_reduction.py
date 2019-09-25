import pytest

from kopf.structs.diffs import reduce, Diff, DiffItem, DiffOperation


DIFF = Diff([
    DiffItem(DiffOperation.ADD   , ('key1',), None, 'new1'),
    DiffItem(DiffOperation.CHANGE, ('key2',), 'old2', 'new2'),
    DiffItem(DiffOperation.ADD   , ('key2', 'suba'), 'olda', 'newa'),
    DiffItem(DiffOperation.REMOVE, ('key2', 'subb'), 'oldb', 'newb'),
    DiffItem(DiffOperation.REMOVE, ('key3',), 'old3', None),
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


def test_nonexistent_path_selects_nothing():
    result = reduce(DIFF, ['nonexistent-key'])
    assert result == ()
