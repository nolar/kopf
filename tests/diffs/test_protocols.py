import pytest

from kopf.structs.diffs import Diff, DiffItem, DiffOperation


@pytest.mark.parametrize('operation', list(DiffOperation))
def test_operation_enum_behaves_as_string(operation: DiffOperation):
    assert isinstance(operation, str)
    assert operation == operation.value
    assert str(operation) == str(operation.value)
    assert repr(operation) == repr(operation.value)


@pytest.mark.parametrize('operation', list(DiffOperation))
def test_item_has_all_expected_properties(operation):
    item = DiffItem(operation, ('field',), 'a', 'b')
    assert item.operation is operation
    assert item.op is operation
    assert item.field == ('field',)
    assert item.old == 'a'
    assert item.new == 'b'


@pytest.mark.parametrize('operation', list(DiffOperation))
def test_item_comparison_to_tuple(operation):
    item = DiffItem(operation.value, (), 'a', 'b')
    assert item == (operation.value, (), 'a', 'b')


@pytest.mark.parametrize('operation', list(DiffOperation))
def test_item_comparison_to_list(operation):
    item = DiffItem(operation.value, (), 'a', 'b')
    assert item == [operation.value, (), 'a', 'b']


@pytest.mark.parametrize('operation', list(DiffOperation))
def test_item_comparison_to_another_item(operation):
    item1 = DiffItem(operation.value, (), 'a', 'b')
    item2 = DiffItem(operation.value, (), 'a', 'b')
    assert item1 == item2


# TODO: later implement it so that the order of items is irrelevant.
def test_diff_comparison_to_the_same():
    d1 = Diff([
        DiffItem(DiffOperation.ADD   , ('key1',), None, 'new1'),
        DiffItem(DiffOperation.CHANGE, ('key2',), 'old2', 'new2'),
        DiffItem(DiffOperation.REMOVE, ('key3',), 'old3', None),
    ])
    d2 = Diff([
        DiffItem(DiffOperation.ADD   , ('key1',), None, 'new1'),
        DiffItem(DiffOperation.CHANGE, ('key2',), 'old2', 'new2'),
        DiffItem(DiffOperation.REMOVE, ('key3',), 'old3', None),
    ])
    assert d1 == d2
