import pytest

from kopf.structs.references import EVERYTHING, Selector


@pytest.mark.parametrize('kw', ['kind', 'plural', 'singular', 'shortcut', 'category', 'any_name'])
def test_repr_with_names(kw):
    selector = Selector(**{kw: 'name1'})
    selector_repr = repr(selector)
    assert selector_repr == f"Selector({kw}='name1')"


@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_repr_with_group(group):
    selector = Selector(group=group, any_name='name1')
    selector_repr = repr(selector)
    assert selector_repr == f"Selector(group='{group}', any_name='name1')"


@pytest.mark.parametrize('kw', ['kind', 'plural', 'singular', 'shortcut', 'any_name'])
def test_is_specific_with_names(kw):
    selector = Selector(**{kw: 'name1'})
    assert selector.is_specific


@pytest.mark.parametrize('kw', ['category'])
def test_is_not_specific_with_categories(kw):
    selector = Selector(**{kw: 'name1'})
    assert not selector.is_specific


@pytest.mark.parametrize('kw', ['category'])
def test_is_not_specific_with_categories(kw):
    selector = Selector(EVERYTHING)
    assert not selector.is_specific


@pytest.mark.parametrize('kw', ['category'])
def test_is_not_specific_with_categories(kw):
    selector = Selector(**{kw: 'name1'})
    assert not selector.is_specific
