import pytest

from kopf.structs.references import EVERYTHING, Selector


def test_no_args():
    with pytest.raises(TypeError) as err:
        Selector()
    assert "Unspecific resource with no names" in str(err.value)


def test_no_name_but_group():
    with pytest.raises(TypeError) as err:
        Selector(group='group1')
    assert "Unspecific resource with no names" in str(err.value)


def test_no_name_but_version():
    with pytest.raises(TypeError) as err:
        Selector(version='version1')
    assert "Unspecific resource with no names" in str(err.value)


@pytest.mark.parametrize('name', ['name1', EVERYTHING])
def test_one_arg_with_only_name(name):
    selector = Selector(name)
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == name


@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_one_arg_with_group(group):
    selector = Selector(f'name1.{group}')
    assert selector.group == group
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == 'name1'


@pytest.mark.parametrize('version', ['v1', 'v99', 'v99beta99', 'v99alpha99'])
@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_one_arg_with_group_and_conventional_version(version, group):
    selector = Selector(f'name1.{version}.{group}')
    assert selector.group == group
    assert selector.version == version
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == 'name1'


@pytest.mark.parametrize('name', ['name1', EVERYTHING])
@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_two_args_with_only_group(group, name):
    selector = Selector(f'{group}', name)
    assert selector.group == group
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == name


@pytest.mark.parametrize('name', ['name1', EVERYTHING])
@pytest.mark.parametrize('version', ['v1', 'v99', 'v99beta99', 'v99alpha99'])
@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_two_args_with_group_and_conventional_version(version, group, name):
    selector = Selector(f'{group}/{version}', name)
    assert selector.group == group
    assert selector.version == version
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == name


@pytest.mark.parametrize('name', ['name1', EVERYTHING])
def test_two_args_and_corev1(name):
    selector = Selector('v1', name)
    assert selector.group == ''
    assert selector.version == 'v1'
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == name


@pytest.mark.parametrize('name', ['name1', EVERYTHING])
@pytest.mark.parametrize('version', ['v1', 'v99', 'v99beta99', 'v99alpha99'])
@pytest.mark.parametrize('group', ['group1', 'group1.example.com', 'v1nonconventional'])
def test_three_args(group, version, name):
    selector = Selector(group, version, name)
    assert selector.group == group
    assert selector.version == version
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == name


def test_too_many_args():
    with pytest.raises(TypeError) as err:
        Selector('group1', 'version1', 'name1', 'etc')
    assert "Too many positional arguments" in str(err.value)


def test_kwarg_group():
    selector = Selector(group='group1', any_name='whatever')
    assert selector.group == 'group1'
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == 'whatever'


def test_kwarg_version():
    selector = Selector(version='version1', any_name='whatever')
    assert selector.group is None
    assert selector.version == 'version1'
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == 'whatever'


def test_kwarg_kind():
    selector = Selector(kind='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind == 'name1'
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name is None


def test_kwarg_plural():
    selector = Selector(plural='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural == 'name1'
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name is None


def test_kwarg_singular():
    selector = Selector(singular='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular == 'name1'
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name is None


def test_kwarg_shortcut():
    selector = Selector(shortcut='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut == 'name1'
    assert selector.category is None
    assert selector.any_name is None


def test_kwarg_category():
    selector = Selector(category='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category == 'name1'
    assert selector.any_name is None


def test_kwarg_any_name():
    selector = Selector(any_name='name1')
    assert selector.group is None
    assert selector.version is None
    assert selector.kind is None
    assert selector.plural is None
    assert selector.singular is None
    assert selector.shortcut is None
    assert selector.category is None
    assert selector.any_name == 'name1'


@pytest.mark.parametrize('kwargs', [
    {kwarg1: 'name1', kwarg2: 'name2'}
    for kwarg1 in ['kind', 'plural', 'singular', 'shortcut', 'category', 'any_name']
    for kwarg2 in ['kind', 'plural', 'singular', 'shortcut', 'category', 'any_name']
    if kwarg1 != kwarg2
])
def test_conflicting_names_with_only_kwargs(kwargs):
    with pytest.raises(TypeError) as err:
        Selector(**kwargs)
    assert "Ambiguous resource specification with names" in str(err.value)


@pytest.mark.parametrize('kwarg', ['kind', 'plural', 'singular', 'shortcut', 'category'])
def test_conflicting_name_with_positional_name(kwarg):
    with pytest.raises(TypeError) as err:
        Selector('name1', **{kwarg: 'name2'})
    assert "Ambiguous resource specification with names" in str(err.value)


@pytest.mark.parametrize('kwarg', ['kind', 'plural', 'singular', 'shortcut', 'category'])
def test_empty_names_are_prohibited_with_kwargs(kwarg):
    with pytest.raises(TypeError) as err:
        Selector(**{kwarg: ''})
    assert "Names must not be empty strings" in str(err.value)


def test_empty_names_are_prohibited_with_positionals():
    with pytest.raises(TypeError) as err:
        Selector('')
    assert "Names must not be empty strings" in str(err.value)
