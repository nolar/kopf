import pytest

from kopf.structs.references import EVERYTHING, Resource, Selector


@pytest.fixture()
def resource():
    return Resource(
        group='group1', version='version1', preferred=True,
        plural='plural1', singular='singular1', kind='kind1',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
        subresources=[], verbs=[], namespaced=True,  # not used in matching
    )


@pytest.fixture()
def v1_resource():
    return Resource(
        group='', version='v1', preferred=True,
        plural='plural1', singular='singular1', kind='kind1',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
        subresources=[], verbs=[], namespaced=True,  # not used in matching
    )


@pytest.mark.parametrize('group, version', [
    (None, None),
    ('group1', None),
    (None, 'version1'),
    ('group1', 'version1'),
])
def test_when_matches_everything(resource, group, version):
    selector = Selector(EVERYTHING, group=group, version=version)
    matches = selector.check(resource)
    assert matches


@pytest.mark.parametrize('kwarg, kwval', [
    ('kind', 'kind1'),
    ('plural', 'plural1'),
    ('singular', 'singular1'),
    ('shortcut', 'shortcut1'),
    ('shortcut', 'shortcut2'),
    ('category', 'category1'),
    ('category', 'category2'),
    ('any_name', 'kind1'),
    ('any_name', 'plural1'),
    ('any_name', 'singular1'),
    ('any_name', 'shortcut1'),
    ('any_name', 'shortcut2'),
])
@pytest.mark.parametrize('group, version', [
    (None, None),
    ('group1', None),
    (None, 'version1'),
    ('group1', 'version1'),
])
def test_when_matches_names(resource, kwarg, kwval, group, version):
    selector = Selector(group=group, version=version, **{kwarg: kwval})
    matches = selector.check(resource)
    assert matches


@pytest.mark.parametrize('kwarg, kwval', [
    ('kind', 'kind1'),
    ('plural', 'plural1'),
    ('singular', 'singular1'),
    ('shortcut', 'shortcut1'),
    ('shortcut', 'shortcut2'),
    ('category', 'category1'),
    ('category', 'category2'),
    ('any_name', 'kind1'),
    ('any_name', 'plural1'),
    ('any_name', 'singular1'),
    ('any_name', 'shortcut1'),
    ('any_name', 'shortcut2'),
])
@pytest.mark.parametrize('group, version', [
    ('group9', None),
    (None, 'version9'),
    ('group1', 'version9'),
    ('group9', 'version1'),
    ('group9', 'version9'),
])
def test_when_groupversion_mismatch_but_names_do_match(resource, kwarg, kwval, group, version):
    selector = Selector(group=group, version=version, **{kwarg: kwval})
    matches = selector.check(resource)
    assert not matches


@pytest.mark.parametrize('kwarg, kwval', [
    ('kind', 'kind9'),
    ('plural', 'plural9'),
    ('singular', 'singular9'),
    ('shortcut', 'shortcut9'),
    ('category', 'category9'),
    ('any_name', 'category9'),
    ('any_name', 'category1'),  # categories are not used with any_name, must be explicit.
    ('any_name', 'category2'),  # categories are not used with any_name, must be explicit.
])
@pytest.mark.parametrize('group, version', [
    (None, None),
    ('group1', None),
    (None, 'version1'),
    ('group1', 'version1'),
])
def test_when_groupversion_do_match_but_names_mismatch(resource, kwarg, kwval, group, version):
    selector = Selector(group=group, version=version, **{kwarg: kwval})
    matches = selector.check(resource)
    assert not matches


def test_catchall_versions_are_ignored_for_nonpreferred_resources():
    resource = Resource(
        group='group1', version='version1', preferred=False,
        plural='plural1', singular='singular1', kind='kind1',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
        subresources=[], verbs=[], namespaced=True,  # not used in matching
    )
    selector = Selector(EVERYTHING)
    matches = selector.check(resource)
    assert not matches


@pytest.mark.parametrize('kwarg, kwval', [
    ('kind', 'kind1'),
    ('plural', 'plural1'),
    ('singular', 'singular1'),
    ('shortcut', 'shortcut1'),
    ('shortcut', 'shortcut2'),
    ('any_name', 'kind1'),
    ('any_name', 'plural1'),
    ('any_name', 'singular1'),
    ('any_name', 'shortcut1'),
    ('any_name', 'shortcut2'),
])
def test_selection_of_specific_resources(resource, kwarg, kwval):
    selector = Selector(**{kwarg: kwval})
    selected = selector.select([resource])
    assert selector.is_specific  # prerequisite
    assert selected == {resource}


@pytest.mark.parametrize('kwarg, kwval', [
    ('category', 'category1'),
    ('category', 'category2'),
    ('any_name', EVERYTHING),
])
def test_selection_of_nonspecific_resources(resource, kwarg, kwval):
    selector = Selector(**{kwarg: kwval})
    selected = selector.select([resource])
    assert not selector.is_specific  # prerequisite
    assert selected == {resource}


@pytest.mark.parametrize('kwarg, kwval', [
    ('kind', 'kind1'),
    ('plural', 'plural1'),
    ('singular', 'singular1'),
    ('shortcut', 'shortcut1'),
    ('shortcut', 'shortcut2'),
    ('any_name', 'kind1'),
    ('any_name', 'plural1'),
    ('any_name', 'singular1'),
    ('any_name', 'shortcut1'),
    ('any_name', 'shortcut2'),
])
def test_precedence_of_corev1_over_others_when_specific(resource, v1_resource, kwarg, kwval):
    selector = Selector(**{kwarg: kwval})
    selected = selector.select([resource, v1_resource])
    assert selector.is_specific  # prerequisite
    assert selected == {v1_resource}


@pytest.mark.parametrize('kwarg, kwval', [
    ('category', 'category1'),
    ('category', 'category2'),
    ('any_name', EVERYTHING),
])
def test_precedence_of_corev1_same_as_others_when_nonspecific(resource, v1_resource, kwarg, kwval):
    selector = Selector(**{kwarg: kwval})
    selected = selector.select([resource, v1_resource])
    assert not selector.is_specific  # prerequisite
    assert selected == {resource, v1_resource}
