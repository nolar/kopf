import pytest

from kopf.structs.references import EVERYTHING, Resource, Selector


@pytest.fixture()
def resource():
    return Resource(
        group='group1', version='version1', preferred=True,
        plural='plural1', singular='singular1', kind='kind1',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
    )


@pytest.fixture()
def v1_resource():
    return Resource(
        group='', version='v1', preferred=True,
        plural='plural1', singular='singular1', kind='kind1',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
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
    )
    selector = Selector(EVERYTHING)
    matches = selector.check(resource)
    assert not matches


@pytest.mark.parametrize('selector_args', [
    pytest.param(['events'], id='only-name'),
    pytest.param(['v1', 'events'], id='with-version'),
    pytest.param(['', 'v1', 'events'], id='with-groupversion'),
])
def test_events_are_matched_when_explicitly_named(selector_args):
    resource = Resource('', 'v1', 'events')
    selector = Selector(*selector_args)
    matches = selector.check(resource)
    assert matches


@pytest.mark.parametrize('selector_args', [
    pytest.param([EVERYTHING], id='only-marker'),
    pytest.param(['v1', EVERYTHING], id='with-core-version'),
    pytest.param(['', 'v1', EVERYTHING], id='with-core-groupversion'),
    pytest.param(['events.k8s.io', EVERYTHING], id='with-k8sio-group'),
    pytest.param(['events.k8s.io', 'v1beta1', EVERYTHING], id='with-k8sio-groupversion'),
])
@pytest.mark.parametrize('resource_kwargs', [
    pytest.param(dict(group='', version='v1'), id='core-v1'),
    pytest.param(dict(group='events.k8s.io', version='v1'), id='k8sio-v1'),
    pytest.param(dict(group='events.k8s.io', version='v1beta1'), id='k8sio-v1beta1'),
])
def test_events_are_excluded_from_everything(resource_kwargs, selector_args):
    resource = Resource(**resource_kwargs, plural='events')
    selector = Selector(*selector_args)
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
