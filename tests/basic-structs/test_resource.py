import pytest

from kopf.structs.references import Resource

DEFAULTS = dict(
    kind='...', singular='...', namespaced=True, preferred=True,
    shortcuts=[], categories=[], subresources=[], verbs=[],
)


def test_creation_with_no_args():
    with pytest.raises(TypeError):
        Resource()


def test_creation_with_all_kwargs():
    resource = Resource(
        group='group',
        version='version',
        plural='plural',
        kind='kind',
        singular='singular',
        shortcuts=['shortcut1', 'shortcut2'],
        categories=['category1', 'category2'],
        subresources=['sub1', 'sub2'],
        namespaced=True,
        preferred=True,
        verbs=['verb1', 'verb2'],
    )
    assert resource.group == 'group'
    assert resource.version == 'version'
    assert resource.plural == 'plural'
    assert resource.kind == 'kind'
    assert resource.singular == 'singular'
    assert resource.shortcuts == ['shortcut1', 'shortcut2']
    assert resource.categories == ['category1', 'category2']
    assert resource.subresources == ['sub1', 'sub2']
    assert resource.namespaced == True
    assert resource.preferred == True
    assert resource.verbs == ['verb1', 'verb2']


def test_api_version_of_custom_resource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    api_version = resource.api_version
    assert api_version == 'group/version'


def test_api_version_of_builtin_resource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    api_version = resource.api_version
    assert api_version == 'v1'


def test_name_of_custom_resource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    name = resource.name
    assert name == 'plural.group'


def test_name_of_builtin_resource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    name = resource.name
    assert name == 'plural'


def test_url_of_custom_resource_list_cluster_scoped():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url()
    assert url == '/apis/group/version/plural'


def test_url_of_custom_resource_list_namespaced():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(namespace='ns-a.b')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural'


def test_url_of_custom_resource_item_cluster_scoped():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b')
    assert url == '/apis/group/version/plural/name-a.b'


def test_url_of_custom_resource_item_namespaced():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural/name-a.b'


def test_url_of_builtin_resource_list_cluster_scoped():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url()
    assert url == '/api/v1/plural'


def test_url_of_builtin_resource_list_namespaced():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url(namespace='ns-a.b')
    assert url == '/api/v1/namespaces/ns-a.b/plural'


def test_url_of_builtin_resource_item_cluster_scoped():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b')
    assert url == '/api/v1/plural/name-a.b'


def test_url_of_builtin_resource_item_namespaced():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b')
    assert url == '/api/v1/namespaces/ns-a.b/plural/name-a.b'


def test_url_with_arbitrary_params():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(params=dict(watch='true', resourceVersion='abc%def xyz'))
    assert url == '/apis/group/version/plural?watch=true&resourceVersion=abc%25def+xyz'


def test_url_of_custom_resource_list_cluster_scoped_with_subresource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    with pytest.raises(ValueError):
        resource.get_url(subresource='status')


def test_url_of_custom_resource_list_namespaced_with_subresource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    with pytest.raises(ValueError):
        resource.get_url(namespace='ns-a.b', subresource='status')


def test_url_of_custom_resource_item_cluster_scoped_with_subresource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b', subresource='status')
    assert url == '/apis/group/version/plural/name-a.b/status'


def test_url_of_custom_resource_item_namespaced_with_subresource():
    resource = Resource('group', 'version', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b', namespace='ns-a.b', subresource='status')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural/name-a.b/status'


def test_url_of_builtin_resource_list_cluster_scoped_with_subresource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    with pytest.raises(ValueError):
        resource.get_url(subresource='status')


def test_url_of_builtin_resource_list_namespaced_with_subresource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    with pytest.raises(ValueError):
        resource.get_url(namespace='ns-a.b', subresource='status')


def test_url_of_builtin_resource_item_cluster_scoped_with_subresource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b', subresource='status')
    assert url == '/api/v1/plural/name-a.b/status'


def test_url_of_builtin_resource_item_namespaced_with_subresource():
    resource = Resource('', 'v1', 'plural', **DEFAULTS)
    url = resource.get_url(name='name-a.b', namespace='ns-a.b', subresource='status')
    assert url == '/api/v1/namespaces/ns-a.b/plural/name-a.b/status'
