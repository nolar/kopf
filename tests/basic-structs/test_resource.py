import pytest

from kopf.structs.references import Resource


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


def test_url_for_a_list_of_clusterscoped_custom_resources_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    url = resource.get_url(namespace=None)
    assert url == '/apis/group/version/plural'


def test_url_for_a_list_of_clusterscoped_custom_resources_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_for_a_list_of_namespaced_custom_resources_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    url = resource.get_url(namespace=None)
    assert url == '/apis/group/version/plural'


def test_url_for_a_list_of_namespaced_custom_resources_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural'


def test_url_for_a_specific_clusterscoped_custom_resource_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    url = resource.get_url(namespace=None, name='name-a.b')
    assert url == '/apis/group/version/plural/name-a.b'


def test_url_for_a_specific_clusterscoped_custom_resource_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', name='name-a.b')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_for_a_specific_namespaced_custom_resource_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, name='name-a.b')
    assert str(err.value) == "Specific namespaces are required for specific namespaced resources."


def test_url_for_a_specific_namespaced_custom_resource_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural/name-a.b'


def test_url_for_a_list_of_clusterscoped_corev1_resources_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    url = resource.get_url(namespace=None)
    assert url == '/api/v1/plural'


def test_url_for_a_list_of_clusterscoped_corev1_resources_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_for_a_list_of_namespaced_corev1_resources_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    url = resource.get_url(namespace=None)
    assert url == '/api/v1/plural'


def test_url_for_a_list_of_namespaced_corev1_resources_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b')
    assert url == '/api/v1/namespaces/ns-a.b/plural'


def test_url_of_a_specific_clusterscoped_corev1_resource_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    url = resource.get_url(namespace=None, name='name-a.b')
    assert url == '/api/v1/plural/name-a.b'


def test_url_of_a_specific_clusterscoped_corev1_resource_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', name='name-a.b')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_of_a_specific_namespaced_corev1_resource_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, name='name-a.b')
    assert str(err.value) == "Specific namespaces are required for specific namespaced resources."


def test_url_of_a_specific_namespaced_corev1_resource_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b')
    assert url == '/api/v1/namespaces/ns-a.b/plural/name-a.b'


def test_url_with_arbitrary_params():
    resource = Resource('group', 'version', 'plural')
    url = resource.get_url(params=dict(watch='true', resourceVersion='abc%def xyz'))
    assert url == '/apis/group/version/plural?watch=true&resourceVersion=abc%25def+xyz'


def test_url_for_a_list_of_clusterscoped_custom_subresources_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_list_of_clusterscoped_custom_subresources_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', subresource='status')
    assert str(err.value) in {
        "Specific namespaces are not supported for cluster-scoped resources.",
        "Subresources can be used only with specific resources by their name.",
    }


def test_url_for_a_list_of_namespaced_custom_subresources_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_list_of_namespaced_custom_subresources_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns-a.b', subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_specific_clusterscoped_custom_subresource_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    url = resource.get_url(namespace=None, name='name-a.b', subresource='status')
    assert url == '/apis/group/version/plural/name-a.b/status'


def test_url_for_a_specific_clusterscoped_custom_subresource_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', name='name-a.b', subresource='status')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_for_a_specific_namespaced_custom_subresource_clusterwide():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, name='name-a.b', subresource='status')
    assert str(err.value) == "Specific namespaces are required for specific namespaced resources."


def test_url_for_a_specific_namespaced_custom_subresource_in_a_namespace():
    resource = Resource('group', 'version', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b', subresource='status')
    assert url == '/apis/group/version/namespaces/ns-a.b/plural/name-a.b/status'


def test_url_for_a_list_of_clusterscoped_corev1_subresources_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_list_of_clusterscoped_corev1_subresources_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', subresource='status')
    assert str(err.value) in {
        "Specific namespaces are not supported for cluster-scoped resources.",
        "Subresources can be used only with specific resources by their name.",
    }


def test_url_for_a_list_of_namespaced_corev1_subresources_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_list_of_namespaced_corev1_subresources_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns-a.b', subresource='status')
    assert str(err.value) == "Subresources can be used only with specific resources by their name."


def test_url_for_a_specific_clusterscoped_corev1_subresource_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    url = resource.get_url(namespace=None, name='name-a.b', subresource='status')
    assert url == '/api/v1/plural/name-a.b/status'


def test_url_for_a_specific_clusterscoped_corev1_subresource_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=False)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace='ns', name='name-a.b', subresource='status')
    assert str(err.value) == "Specific namespaces are not supported for cluster-scoped resources."


def test_url_for_a_specific_namespaced_corev1_subresource_clusterwide():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    with pytest.raises(ValueError) as err:
        resource.get_url(namespace=None, name='name-a.b', subresource='status')
    assert str(err.value) == "Specific namespaces are required for specific namespaced resources."


def test_url_for_a_specific_namespaced_corev1_subresource_in_a_namespace():
    resource = Resource('', 'v1', 'plural', namespaced=True)
    url = resource.get_url(namespace='ns-a.b', name='name-a.b', subresource='status')
    assert url == '/api/v1/namespaces/ns-a.b/plural/name-a.b/status'
