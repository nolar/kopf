import pytest

import kopf
from kopf.reactor.admission import build_webhooks
from kopf.structs.references import Resource


@pytest.fixture()
def handlers(resource, registry):

    @kopf.on.validate(*resource, registry=registry)
    def validate_fn(**_):
        pass

    @kopf.on.mutate(*resource, registry=registry)
    def mutate_fn(**_):
        pass

    return registry._resource_webhooks.get_all_handlers()


@pytest.mark.parametrize('id, field, exp_name', [
    ('id', None, 'id.sfx'),
    ('id.', None, 'id..sfx'),
    ('id-', None, 'id-.sfx'),
    ('id_', None, 'id-.sfx'),
    ('id!', None, 'id21.sfx'),
    ('id%', None, 'id25.sfx'),
    ('id/sub', None, 'id.sub.sfx'),
    ('id', 'fld1.fld2', 'id.fld1.fld2.sfx'),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_name_is_normalised(registry, resource, decorator, id, field, exp_name):

    @decorator(*resource, id=id, field=field, registry=registry)
    def fn(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert webhooks[0]['name'] == exp_name


@pytest.mark.parametrize('id, field, exp_url', [
    ('id', None, 'https://hostname/p1/p2/id'),
    ('id.', None, 'https://hostname/p1/p2/id.'),
    ('id-', None, 'https://hostname/p1/p2/id-'),
    ('id_', None, 'https://hostname/p1/p2/id_'),
    ('id!', None, 'https://hostname/p1/p2/id%21'),
    ('id%', None, 'https://hostname/p1/p2/id%25'),
    ('id/sub', None, 'https://hostname/p1/p2/id/sub'),
    ('id', 'fld1.fld2', 'https://hostname/p1/p2/id/fld1.fld2'),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_url_is_suffixed(registry, resource, decorator, id, field, exp_url):

    @decorator(*resource, id=id, field=field, registry=registry)
    def fn(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={'url': 'https://hostname/p1/p2'})

    assert len(webhooks) == 1
    assert webhooks[0]['clientConfig']['url'] == exp_url


@pytest.mark.parametrize('id, field, exp_path', [
    ('id', None, 'p1/p2/id'),
    ('id.', None, 'p1/p2/id.'),
    ('id-', None, 'p1/p2/id-'),
    ('id_', None, 'p1/p2/id_'),
    ('id!', None, 'p1/p2/id%21'),
    ('id%', None, 'p1/p2/id%25'),
    ('id/sub', None, 'p1/p2/id/sub'),
    ('id', 'fld1.fld2', 'p1/p2/id/fld1.fld2'),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_path_is_suffixed(registry, resource, decorator, id, field, exp_path):

    @decorator(*resource, id=id, field=field, registry=registry)
    def fn(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={'service': {'path': 'p1/p2'}})

    assert len(webhooks) == 1
    assert webhooks[0]['clientConfig']['service']['path'] == exp_path


@pytest.mark.parametrize('opts, key, val', [
    (dict(side_effects=False), 'sideEffects', 'None'),
    (dict(side_effects=True), 'sideEffects', 'NoneOnDryRun'),
    (dict(ignore_failures=False), 'failurePolicy', 'Fail'),
    (dict(ignore_failures=True), 'failurePolicy', 'Ignore'),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_flat_options_are_mapped(registry, resource, decorator, opts, key, val):

    @decorator(*resource, registry=registry, **opts)
    def fn(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert webhooks[0][key] == val
    assert webhooks[0]['matchPolicy'] == 'Equivalent'
    assert webhooks[0]['timeoutSeconds'] == 30
    assert webhooks[0]['admissionReviewVersions'] == ['v1', 'v1beta1']


@pytest.mark.parametrize('opts, key, val', [
    (dict(), 'operations', ['*']),
    (dict(operation='CREATE'), 'operations', ['CREATE']),
    (dict(operation='UPDATE'), 'operations', ['UPDATE']),
    (dict(operation='DELETE'), 'operations', ['DELETE']),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_rule_options_are_mapped(registry, resource, decorator, opts, key, val):

    @decorator(*resource, registry=registry, **opts)
    def fn(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert len(webhooks[0]['rules']) == 1
    assert webhooks[0]['rules'][0][key] == val
    assert webhooks[0]['rules'][0]['scope'] == '*'
    assert webhooks[0]['rules'][0]['apiGroups'] == [resource.group]
    assert webhooks[0]['rules'][0]['apiVersions'] == [resource.version]
    assert webhooks[0]['rules'][0]['resources'] == [resource.plural]


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_multiple_handlers(registry, resource, decorator):

    @decorator(*resource, registry=registry)
    def fn1(**_):
        pass

    @decorator(*resource, registry=registry)
    def fn2(**_):
        pass

    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 2
    assert len(webhooks[0]['rules']) == 1
    assert len(webhooks[1]['rules']) == 1


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_irrelevant_resources_are_ignored(registry, resource, decorator):

    @decorator(*resource, registry=registry)
    def fn(**_):
        pass

    irrelevant_resource = Resource('grp', 'vers', 'plural')
    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[irrelevant_resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert len(webhooks[0]['rules']) == 0


@pytest.mark.parametrize('label_value, exp_expr', [
    (kopf.PRESENT, {'key': 'lbl', 'operator': 'Exists'}),
    (kopf.ABSENT, {'key': 'lbl', 'operator': 'DoesNotExist'}),
    ('val', {'key': 'lbl', 'operator': 'In', 'values': ['val']}),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_labels_specific_filter(registry, resource, decorator, label_value, exp_expr):

    @decorator(*resource, registry=registry, labels={'lbl': label_value})
    def fn(**_):
        pass

    irrelevant_resource = Resource('grp', 'vers', 'plural')
    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[irrelevant_resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert webhooks[0]['objectSelector'] == {'matchExpressions': [exp_expr]}


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
def test_labels_callable_filter(registry, resource, decorator):

    @decorator(*resource, registry=registry, labels={'lbl': lambda *_, **__: None})
    def fn(**_):
        pass

    irrelevant_resource = Resource('grp', 'vers', 'plural')
    webhooks = build_webhooks(
        registry._resource_webhooks.get_all_handlers(),
        resources=[irrelevant_resource],
        name_suffix='sfx',
        client_config={})

    assert len(webhooks) == 1
    assert webhooks[0]['objectSelector'] is None
