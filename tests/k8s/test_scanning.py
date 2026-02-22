import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.scanning import scan_resources


async def test_no_resources_in_empty_apis(kmock, settings, logger):
    core = kmock['get /api'] << {'versions': []}
    apis = kmock['get /apis'] << {'groups': []}

    resources = await scan_resources(settings=settings, logger=logger)
    assert len(resources) == 0

    assert len(core) == 1
    assert len(apis) == 1


@pytest.mark.parametrize('namespaced', [True, False])
async def test_resources_in_old_apis(kmock, settings, logger, namespaced):
    core = kmock['get /api'] << {'versions': ['v1']}
    apis = kmock['get /apis'] << {'groups': []}
    scan = kmock['get /api/v1'] << {'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': namespaced,
            'categories': ['category1', 'category2'],
            'shortNames': ['shortname1', 'shortname2'],
            'verbs': ['verb1', 'verb2'],
        },
    ]}

    resources = await scan_resources(settings=settings, logger=logger)
    assert len(resources) == 1

    resource1 = list(resources)[0]
    assert resource1.group == ''
    assert resource1.version == 'v1'
    assert resource1.kind == 'kind1'
    assert resource1.plural == 'plural1'
    assert resource1.singular == 'singular1'
    assert resource1.preferred == True
    assert resource1.namespaced == namespaced
    assert resource1.subresources == set()
    assert resource1.categories == {'category1', 'category2'}
    assert resource1.shortcuts == {'shortname1', 'shortname2'}
    assert resource1.verbs == {'verb1', 'verb2'}

    assert len(core) == 1
    assert len(apis) == 1
    assert len(scan) == 1


@pytest.mark.parametrize('namespaced', [True, False])
@pytest.mark.parametrize('preferred_version, expected_preferred', [
    ('version1', True),
    ('versionX', False),
])
async def test_resources_in_new_apis(
        kmock, settings, logger, namespaced,
        preferred_version, expected_preferred):
    core = kmock['get /api'] << {'versions': []}
    apis = kmock['get /apis'] << {'groups': [
        {
            'name': 'group1',
            'preferredVersion': {'version': preferred_version},
            'versions': [{'version': 'version1'}],
        },
    ]}
    g1v1 = kmock['get /apis/group1/version1'] << {'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': namespaced,
            'categories': ['category1', 'category2'],
            'shortNames': ['shortname1', 'shortname2'],
            'verbs': ['verb1', 'verb2'],
        },
    ]}

    resources = await scan_resources(settings=settings, logger=logger)
    assert len(resources) == 1

    resource1 = list(resources)[0]
    assert resource1.group == 'group1'
    assert resource1.version == 'version1'
    assert resource1.kind == 'kind1'
    assert resource1.plural == 'plural1'
    assert resource1.singular == 'singular1'
    assert resource1.preferred == expected_preferred
    assert resource1.namespaced == namespaced
    assert resource1.subresources == set()
    assert resource1.categories == {'category1', 'category2'}
    assert resource1.shortcuts == {'shortname1', 'shortname2'}
    assert resource1.verbs == {'verb1', 'verb2'}

    assert len(core) == 1
    assert len(apis) == 1
    assert len(g1v1) == 1


async def test_subresources_in_old_apis(kmock, settings, logger):
    kmock['get /api'] << {'versions': ['v1']}
    kmock['get /apis'] << {'groups': []}
    kmock['get /api/v1'] << {'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': True,
            'categories': [],
            'shortNames': [],
            'verbs': [],
        },
        {
            'name': 'plural1/sub1',
        },
        {
            'name': 'plural1/sub2',
        },
        {
            'name': 'pluralX/sub3',
        },
    ]}

    resources = await scan_resources(settings=settings, logger=logger)
    assert len(resources) == 1
    resource1 = list(resources)[0]
    assert resource1.subresources == {'sub1', 'sub2'}


async def test_subresources_in_new_apis(kmock, settings, logger):
    kmock.resources['group1/v1/plural1'].kind = 'kind1'
    kmock.resources['group1/v1/plural1'].singular = 'singular1'
    kmock.resources['group1/v1/plural1'].namespaced = True
    kmock.resources['group1/v1/plural1'].subresources = {'sub1', 'sub2'}
    resources = await scan_resources(settings=settings, logger=logger)
    assert not kmock.errors
    assert len(resources) == 1
    resource1 = list(resources)[0]
    assert resource1.subresources == {'sub1', 'sub2'}


@pytest.mark.parametrize('group_filter, exp_core, exp_apis, exp_crv1, exp_g1v1, exp_g2v1', [
    pytest.param([''], 1, 0, 1, 0, 0, id='only-core'),
    pytest.param(['g1'], 0, 1, 0, 1, 0, id='only-g1'),
    pytest.param(['g2'], 0, 1, 0, 0, 1, id='only-g2'),
    pytest.param(['', 'g1'], 1, 1, 1, 1, 0, id='core-and-g1'),
    pytest.param(['', 'g2'], 1, 1, 1, 0, 1, id='core-and-g2'),
    pytest.param(['g1', 'g2'], 0, 1, 0, 1, 1, id='g1-and-g2'),
    pytest.param(['X'], 0, 1, 0, 0, 0, id='unexistent'),
    pytest.param([], 0, 0, 0, 0, 0, id='restrictive'),
    pytest.param(None, 1, 1, 1, 1, 1, id='unfiltered'),
])
async def test_group_filtering(
        kmock, settings, logger,
        group_filter, exp_core, exp_apis, exp_crv1, exp_g1v1, exp_g2v1):
    core = kmock['get /api'] << {'versions': ['v1']}
    apis = kmock['get /apis'] << {'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]},
        {'name': 'g2', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g2v1'}]},
    ]}
    crv1 = kmock['get /api/v1'] << {'resources': []}
    g1v1 = kmock['get /apis/g1/g1v1'] << {'resources': []}
    g2v1 = kmock['get /apis/g2/g2v1'] << {'resources': []}

    await scan_resources(groups=group_filter, settings=settings, logger=logger)

    assert len(core) == exp_core
    assert len(apis) == exp_apis
    assert len(crv1) == exp_crv1
    assert len(g1v1) == exp_g1v1
    assert len(g2v1) == exp_g2v1


@pytest.mark.parametrize('status', [404])
async def test_http404_returns_no_resources_from_old_apis(kmock, settings, logger, status):
    kmock['get /api'] << {'versions': ['v1']}
    kmock['get /apis'] << {'groups': []}
    crv1 = kmock['get /api/v1'] << status

    resources = await scan_resources(settings=settings, logger=logger)

    assert not resources
    assert len(crv1) == 1


@pytest.mark.parametrize('status', [404])
async def test_http404_returns_no_resources_from_new_apis(kmock, settings, logger, status):
    kmock['get /api'] << {'versions': []}
    kmock['get /apis'] << {'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]}]}
    g1v1 = kmock['get /apis/g1/g1v1'] << status

    resources = await scan_resources(settings=settings, logger=logger)

    assert not resources
    assert len(g1v1) == 1


@pytest.mark.parametrize('status', [403, 500, 666])
async def test_unknown_api_statuses_escalate_from_old_apis(kmock, settings, logger, status):
    kmock['get /api'] << {'versions': ['v1']}
    kmock['get /apis'] << {'groups': []}
    crv1 = kmock['get /api/v1'] << status

    with pytest.raises(APIError) as err:
        await scan_resources(settings=settings, logger=logger)

    assert err.value.status == status
    assert len(crv1) == 1


@pytest.mark.parametrize('status', [403, 500, 666])
async def test_unknown_api_statuses_escalate_from_new_apis(kmock, settings, logger, status):
    kmock['get /api'] << {'versions': []}
    kmock['get /apis'] << {'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]}]}
    g1v1 = kmock['get /apis/g1/g1v1'] << status

    with pytest.raises(APIError) as err:
        await scan_resources(settings=settings, logger=logger)

    assert err.value.status == status
    assert len(g1v1) == 1


async def test_empty_singulars_fall_back_to_kinds(kmock, settings, logger):
    kmock['get /api'] << {'versions': ['v1']}
    kmock['get /apis'] << {'groups': []}

    # Only one endpoint is enough, core v1 is easier to mock:
    kmock['get /api/v1'] << {'resources': [
        {
            'kind': 'MultiWordKind',
            'name': '...',
            'singularName': '',  # as in K3s
            'namespaced': True,
            'categories': [],
            'shortNames': [],
            'verbs': [],
        },
    ]}

    resources = await scan_resources(groups=[''], settings=settings, logger=logger)
    assert len(resources) == 1

    resource1 = list(resources)[0]
    assert resource1.singular == 'multiwordkind'


# TODO: LATER: test that the requests are done in parallel, and the total timing is the best possible.
