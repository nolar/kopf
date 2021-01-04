import aiohttp.web
import pytest

from kopf.clients.errors import APIError
from kopf.clients.scanning import scan_resources


async def test_no_resources_in_empty_apis(
        resp_mocker, aresponses, hostname):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': []}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))

    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)

    resources = await scan_resources()
    assert len(resources) == 0

    assert core_mock.call_count == 1
    assert apis_mock.call_count == 1


@pytest.mark.parametrize('namespaced', [True, False])
async def test_resources_in_old_apis(
        resp_mocker, aresponses, hostname, namespaced):


    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))
    scan_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': namespaced,
            'categories': ['category1', 'category2'],
            'shortNames': ['shortname1', 'shortname2'],
            'verbs': ['verb1', 'verb2'],
        },
    ]}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/api/v1', 'get', scan_mock)

    resources = await scan_resources()
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

    assert core_mock.call_count == 1
    assert apis_mock.call_count == 1
    assert scan_mock.call_count == 1


@pytest.mark.parametrize('namespaced', [True, False])
@pytest.mark.parametrize('preferred_version, expected_preferred', [
    ('version1', True),
    ('versionX', False),
])
async def test_resources_in_new_apis(
        resp_mocker, aresponses, hostname, namespaced,
        preferred_version, expected_preferred):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': []}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {
            'name': 'group1',
            'preferredVersion': {'version': preferred_version},
            'versions': [{'version': 'version1'}],
        },
    ]}))
    g1v1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': namespaced,
            'categories': ['category1', 'category2'],
            'shortNames': ['shortname1', 'shortname2'],
            'verbs': ['verb1', 'verb2'],
        },
    ]}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/apis/group1/version1', 'get', g1v1_mock)

    resources = await scan_resources()
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

    assert core_mock.call_count == 1
    assert apis_mock.call_count == 1
    assert g1v1_mock.call_count == 1


async def test_subresources_in_old_apis(
        resp_mocker, aresponses, hostname):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))
    v1v1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
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
    ]}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/api/v1', 'get', v1v1_mock)

    resources = await scan_resources()
    assert len(resources) == 1
    resource1 = list(resources)[0]
    assert resource1.subresources == {'sub1', 'sub2'}


async def test_subresources_in_new_apis(
        resp_mocker, aresponses, hostname):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': []}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {
            'name': 'group1',
            'preferredVersion': {'version': 'version1'},
            'versions': [{'version': 'version1'}],
        },
    ]}))
    g1v1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
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
    ]}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/apis/group1/version1', 'get', g1v1_mock)

    resources = await scan_resources()
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
        resp_mocker, aresponses, hostname,
        group_filter, exp_core, exp_apis, exp_crv1, exp_g1v1, exp_g2v1):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]},
        {'name': 'g2', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g2v1'}]},
    ]}))
    crv1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': []}))
    g1v1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': []}))
    g2v1_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': []}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/api/v1', 'get', crv1_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/apis/g1/g1v1', 'get', g1v1_mock)
    aresponses.add(hostname, '/apis/g2/g2v1', 'get', g2v1_mock)

    await scan_resources(groups=group_filter)

    assert core_mock.call_count == exp_core
    assert apis_mock.call_count == exp_apis

    assert crv1_mock.call_count == exp_crv1
    assert g1v1_mock.call_count == exp_g1v1
    assert g2v1_mock.call_count == exp_g2v1



@pytest.mark.parametrize('status', [404])
async def test_http404_returns_no_resources_from_old_apis(
        resp_mocker, aresponses, hostname, status):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))
    status_mock = resp_mocker(return_value=aresponses.Response(status=status))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/api/v1', 'get', status_mock)

    resources = await scan_resources()

    assert not resources
    assert status_mock.call_count == 1


@pytest.mark.parametrize('status', [404])
async def test_http404_returns_no_resources_from_new_apis(
        resp_mocker, aresponses, hostname, status):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': []}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]},
    ]}))
    status_mock = resp_mocker(return_value=aresponses.Response(status=status))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/apis/g1/g1v1', 'get', status_mock)

    resources = await scan_resources()

    assert not resources
    assert status_mock.call_count == 1


@pytest.mark.parametrize('status', [403, 500, 666])
async def test_unknown_api_statuses_escalate_from_old_apis(
        resp_mocker, aresponses, hostname, status):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))
    status_mock = resp_mocker(return_value=aresponses.Response(status=status))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/api/v1', 'get', status_mock)

    with pytest.raises(APIError) as err:
        await scan_resources()

    assert err.value.status == status
    assert status_mock.call_count == 1


@pytest.mark.parametrize('status', [403, 500, 666])
async def test_unknown_api_statuses_escalate_from_new_apis(
        resp_mocker, aresponses, hostname, status):

    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': []}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {'name': 'g1', 'preferredVersion': {'version': ''}, 'versions': [{'version': 'g1v1'}]},
    ]}))
    status_mock = resp_mocker(return_value=aresponses.Response(status=status))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/apis/g1/g1v1', 'get', status_mock)

    with pytest.raises(APIError) as err:
        await scan_resources()

    assert err.value.status == status
    assert status_mock.call_count == 1


async def test_empty_singulars_fall_back_to_kinds(
        resp_mocker, aresponses, hostname):

    # Only one endpoint is enough, core v1 is easier to mock:
    core_mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    apis_mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': []}))
    scan_mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
        {
            'kind': 'MultiWordKind',
            'name': '...',
            'singularName': '',  # as in K3s
            'namespaced': True,
            'categories': [],
            'shortNames': [],
            'verbs': [],
        },
    ]}))
    aresponses.add(hostname, '/api', 'get', core_mock)
    aresponses.add(hostname, '/apis', 'get', apis_mock)
    aresponses.add(hostname, '/api/v1', 'get', scan_mock)

    resources = await scan_resources(groups=[''])
    assert len(resources) == 1

    resource1 = list(resources)[0]
    assert resource1.singular == 'multiwordkind'


# TODO: LATER: test that the requests are done in parallel, and the total timing is the best possible.
