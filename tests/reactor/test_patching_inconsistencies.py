import logging

import aiohttp.web
import pytest

from kopf.engines.loggers import LocalObjectLogger
from kopf.reactor.effects import patch_and_check
from kopf.structs.bodies import Body
from kopf.structs.patches import Patch

# Assume that the underlying patch_obj() is already tested with/without status as a sub-resource.
# Assume that the underlying diff() is already tested with left/right/full scopes and all values.
# Test ONLY the logging/warning on patch-vs-response inconsistencies here.


@pytest.mark.parametrize('patch, response', [

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 id='response-exact'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 't'}, 'metadata': '...'},
                 id='response-root-extra'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y', 'extra': '...'}, 'status': {'s': 't'}},
                 id='response-spec-extra'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 't', 'extra': '...'}},
                 id='response-status-extra'),

    pytest.param({'spec': {'x': None}, 'status': {'s': None}},
                 {'spec': {}, 'status': {}},
                 id='response-clean'),
])
async def test_patching_without_inconsistencies(
        resource, settings, caplog, assert_logs, version_api,
        aresponses, hostname, resp_mocker,
        patch, response):
    caplog.set_level(logging.DEBUG)

    url = resource.get_url(namespace='ns1', name='name1')
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response(response))
    aresponses.add(hostname, url, 'patch', patch_mock)

    body = Body({'metadata': {'namespace': 'ns1', 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    await patch_and_check(
        resource=resource,
        body=body,
        patch=Patch(patch),
        logger=logger,
    )

    assert_logs([
        "Patching with:",
    ], prohibited=[
        "Patching failed with inconsistencies:",
    ])


@pytest.mark.parametrize('patch, response', [

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {},
                 id='response-empty'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}},
                 id='response-status-lost'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'status': {'s': 't'}},
                 id='response-spec-lost'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'not-y'}, 'status': {'s': 't'}},
                 id='response-spec-altered'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 'not-t'}},
                 id='response-status-altered'),

    pytest.param({'spec': {'x': None}, 'status': {'s': None}},
                 {'spec': {'x': 'y'}, 'status': {}},
                 id='response-spec-undeleted'),

    pytest.param({'spec': {'x': None}, 'status': {'s': None}},
                 {'spec': {}, 'status': {'s': 't'}},
                 id='response-status-undeleted'),
])
async def test_patching_with_inconsistencies(
        resource, settings, caplog, assert_logs, version_api,
        aresponses, hostname, resp_mocker,
        patch, response):
    caplog.set_level(logging.DEBUG)

    url = resource.get_url(namespace='ns1', name='name1')
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response(response))
    aresponses.add(hostname, url, 'patch', patch_mock)

    body = Body({'metadata': {'namespace': 'ns1', 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    await patch_and_check(
        resource=resource,
        body=body,
        patch=Patch(patch),
        logger=logger,
    )

    assert_logs([
        "Patching with:",
        "Patching failed with inconsistencies:",
    ])
