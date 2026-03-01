import pytest

from kopf._cogs.structs.bodies import Body
from kopf._cogs.structs.patches import Patch
from kopf._core.actions.application import patch_and_check
from kopf._core.actions.loggers import LocalObjectLogger

# Assume that the underlying patch_obj() is already tested with/without status as a sub-resource.
# Assume that the underlying diff() is already tested with left/right/full scopes and all values.
# Test ONLY the logging/warning on patch-vs-response inconsistencies here.


@pytest.mark.parametrize('patch, response', [

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 id='response-exact'),

    pytest.param({'spec': {'x': 'y'}, 'status': {'s': 't'}},
                 {'spec': {'x': 'y'}, 'status': {'s': 't'}, 'extra': '...'},
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

    # False-positive inconsistencies for K8s-managed fields.
    pytest.param({'metadata': {'annotations': {}}}, {'metadata': {}}, id='false-annotations'),
    pytest.param({'metadata': {'finalizers': []}}, {'metadata': {}}, id='false-finalizers'),
    pytest.param({'metadata': {'labels': {}}}, {'metadata': {}}, id='false-labels'),

])
async def test_patching_without_inconsistencies(
        kmock, resource, namespace, settings, assert_logs, patch, response):
    kmock.objects[resource, namespace, 'name1'] = {}  # suppress 404s
    kmock['patch', resource, kmock.namespace(namespace), kmock.name('name1')] << response

    body = Body({'metadata': {'namespace': namespace, 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    await patch_and_check(
        settings=settings,
        resource=resource,
        body=body,
        patch=Patch(patch),
        logger=logger,
    )

    assert_logs([
        "Merge-patching",
    ], prohibited=[
        "inconsistencies",
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

    # True-positive inconsistencies for K8s-managed fields with possible false-positives.
    pytest.param({'metadata': {'annotations': {'x': 'y'}}}, {'metadata': {}}, id='true-annotations'),
    pytest.param({'metadata': {'finalizers': ['x', 'y']}}, {'metadata': {}}, id='true-finalizers'),
    pytest.param({'metadata': {'labels': {'x': 'y'}}}, {'metadata': {}}, id='true-labels'),

])
async def test_patching_with_inconsistencies(
        kmock, resource, namespace, settings, assert_logs, patch, response):
    kmock.objects[resource, namespace, 'name1'] = {}  # suppress 404s
    kmock['patch', resource, kmock.namespace(namespace), kmock.name('name1')] << response

    body = Body({'metadata': {'namespace': namespace, 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    await patch_and_check(
        settings=settings,
        resource=resource,
        body=body,
        patch=Patch(patch),
        logger=logger,
    )

    assert_logs([
        "Merge-patching",
        "Merge-patching finished with inconsistencies:",
    ])


@pytest.mark.parametrize('expect_mangled, response_metadata', [
    pytest.param(False, {},
                 id='alive-without-finalizers'),
    pytest.param(False, {'finalizers': ['x']},
                 id='alive-with-finalizers'),
    pytest.param(False, {'deletionTimestamp': '2024-01-01T00:00:00Z', 'finalizers': ['x']},
                 id='deleting-with-finalizers'),
    pytest.param(True, {'deletionTimestamp': '2024-01-01T00:00:00Z'},
                 id='deleting-without-finalizers'),
    pytest.param(True, {'deletionTimestamp': '2024-01-01T00:00:00Z', 'finalizers': []},
                 id='deleting-with-empty-finalizers'),
])
async def test_resource_version_on_deletion_workaround(
        kmock, resource, namespace, settings, response_metadata, expect_mangled):
    kmock.objects[resource, namespace, 'name1'] = {}  # suppress 404s
    kmock['patch', resource, kmock.namespace(namespace), kmock.name('name1')] << {
        'metadata': {'resourceVersion': '123', **response_metadata},
        'spec': {'x': 'y'},
    }

    body = Body({'metadata': {'namespace': namespace, 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    rv, _ = await patch_and_check(
        settings=settings,
        resource=resource,
        body=body,
        patch=Patch({'spec': {'x': 'y'}}),
        logger=logger,
    )

    if expect_mangled:
        assert rv is not None
        assert rv != '123'
        assert '~which~never~arrives' in rv
    else:
        assert rv == '123'


async def test_patching_with_disappearance(
        kmock, resource, namespace, settings, assert_logs):
    kmock['patch', resource, kmock.namespace(namespace), kmock.name('name1')] << 404

    patch = {'spec': {'x': 'y'}, 'status': {'s': 't'}}  # irrelevant
    body = Body({'metadata': {'namespace': namespace, 'name': 'name1'}})
    logger = LocalObjectLogger(body=body, settings=settings)
    await patch_and_check(
        settings=settings,
        resource=resource,
        body=body,
        patch=Patch(patch),
        logger=logger,
    )

    assert_logs([
        "Merge-patching",
        "Patching was skipped: the object does not exist anymore",
    ], prohibited=[
        "inconsistencies"
    ])
