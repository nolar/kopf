"""
We test these aspects:

- If we skip the irrelevant patches: empty or not applicable (for status).
- If the status patches are executed separately if it is a subresource.
- If the patches are formed properly (ops-wise, content-changing).
- If the patches contain the "test" op with the proper `resourceVersion`.
  - Of the latest preceding patch, if any.
  - Of the original body if no patches precede it.
- If we react properly to the HTTP 422 failures of the "test" op.

We have 0-4 patches:

- merge-patch on the object
- merge-patch on the status
- json-patch on the object
- json-patch on the status

Each can be present or absent, depending on the setup:

- is status a subresource: yes or no?
- are fns present and change anything: yes or no?

Instead of doing the full matrix, we focus only on the latest patch
and the patch chronologically preceding it (for its resourceVersion),
skipping the cases when 2+ patches precede it (affects nothing),
and the cases when other patches follow it (tested in other functions).

Those are "happy paths".

Additionally, the JSON-patches can catch HTTP 422 and return
different results (the remaining patches).

Additionally, every of these patches can catch HTTP API errors
(of which HTTP 404 is handled gracefully, while others escalate).
"""
import dataclasses

import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.patching import patch_obj
from kopf._cogs.structs.patches import Patch
from kopf._cogs.structs.references import Resource

# The key difference is the `resourceVersion` — we check that the proper one is used.
# The empty status is present to ensure predictable json-patch diffs with one field only.
OBJECT_MERGE_RESPONSE = {'metadata': {'resourceVersion': 'rv-object-merge'}, 'status': {}}
STATUS_MERGE_RESPONSE = {'metadata': {'resourceVersion': 'rv-status-merge'}, 'status': {}}
OBJECT_JSONP_RESPONSE = {'metadata': {'resourceVersion': 'rv-object-jsonp'}, 'status': {}}
STATUS_JSONP_RESPONSE = {'metadata': {'resourceVersion': 'rv-status-jsonp'}, 'status': {}}


@pytest.fixture()
def resource():
    # We do not care about namespaced/cluster-wide here, only on the subresource presence.
    return Resource('kopf.dev', 'v1', 'kopfexamples', subresources=frozenset({'status'}))


@pytest.fixture(autouse=True)
def _endpoints(kmock, resource, namespace):

    def set_body(body):
        kmock.objects[resource, namespace, 'name1'] = dict(body)
        return body

    set_body({})  # suppress 404s in the emulator

    # Every time the request is made, also update the stored body to the request's response,
    # so that json-patches could be applied properly on the next requests, in particular:
    # - the validity of he "test" operations on "/metadata/resourceVersion";
    # - the existence of the metadata/spec/status stanzas for proper "add/replace" ops.
    common = kmock['patch', resource, kmock.namespace(namespace), kmock.name('name1')]
    common[{'Content-Type': 'application/merge-patch+json'}, kmock.subresource(None)] << (lambda: set_body(OBJECT_MERGE_RESPONSE))
    common[{'Content-Type': 'application/merge-patch+json'}, kmock.subresource('status')] << (lambda: set_body(STATUS_MERGE_RESPONSE))
    common[{'Content-Type': 'application/json-patch+json'}, kmock.subresource(None)] << (lambda: set_body(OBJECT_JSONP_RESPONSE))
    common[{'Content-Type': 'application/json-patch+json'}, kmock.subresource('status')] << (lambda: set_body(STATUS_JSONP_RESPONSE))


def _noop(body):
    pass


def _add_finalizer(body):
    body.setdefault('metadata', {}).setdefault('finalizers', []).append('fin')


def _add_status_field(body):
    body.setdefault('status', {})['count'] = 1


#
# Test sequencing of patches and proper use of `resourceVersion` on each step.
# Limit to max 2 patches per operation — there is no extra logic in 3+ patches.
#
async def test_empty_or_noop_patch_makes_no_api_calls(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    patch = Patch({}, body=original_body, fns=[_noop])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )

    assert result is None
    assert remaining is None
    assert len(kmock) == 0

    assert_logs([
    ], prohibited=[
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_object_merge_alone(caplog,
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}}, body=original_body, fns=[])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_MERGE_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert_logs([
        "Merge-patching the resource with",
    ], prohibited=[
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_status_merge_alone(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'status': {'x': 'y'}}, body=original_body, fns=[])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_MERGE_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource == 'status'
    assert kmock[0].data == {'status': {'x': 'y'}}
    assert_logs([
        "Merge-patching the status with",
    ], prohibited=[
        "Merge-patching the resource with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_status_merge_after_object_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body=original_body, fns=[])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_MERGE_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource is None
    assert kmock[1].subresource == 'status'
    assert kmock[0].data == {'spec': {'x': 'y'}}  # status absent
    assert kmock[1].data == {'status': {'x': 'y'}}  # spec absent
    assert_logs([
        "Merge-patching the resource with",
        "Merge-patching the status with",
    ], prohibited=[
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_object_jsonp_alone(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({}, body=original_body, fns=[_add_finalizer])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv0'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
    assert_logs([
        "JSON-patching the resource with",
    ], prohibited=[
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the status with",
    ])


async def test_object_jsonp_after_object_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}}, body=original_body, fns=[_add_finalizer])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource is None
    assert kmock[1].subresource is None
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert kmock[1].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-object-merge'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
    assert_logs([
        "Merge-patching the resource with",
        "JSON-patching the resource with",
    ], prohibited=[
        "Merge-patching the status with",
        "JSON-patching the status with",
    ])


async def test_object_jsonp_after_status_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'status': {'x': 'y'}}, body=original_body, fns=[_add_finalizer])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource == 'status'
    assert kmock[1].subresource is None
    assert kmock[0].data == {'status': {'x': 'y'}}
    assert kmock[1].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-status-merge'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
    assert_logs([
        "Merge-patching the status with",
        "JSON-patching the resource with",
    ], prohibited=[
        "Merge-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_status_jsonp_alone(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({}, body=original_body, fns=[_add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource == 'status'
    assert kmock[0].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv0'},
        {'op': 'add', 'path': '/status/count', 'value': 1},
    ]
    assert_logs([
        "JSON-patching the status with",
    ], prohibited=[
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the resource with",
    ])


async def test_status_jsonp_after_object_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}}, body=original_body, fns=[_add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource is None
    assert kmock[1].subresource == 'status'
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert kmock[1].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-object-merge'},
        {'op': 'add', 'path': '/status/count', 'value': 1},
    ]
    assert_logs([
        "Merge-patching the resource with",
        "JSON-patching the status with",
    ], prohibited=[
        "Merge-patching the status with",
        "JSON-patching the resource with",
    ])


async def test_status_jsonp_after_status_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'status': {'x': 'y'}}, body=original_body, fns=[_add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource == 'status'
    assert kmock[1].subresource == 'status'
    assert kmock[0].data == {'status': {'x': 'y'}}
    assert kmock[1].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-status-merge'},
        {'op': 'add', 'path': '/status/count', 'value': 1},
    ]
    assert_logs([
        "Merge-patching the status with",
        "JSON-patching the status with",
    ], prohibited=[
        "Merge-patching the resource with",
        "JSON-patching the resource with",
    ])


async def test_status_jsonp_after_object_jsonp(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch(body=original_body, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 2
    assert kmock[0].subresource is None
    assert kmock[1].subresource == 'status'
    assert kmock[0].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv0'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
    assert kmock[1].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-object-jsonp'},
        {'op': 'add', 'path': '/status/count', 'value': 1},
    ]
    assert_logs([
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ], prohibited=[
        "Merge-patching the resource with",
        "Merge-patching the status with",
    ])


# Not of direct interest (already tested implicitly), but worth checking the worst case: 4x patches.
async def test_all_four_patches(
        kmock, settings, resource, namespace, logger, assert_logs):
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body=original_body, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 4
    assert kmock[0].subresource is None  # merge-patch
    assert kmock[1].subresource == 'status'  # merge-patch
    assert kmock[2].subresource is None  # json-patch
    assert kmock[3].subresource == 'status'  # json-patch
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert kmock[1].data == {'status': {'x': 'y'}}
    assert kmock[2].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-status-merge'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
    assert kmock[3].data == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv-object-jsonp'},
        {'op': 'add', 'path': '/status/count', 'value': 1},
    ]
    assert_logs([
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


#
# Test status as a direct field (not a subresource): must not make extra API patches.
#
async def test_no_subresource_skips_status_merge(
        kmock, settings, resource, namespace, logger, assert_logs):
    resource = dataclasses.replace(resource, subresources=frozenset())
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body=original_body)
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_MERGE_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data == {'spec': {'x': 'y'}, 'status': {'x': 'y'}}  # mixed!
    assert_logs([
        "Merge-patching the resource with",
    ], prohibited=[
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
    ])


async def test_no_subresource_skips_status_jsonp(
        kmock, settings, resource, namespace, logger, assert_logs):
    resource = dataclasses.replace(resource, subresources=frozenset())
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    kmock.objects[resource, namespace, 'name1'] = original_body
    patch = Patch(body=original_body, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_JSONP_RESPONSE
    assert remaining is None
    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data[:1] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv0'},
    ]
    # The order of ops does not matter. What matters is the mix of status and metadata.
    paths = {op['path'] for op in kmock[0].data[1:]}
    assert paths == {'/metadata/finalizers', '/status/count'}  # mixed!
    assert_logs([
        "JSON-patching the resource with",
    ], prohibited=[
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the status with",
    ])


#
# Test the remaining patches on HTTP 422 from JSON-patches.
#
async def test_422_in_object_jsonp_returns_the_remaining_patch(
        kmock, settings, resource, namespace, logger, assert_logs):
    kmock[kmock.subresource(None), {'Content-Type': 'application/json-patch+json'}] ** 1 << 422
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body=original_body, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == STATUS_MERGE_RESPONSE  # we only need to see that it is not None
    assert remaining is not None
    assert remaining._original is None  # do not carry the body through cycles
    assert list(remaining.fns) == [_add_finalizer, _add_status_field]
    assert dict(remaining) == {}
    assert len(kmock) == 3
    assert kmock[0].subresource is None  # merge-patch
    assert kmock[1].subresource == 'status'  # merge-patch
    assert kmock[2].subresource is None  # json-patch
    assert_logs([
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "Could not apply the patch in full",
    ], prohibited=[
        "JSON-patching the status with",
    ])


async def test_422_in_status_jsonp_returns_the_remaining_patch(
        kmock, settings, resource, namespace, logger, assert_logs):
    kmock[kmock.subresource('status'), {'Content-Type': 'application/json-patch+json'}] ** 1 << 422
    original_body = {'metadata': {'resourceVersion': 'rv0'}, 'status': {}}
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body=original_body, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result == OBJECT_JSONP_RESPONSE  # we only need to see that it is not None
    assert remaining is not None
    assert remaining._original is None  # do not carry the body through cycles
    assert list(remaining.fns) == [_add_finalizer, _add_status_field]
    assert dict(remaining) == {}
    assert len(kmock) == 4
    assert kmock[0].subresource is None  # merge-patch
    assert kmock[1].subresource == 'status'  # merge-patch
    assert kmock[2].subresource is None  # json-patch
    assert kmock[3].subresource == 'status'  # json-patch
    assert_logs([
        "Merge-patching the resource with",
        "Merge-patching the status with",
        "JSON-patching the resource with",
        "JSON-patching the status with",
        "Could not apply the patch in full",
    ])


#
# Test API errors in every individual patch.
#
@pytest.mark.parametrize('exp_api_count, content_type, subresource', [
    pytest.param(1, 'application/merge-patch+json', None),
    pytest.param(2, 'application/merge-patch+json', 'status'),
    pytest.param(3, 'application/json-patch+json', None),
    pytest.param(4, 'application/json-patch+json', 'status'),
])
async def test_404_ignores_absent_objects(
        kmock, settings, resource, namespace, logger, content_type, subresource,
        cluster_resource, namespaced_resource, assert_logs, exp_api_count):
    kmock[kmock.subresource(subresource), {'Content-Type': content_type}] ** 1 << 404
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body={}, fns=[_add_finalizer, _add_status_field])
    result, remaining = await patch_obj(
        logger=logger, settings=settings, resource=resource,
        namespace=namespace, name='name1', patch=patch,
    )
    assert result is None
    assert remaining is None
    assert len(kmock) == exp_api_count
    assert_logs([
        "Patching was skipped: the object does not exist anymore",
    ])


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('exp_api_count, content_type, subresource', [
    pytest.param(1, 'application/merge-patch+json', None),
    pytest.param(2, 'application/merge-patch+json', 'status'),
    pytest.param(3, 'application/json-patch+json', None),
    pytest.param(4, 'application/json-patch+json', 'status'),
])
@pytest.mark.parametrize('status', [400, 403, 500, 666])
async def test_api_errors_raised(
        kmock, settings, status, resource, namespace, logger, content_type, subresource,
        cluster_resource, namespaced_resource, exp_api_count):
    kmock[kmock.subresource(subresource), {'Content-Type': content_type}] ** 1 << status
    patch = Patch({'spec': {'x': 'y'}, 'status': {'x': 'y'}}, body={}, fns=[_add_finalizer, _add_status_field])
    with pytest.raises(APIError) as e:
        await patch_obj(
            logger=logger, settings=settings, resource=resource,
            namespace=namespace, name='name1', patch=patch,
        )
    assert len(kmock) == exp_api_count
    assert e.value.status == status
