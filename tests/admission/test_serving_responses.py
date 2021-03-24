import base64
import json

import pytest

import kopf
from kopf.reactor.admission import AdmissionError, serve_admission_request
from kopf.reactor.handling import PermanentError, TemporaryError


async def test_metadata_reflects_the_request(
        settings, registry, memories, insights, indices, adm_request):

    adm_request['apiVersion'] = 'any.group/any.version'
    adm_request['kind'] = 'AnyKindOfAdmissionReview'
    adm_request['request']['uid'] = 'anyuid'
    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['apiVersion'] == 'any.group/any.version'
    assert response['kind'] == 'AnyKindOfAdmissionReview'
    assert response['response']['uid'] == 'anyuid'


async def test_simple_response_with_no_handlers_allows_admission(
        settings, registry, memories, insights, indices, adm_request):

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'warnings' not in response['response']
    assert 'patchType' not in response['response']
    assert 'patch' not in response['response']
    assert 'status' not in response['response']
    assert response['response']['allowed'] is True


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
async def test_simple_handler_allows_admission(
        settings, registry, resource, memories, insights, indices, adm_request,
        decorator):

    @decorator(*resource)
    def fn(**_):
        pass

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'warnings' not in response['response']
    assert 'patchType' not in response['response']
    assert 'patch' not in response['response']
    assert 'status' not in response['response']
    assert response['response']['allowed'] is True


@pytest.mark.parametrize('error, exp_msg, exp_code', [
    (Exception("No!"), "No!", 500),
    (kopf.PermanentError("No!"), "No!", 500),
    (kopf.TemporaryError("No!"), "No!", 500),
    (kopf.AdmissionError("No!"), "No!", 500),
    (kopf.AdmissionError("No!", code=123), "No!", 123),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
async def test_errors_deny_admission(
        settings, registry, resource, memories, insights, indices, adm_request,
        decorator, error, exp_msg, exp_code):

    @decorator(*resource)
    def fn(**_):
        raise error

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'warnings' not in response['response']
    assert 'patchType' not in response['response']
    assert 'patch' not in response['response']
    assert response['response']['allowed'] is False
    assert response['response']['status'] == {'message': exp_msg, 'code': exp_code}


@pytest.mark.parametrize('error1, error2, exp_msg', [
    pytest.param(Exception("err1"), Exception("err2"), "err1", id='builtin-first-samerank'),
    pytest.param(TemporaryError("err1"), TemporaryError("err2"), "err1", id='temp-first-samerank'),
    pytest.param(PermanentError("err1"), PermanentError("err2"), "err1", id='perm-first-samerank'),
    pytest.param(AdmissionError("err1"), AdmissionError("err2"), "err1", id='adms-first-samerank'),
    pytest.param(Exception("err1"), TemporaryError("err2"), "err2", id='temp-over-builtin'),
    pytest.param(Exception("err1"), AdmissionError("err2"), "err2", id='adms-over-builtin'),
    pytest.param(Exception("err1"), PermanentError("err2"), "err2", id='perm-over-builtin'),
    pytest.param(TemporaryError("err1"), PermanentError("err2"), "err2", id='perm-over-temp'),
    pytest.param(TemporaryError("err1"), AdmissionError("err2"), "err2", id='adms-over-temp'),
    pytest.param(PermanentError("err1"), AdmissionError("err2"), "err2", id='adms-over-perm'),
])
@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
async def test_errors_priorities(
        settings, registry, resource, memories, insights, indices, adm_request,
        decorator, error1, error2, exp_msg):

    @decorator(*resource)
    def fn1(**_):
        raise error1

    @decorator(*resource)
    def fn2(**_):
        raise error2

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'warnings' not in response['response']
    assert 'patchType' not in response['response']
    assert 'patch' not in response['response']
    assert response['response']['allowed'] is False
    assert response['response']['status'] == {'message': exp_msg, 'code': 500}


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
async def test_warnings_are_returned_to_kubernetes(
        settings, registry, resource, memories, insights, indices, adm_request,
        decorator):

    @decorator(*resource)
    def fn(warnings, **_):
        warnings.append("oops!")

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'patchType' not in response['response']
    assert 'patch' not in response['response']
    assert 'status' not in response['response']
    assert response['response']['warnings'] == ['oops!']
    assert response['response']['allowed'] is True


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
async def test_patch_is_returned_to_kubernetes(
        settings, registry, resource, memories, insights, indices, adm_request,
        decorator):

    @decorator(*resource)
    def fn(patch, **_):
        patch['xyz'] = 123

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert 'warnings' not in response['response']
    assert 'status' not in response['response']
    assert response['response']['allowed'] is True
    assert response['response']['patchType'] == 'JSONPatch'
    assert json.loads(base64.b64decode(response['response']['patch'])) == [
        {'op': 'replace', 'path': '/xyz', 'value': 123},
    ]
