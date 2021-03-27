from unittest.mock import Mock

import pytest

import kopf
from kopf.reactor.admission import serve_admission_request


@pytest.mark.parametrize('dryrun', [True, False])
async def test_dryrun_passed(
        settings, registry, resource, memories, insights, indices, adm_request, dryrun):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    adm_request['request']['dryRun'] = dryrun
    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['dryrun'] == dryrun


async def test_headers_passed(
        settings, registry, resource, memories, insights, indices, adm_request):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    headers = {'X': '123', 'Y': '456'}
    await serve_admission_request(
        adm_request, headers=headers,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['headers'] == headers


async def test_headers_not_passed_but_injected(
        settings, registry, resource, memories, insights, indices, adm_request):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['headers'] == {}


async def test_sslpeer_passed(
        settings, registry, resource, memories, insights, indices, adm_request):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    sslpeer = {'X': '123', 'Y': '456'}
    await serve_admission_request(
        adm_request, sslpeer=sslpeer,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['sslpeer'] == sslpeer


async def test_sslpeer_not_passed_but_injected(
        settings, registry, resource, memories, insights, indices, adm_request):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['sslpeer'] == {}


async def test_userinfo_passed(
        settings, registry, resource, memories, insights, indices, adm_request):
    mock = Mock()

    @kopf.on.validate(*resource)
    def fn(**kwargs):
        mock(**kwargs)

    userinfo = {'X': '123', 'Y': '456'}
    adm_request['request']['userInfo'] = userinfo
    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert mock.call_count == 1
    assert mock.call_args[1]['userinfo'] == userinfo
