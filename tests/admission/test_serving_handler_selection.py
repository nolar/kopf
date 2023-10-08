from unittest.mock import Mock

import pytest

import kopf
from kopf._cogs.structs.ids import HandlerId
from kopf._core.engines.admission import serve_admission_request
from kopf._core.intents.causes import WebhookType


async def test_all_handlers_with_no_id_or_reason_requested(
        settings, registry, resource, memories, insights, indices, adm_request):

    mock1 = Mock()
    mock2 = Mock()
    mock3 = Mock()
    mock4 = Mock()

    @kopf.on.validate(*resource)
    def fn1(**kwargs):
        mock1(**kwargs)

    @kopf.on.validate(*resource)
    def fn2(**kwargs):
        mock2(**kwargs)

    @kopf.on.mutate(*resource)
    def fn3(**kwargs):
        mock3(**kwargs)

    @kopf.on.mutate(*resource)
    def fn4(**kwargs):
        mock4(**kwargs)

    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert mock1.call_count == 1
    assert mock2.call_count == 1
    assert mock3.call_count == 1
    assert mock4.call_count == 1


@pytest.mark.parametrize('reason', set(WebhookType))
async def test_handlers_with_reason_requested(
        settings, registry, resource, memories, insights, indices, adm_request, reason):

    mock1 = Mock()
    mock2 = Mock()
    mock3 = Mock()
    mock4 = Mock()

    @kopf.on.validate(*resource)
    def fn1(**kwargs):
        mock1(**kwargs)

    @kopf.on.validate(*resource)
    def fn2(**kwargs):
        mock2(**kwargs)

    @kopf.on.mutate(*resource)
    def fn3(**kwargs):
        mock3(**kwargs)

    @kopf.on.mutate(*resource)
    def fn4(**kwargs):
        mock4(**kwargs)

    response = await serve_admission_request(
        adm_request, reason=reason,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert mock1.call_count == (1 if reason == WebhookType.VALIDATING else 0)
    assert mock2.call_count == (1 if reason == WebhookType.VALIDATING else 0)
    assert mock3.call_count == (1 if reason == WebhookType.MUTATING else 0)
    assert mock4.call_count == (1 if reason == WebhookType.MUTATING else 0)


async def test_handlers_with_webhook_requested(
        settings, registry, resource, memories, insights, indices, adm_request):

    mock1 = Mock()
    mock2 = Mock()
    mock3 = Mock()
    mock4 = Mock()

    @kopf.on.validate(*resource, id='fnX')
    def fn1(**kwargs):
        mock1(**kwargs)

    @kopf.on.validate(*resource)
    def fn2(**kwargs):
        mock2(**kwargs)

    @kopf.on.mutate(*resource)
    def fn3(**kwargs):
        mock3(**kwargs)

    @kopf.on.mutate(*resource, id='fnX')
    def fn4(**kwargs):
        mock4(**kwargs)

    response = await serve_admission_request(
        adm_request, webhook=HandlerId('fnX'),
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert mock1.call_count == 1
    assert mock2.call_count == 0
    assert mock3.call_count == 0
    assert mock4.call_count == 1


@pytest.mark.parametrize('reason', set(WebhookType))
async def test_handlers_with_reason_and_webhook_requested(
        settings, registry, resource, memories, insights, indices, adm_request, reason):

    mock1 = Mock()
    mock2 = Mock()
    mock3 = Mock()
    mock4 = Mock()

    @kopf.on.validate(*resource, id='fnX')
    def fn1(**kwargs):
        mock1(**kwargs)

    @kopf.on.validate(*resource)
    def fn2(**kwargs):
        mock2(**kwargs)

    @kopf.on.mutate(*resource)
    def fn3(**kwargs):
        mock3(**kwargs)

    @kopf.on.mutate(*resource, id='fnX')
    def fn4(**kwargs):
        mock4(**kwargs)

    response = await serve_admission_request(
        adm_request, webhook=HandlerId('fnX'), reason=reason,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert mock1.call_count == (1 if reason == WebhookType.VALIDATING else 0)
    assert mock2.call_count == 0
    assert mock3.call_count == 0
    assert mock4.call_count == (1 if reason == WebhookType.MUTATING else 0)


@pytest.mark.parametrize('operation', ['CREATE', 'UPDATE', 'CONNECT', '*WHATEVER*'])
async def test_mutating_handlers_are_selected_for_nondeletion(
        settings, registry, resource, memories, insights, indices, adm_request, operation):

    v_mock = Mock()
    m_mock = Mock()

    @kopf.on.validate(*resource)
    def v_fn(**kwargs):
        v_mock(**kwargs)

    @kopf.on.mutate(*resource)
    def m_fn(**kwargs):
        m_mock(**kwargs)

    adm_request['request']['operation'] = operation
    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert v_mock.call_count == 1
    assert m_mock.call_count == 1


async def test_mutating_handlers_are_not_selected_for_deletion_by_default(
        settings, registry, resource, memories, insights, indices, adm_request):

    v_mock = Mock()
    m_mock = Mock()

    @kopf.on.validate(*resource)
    def v_fn(**kwargs):
        v_mock(**kwargs)

    @kopf.on.mutate(*resource)
    def m_fn(**kwargs):
        m_mock(**kwargs)

    adm_request['request']['operation'] = 'DELETE'
    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert v_mock.call_count == 1
    assert m_mock.call_count == 0


async def test_mutating_handlers_are_selected_for_deletion_if_explicitly_marked(
        settings, registry, resource, memories, insights, indices, adm_request):

    v_mock = Mock()
    m_mock = Mock()

    @kopf.on.validate(*resource)
    def v_fn(**kwargs):
        v_mock(**kwargs)

    @kopf.on.mutate(*resource, operations=['DELETE'])
    def m_fn(**kwargs):
        m_mock(**kwargs)

    adm_request['request']['operation'] = 'DELETE'
    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert v_mock.call_count == 1
    assert m_mock.call_count == 1


@pytest.mark.parametrize('handler_sub, request_sub, exp_calls', [
    ('*', 'no', 1),
    ('*', 'sb', 1),
    ('*', None, 1),
    ('sb', 'no', 0),
    ('sb', 'sb', 1),
    ('sb', None, 0),
    (None, 'no', 0),
    (None, 'sb', 0),
    (None, None, 1),
])
async def test_subresources(
        settings, registry, resource, memories, insights, indices, adm_request,
        handler_sub, request_sub, exp_calls):

    v_mock = Mock()
    m_mock = Mock()

    @kopf.on.validate(*resource, subresource=handler_sub)
    def v_fn(**kwargs):
        v_mock(**kwargs)

    @kopf.on.mutate(*resource, subresource=handler_sub)
    def m_fn(**kwargs):
        m_mock(**kwargs)

    adm_request['request']['subResource'] = request_sub
    adm_request['request']['requestSubResource'] = request_sub
    response = await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    assert response['response']['allowed'] is True
    assert v_mock.call_count == exp_calls
    assert m_mock.call_count == exp_calls
