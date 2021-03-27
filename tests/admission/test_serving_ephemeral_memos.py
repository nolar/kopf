import pytest

from kopf.reactor.admission import serve_admission_request


@pytest.mark.parametrize('operation', ['CREATE'])
async def test_memo_is_not_remembered_if_admission_is_for_creation(
        settings, registry, resource, memories, insights, indices, adm_request, operation):

    adm_request['request']['operation'] = operation
    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    known_memories = list(memories.iter_all_memories())
    assert not known_memories


@pytest.mark.parametrize('operation', ['UPDATE', 'DELETE', 'CONNECT', '*WHATEVER*'])
async def test_memo_is_remembered_if_admission_for_other_operations(
        settings, registry, resource, memories, insights, indices, adm_request, operation):

    adm_request['request']['operation'] = operation
    await serve_admission_request(
        adm_request,
        settings=settings, registry=registry, insights=insights,
        memories=memories, memobase=object(), indices=indices,
    )
    known_memories = list(memories.iter_all_memories())
    assert len(known_memories) == 1
