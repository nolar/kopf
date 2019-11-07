import pytest
import requests

from kopf.clients.fetching import read_crd


async def test_when_present(req_mock, resource):
    result = {}
    req_mock.get.return_value.json.return_value = result

    crd = await read_crd(resource=resource)
    assert crd is result

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert '/customresourcedefinitions/kopfexamples.zalando.org' in url


@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_no_default(req_mock, resource, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    with pytest.raises(requests.exceptions.HTTPError) as e:
        await read_crd(resource=resource)
    assert e.value.response.status_code == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_default(req_mock, resource, default, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    crd = await read_crd(resource=resource, default=default)
    assert crd is default


@pytest.mark.parametrize('status', [400, 401, 500, 666])
async def test_raises_api_error_despite_default(req_mock, resource, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    with pytest.raises(requests.exceptions.HTTPError) as e:
        await read_crd(resource=resource, default=object())
    assert e.value.response.status_code == status
