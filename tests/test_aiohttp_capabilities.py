import pytest

from kopf._cogs.helpers.aiohttpcaps import _check_aiohttp_has_graceful_shutdown, _parse_version


def test_parse_version_simple():
    assert _parse_version('3.12.4') == (3, 12, 4)


def test_parse_version_pre_release_suffix():
    assert _parse_version('3.12.4rc1') == (3, 12, 4)


def test_parse_version_dev_suffix():
    assert _parse_version('3.12.4.dev0') == (3, 12, 4)


def test_parse_version_non_numeric():
    with pytest.raises(ValueError):
        _parse_version('abc.def.ghi')


def test_graceful_shutdown_with_capability(mocker):
    mock_aiohttp = mocker.patch('kopf._cogs.helpers.aiohttpcaps.aiohttp')
    mock_aiohttp.__version__ = '3.12.4'
    assert _check_aiohttp_has_graceful_shutdown() is True


def test_graceful_shutdown_without_capability(mocker):
    mock_aiohttp = mocker.patch('kopf._cogs.helpers.aiohttpcaps.aiohttp')
    mock_aiohttp.__version__ = '3.12.3'
    assert _check_aiohttp_has_graceful_shutdown() is False


def test_graceful_shutdown_fallback_with_capability(mocker):
    mocker.patch('kopf._cogs.helpers.aiohttpcaps.aiohttp', spec=[])
    mocker.patch('importlib.metadata.version', return_value='3.12.4')
    assert _check_aiohttp_has_graceful_shutdown() is True


def test_graceful_shutdown_fallback_without_capability(mocker):
    mocker.patch('kopf._cogs.helpers.aiohttpcaps.aiohttp', spec=[])
    mocker.patch('importlib.metadata.version', return_value='3.12.3')
    assert _check_aiohttp_has_graceful_shutdown() is False


def test_graceful_shutdown_all_methods_fail_returns_true(mocker):
    mocker.patch('kopf._cogs.helpers.aiohttpcaps.aiohttp', spec=[])
    mocker.patch('importlib.metadata.version', side_effect=ValueError)
    assert _check_aiohttp_has_graceful_shutdown() is True
