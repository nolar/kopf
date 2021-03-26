import asyncio
import logging

import pytest


@pytest.mark.parametrize('expect_debug, expect_info, options, envvars', [
    (False, True, [], {}),
    (False, False, ['-q'], {}),
    (False, False, ['--quiet'], {}),
    (False, False, [], {'KOPF_RUN_QUIET': 'true'}),
    (False, True, [], {'KOPF_ENV_QUIET': ''}),
    (True, True, ['-d'], {}),
    (True, True, ['--debug'], {}),
    (True, True, [], {'KOPF_RUN_DEBUG': 'true'}),
    (False, True, [], {'KOPF_ENV_DEBUG': ''}),
    (True, True, ['-v'], {}),
    (True, True, ['--verbose'], {}),
    (True, True, [], {'KOPF_RUN_VERBOSE': 'true'}),
    (False, True, [], {'KOPF_ENV_VERBOSE': ''}),
], ids=[
    'default',
    'opt-short-q', 'opt-long-quiet', 'env-quiet-true', 'env-quiet-empty',
    'opt-short-d', 'opt-long-debug', 'env-debug-true', 'env-debug-empty',
    'opt-short-v', 'opt-long-verbose', 'env-verbose-true', 'env-verbose-empty',
])
def test_verbosity(invoke, caplog, options, envvars, expect_debug, expect_info, preload, real_run):
    result = invoke(['run'] + options, env=envvars)
    assert result.exit_code == 0

    logger = logging.getLogger()
    logger.debug('some debug')
    logger.info('some info')
    logger.warning('some warning')
    logger.error('some error')

    assert len(caplog.records) >= 2 + int(expect_info) + int(expect_debug)
    assert caplog.records[-1].message == 'some error'
    assert caplog.records[-2].message == 'some warning'
    if expect_info:
        assert caplog.records[-3].message == 'some info'
    if expect_debug:
        assert caplog.records[-4].message == 'some debug'


@pytest.mark.parametrize('options', [
    ([]),
    (['-q']),
    (['--quiet']),
    (['-v']),
    (['--verbose']),
], ids=['default', 'q', 'quiet', 'v', 'verbose'])
def test_no_lowlevel_dumps_in_nondebug(invoke, caplog, options, preload, real_run):
    result = invoke(['run'] + options)
    assert result.exit_code == 0

    # TODO: This also goes to the pytest's output. Try to suppress it there (how?).
    logging.getLogger('asyncio').error('boom!')

    alien_records = [m for m in caplog.records if not m.name.startswith('kopf')]
    assert len(alien_records) == 0
    assert not asyncio.get_event_loop().get_debug()


@pytest.mark.parametrize('options', [
    (['-d']),
    (['--debug']),
], ids=['d', 'debug'])
def test_lowlevel_dumps_in_debug_mode(invoke, caplog, options, preload, real_run):
    result = invoke(['run'] + options)
    assert result.exit_code == 0

    logging.getLogger('asyncio').debug('hello!')

    alien_records = [m for m in caplog.records if not m.name.startswith('kopf')]
    assert len(alien_records) == 1
    assert asyncio.get_event_loop().get_debug()
