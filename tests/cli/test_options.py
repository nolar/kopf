import pytest


@pytest.mark.parametrize('kwarg, value, options, envvars', [
    ('paths', (), [], {}),
    ('paths', ('path1', 'path2'), ['path1', 'path2'], {}),
    # ('paths', ('path1', 'path2'), [], {'KOPF_RUN_PATHS': 'path1 path2'}), FIXME: UNSUPPORTED

    ('modules', (), [], {}),
    ('modules', ('mod1', 'mod2'), ['-m', 'mod1', '-m', 'mod2'], {}),
    ('modules', ('mod1', 'mod2'), ['--module', 'mod1', '--module', 'mod2'], {}),
    ('modules', ('mod1', 'mod2'), [], {'KOPF_RUN_MODULES': 'mod1 mod2'}),
], ids=[
    'default-paths', 'arg-paths', #'env-paths',
    'default-modules', 'opt-short-m', 'opt-long-modules', 'env-modules',
])
def test_options_passed_to_preload(invoke, options, envvars, kwarg, value, preload, real_run):
    result = invoke(['run'] + options, env=envvars)
    assert result.exit_code == 0
    assert preload.called
    assert preload.call_args[1][kwarg] == value


@pytest.mark.parametrize('kwarg, value, options, envvars', [
    ('standalone', None, [], {}),
    ('standalone', True, ['--standalone'], {}),
    ('standalone', True, [], {'KOPF_RUN_STANDALONE': 'true'}),

    ('namespaces', (), [], {}),
    ('namespaces', ('ns',), ['-n', 'ns'], {}),
    ('namespaces', ('ns',), ['--namespace=ns'], {}),
    ('namespaces', ('ns',), [], {'KOPF_RUN_NAMESPACE': 'ns'}),
    ('namespaces', ('ns',), [], {'KOPF_RUN_NAMESPACES': 'ns'}),

    ('namespaces', ('ns1', 'ns2'), ['--namespace=ns1', '-n', 'ns2'], {}),
    ('namespaces', ('ns1', 'ns2'), [], {'KOPF_RUN_NAMESPACES': 'ns1 ns2'}),

    ('peering_name', None, [], {}),
    ('peering_name', 'peer', ['-P', 'peer'], {}),
    ('peering_name', 'peer', ['--peering=peer'], {}),
    ('peering_name', 'peer', [], {'KOPF_RUN_PEERING': 'peer'}),

    ('priority', None, [], {}),
    ('priority', 123, ['-p', '123'], {}),
    ('priority', 123, ['--priority=123'], {}),
    ('priority', 123, [], {'KOPF_RUN_PRIORITY': '123'}),
    ('priority', 666, ['--dev'], {}),
], ids=[
    'default-standalone', 'opt-long-standalone', 'env-standalone',
    'default-namespace', 'opt-short-n', 'opt-long-namespace', 'env1-namespace', 'env2-namespace',
    'opt-multi-namespaces', 'env-multi-namespaces',
    'default-peering', 'opt-short-P', 'opt-long-peering', 'env-peering',
    'default-priority', 'opt-short-p', 'opt-long-priority', 'env-priority', 'opt-long-dev',
])
def test_options_passed_to_realrun(invoke, options, envvars, kwarg, value, preload, real_run):
    result = invoke(['run'] + options, env=envvars)
    assert result.exit_code == 0
    assert real_run.called
    assert real_run.call_args[1][kwarg] == value
