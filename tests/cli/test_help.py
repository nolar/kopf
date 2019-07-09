

def test_help_in_root(invoke, mocker):
    login_pykube = mocker.patch('kopf.clients.auth.login_pykube')
    login_client = mocker.patch('kopf.clients.auth.login_client')
    verify_pykube = mocker.patch('kopf.clients.auth.verify_pykube')
    verify_client = mocker.patch('kopf.clients.auth.verify_client')

    result = invoke(['--help'])

    assert result.exit_code == 0
    assert not login_pykube.called
    assert not login_client.called
    assert not verify_pykube.called
    assert not verify_client.called

    assert 'Usage: kopf [OPTIONS]' in result.output
    assert '  run ' in result.output
    assert '  freeze ' in result.output
    assert '  resume ' in result.output


def test_help_in_subcommand(invoke, mocker):
    login_pykube = mocker.patch('kopf.clients.auth.login_pykube')
    login_client = mocker.patch('kopf.clients.auth.login_client')
    verify_pykube = mocker.patch('kopf.clients.auth.verify_pykube')
    verify_client = mocker.patch('kopf.clients.auth.verify_client')
    preload = mocker.patch('kopf.utilities.loaders.preload')
    real_run = mocker.patch('kopf.reactor.queueing.run')

    result = invoke(['run', '--help'])

    assert result.exit_code == 0
    assert not login_pykube.called
    assert not login_client.called
    assert not verify_pykube.called
    assert not verify_client.called
    assert not preload.called
    assert not real_run.called

    # Enough to be sure this is not a root command help.
    assert 'Usage: kopf run [OPTIONS]' in result.output
    assert '  --standalone' in result.output
    assert '  -m, --module' in result.output
